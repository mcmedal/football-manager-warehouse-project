import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const playerKey = parseInt(id, 10);

    if (isNaN(playerKey)) {
      return NextResponse.json({ error: 'Invalid player ID' }, { status: 400 });
    }

    // 1. Fetch player base information
    const playerBase = await executeQuery<{
      player_key: number;
      player_id: number;
      player_name: string;
      age: number;
      position: string;
      is_own_squad: number;
      club_name: string;
      league: string;
      league_difficulty_coef: number;
      wage_per_week_k: number;
      transfer_value_m: number;
      season: string;
      is_current_season: number;
    }>(
      `
      SELECT
        p.player_key,
        p.player_id,
        p.player_name,
        p.age,
        p.position,
        p.is_own_squad,
        t.club_name,
        t.league,
        COALESCE(l.league_difficulty_coef, 1.00) AS league_difficulty_coef,
        v.wage_per_week_k,
        v.transfer_value_m,
        p.season,
        CAST(p.dwh_current_validity AS INT) AS is_current_season
      FROM gold.dim_player_info p
      INNER JOIN gold.dim_team_info t ON p.team_key = t.team_key
      LEFT JOIN gold.dim_league l ON t.league = l.league
      OUTER APPLY (
        SELECT TOP 1 transfer_value_m, wage_per_week_k
        FROM gold.fact_players_value v
        WHERE v.player_key = p.player_key
        ORDER BY v.dwh_current_validity DESC, v.dwh_create_date DESC
      ) v
      WHERE p.player_key = @playerKey
      `,
      { playerKey }
    );

    if (!playerBase.length) {
      return NextResponse.json({ error: 'Player not found' }, { status: 404 });
    }

    const player = playerBase[0];
    const isGK = player.position?.toLowerCase().includes('goalkeeper');

    let stats: Record<string, unknown> | null = null;
    let percentiles: Record<string, number> = {};

    if (!isGK) {
      // Outfield stats: try active mart first
      const outfieldStats = await executeQuery<Record<string, unknown>>(
        `SELECT * FROM gold.mart_outfield_scouting_summary WHERE player_key = @playerKey`,
        { playerKey }
      );

      if (outfieldStats.length) {
        stats = outfieldStats[0];

        // Calculate FBref/Opta percentiles against other qualified players with the same position
        const pctRows = await executeQuery<Record<string, number>>(
          `
          WITH GroupStats AS (
            SELECT 
              player_key,
              PERCENT_RANK() OVER (ORDER BY non_penalty_xGoals_per90) * 100.0 AS pct_npxg,
              PERCENT_RANK() OVER (ORDER BY xAssits_per90) * 100.0 AS pct_xa,
              PERCENT_RANK() OVER (ORDER BY goals_minus_xg_per90) * 100.0 AS pct_goals_minus_xg,
              PERCENT_RANK() OVER (ORDER BY shots_per90) * 100.0 AS pct_shots,
              PERCENT_RANK() OVER (ORDER BY xGoals_per_shot) * 100.0 AS pct_xg_per_shot,
              PERCENT_RANK() OVER (ORDER BY shot_on_target_rate) * 100.0 AS pct_shot_accuracy,
              PERCENT_RANK() OVER (ORDER BY progressive_passes_per90) * 100.0 AS pct_prog_passes,
              PERCENT_RANK() OVER (ORDER BY open_play_key_passes_per90) * 100.0 AS pct_key_passes,
              PERCENT_RANK() OVER (ORDER BY pass_accuracy) * 100.0 AS pct_pass_acc,
              PERCENT_RANK() OVER (ORDER BY dribbles_made_per90) * 100.0 AS pct_dribbles,
              PERCENT_RANK() OVER (ORDER BY defensive_actions_per90) * 100.0 AS pct_def_actions,
              PERCENT_RANK() OVER (ORDER BY non_clearance_def_actions_per90) * 100.0 AS pct_non_clr_def_actions,
              PERCENT_RANK() OVER (ORDER BY duels_won_per90) * 100.0 AS pct_duels_won,
              PERCENT_RANK() OVER (ORDER BY tackles_attempted_per90) * 100.0 AS pct_tackles,
              PERCENT_RANK() OVER (ORDER BY interceptions_per90) * 100.0 AS pct_interceptions,
              PERCENT_RANK() OVER (ORDER BY possession_won_per90) * 100.0 AS pct_poss_won,
              PERCENT_RANK() OVER (ORDER BY possession_lost_per90 DESC) * 100.0 AS pct_poss_lost,
              PERCENT_RANK() OVER (ORDER BY pressure_accuracy) * 100.0 AS pct_pressure_acc,
              PERCENT_RANK() OVER (ORDER BY aerial_duels_won_per90) * 100.0 AS pct_aerials_won,
              PERCENT_RANK() OVER (ORDER BY heading_accuracy) * 100.0 AS pct_heading_acc,
              PERCENT_RANK() OVER (ORDER BY sprints_per_km) * 100.0 AS pct_sprints_per_km
            FROM gold.mart_outfield_scouting_summary
            WHERE position = @pos AND (minutes_played >= 450 OR player_key = @playerKey)
          )
          SELECT * FROM GroupStats WHERE player_key = @playerKey;
          `,
          { pos: player.position, playerKey }
        );

        if (pctRows.length) {
          percentiles = pctRows[0];
        }
      } else {
        // Fallback to fact_outfield_player_stats for previous seasons or unmaterialized players
        const factStats = await executeQuery<Record<string, unknown>>(
          `
          SELECT
            p.player_key,
            p.player_id,
            p.player_name,
            p.age,
            p.position,
            t.club_name,
            t.league,
            COALESCE(l.league_difficulty_coef, 1.00) AS league_difficulty_coef,
            p.is_own_squad,
            p.contracted,
            s.minutes_played,
            v.wage_per_week_k,
            v.transfer_value_m,
            s.goals_per90,
            s.assists_per90,
            s.xGoals_per90,
            s.non_penalty_xGoals_per90,
            s.xAssits_per90,
            s.xAssits_per90 AS xAssists_per90,
            (s.non_penalty_xGoals_per90 + s.xAssits_per90) AS npxg_plus_xa_per90,
            s.shots_per90,
            s.xGoals_per_shot,
            CAST(s.shots_on_target_per90 / NULLIF(s.shots_per90, 0) AS DECIMAL(7,4)) AS shot_on_target_rate,
            s.conversion_rate,
            s.open_play_key_passes_per90,
            s.chances_created_per90,
            s.dribbles_made_per90,
            s.progressive_passes_per90,
            s.pass_accuracy,
            s.possession_won_per90,
            s.possession_lost_per90,
            CAST(((s.tackles_attempted_per90 * s.tackle_accuracy) + s.interceptions_per90 + s.blocks_per90 + s.clearances_per90) AS DECIMAL(7,4)) AS defensive_actions_per90,
            CAST(s.headers_attempted_per90 * s.heading_accuracy AS DECIMAL(7,4)) AS aerial_duels_won_per90,
            s.heading_accuracy
          FROM gold.fact_outfield_player_stats s
          INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
          INNER JOIN gold.dim_team_info t ON s.team_key = t.team_key
          LEFT JOIN gold.dim_league l ON t.league = l.league
          OUTER APPLY (
            SELECT TOP 1 transfer_value_m, wage_per_week_k
            FROM gold.fact_players_value v
            WHERE v.player_key = s.player_key
            ORDER BY v.dwh_current_validity DESC, v.dwh_create_date DESC
          ) v
          WHERE s.player_key = @playerKey
          `,
          { playerKey }
        );

        if (factStats.length) {
          stats = factStats[0];
          const pctRows = await executeQuery<Record<string, number>>(
            `
            WITH GroupStats AS (
              SELECT 
                player_key,
                PERCENT_RANK() OVER (ORDER BY non_penalty_xGoals_per90) * 100.0 AS pct_npxg,
                PERCENT_RANK() OVER (ORDER BY xAssits_per90) * 100.0 AS pct_xa,
                PERCENT_RANK() OVER (ORDER BY shots_per90) * 100.0 AS pct_shots,
                PERCENT_RANK() OVER (ORDER BY xGoals_per_shot) * 100.0 AS pct_xg_per_shot,
                PERCENT_RANK() OVER (ORDER BY shot_on_target_rate) * 100.0 AS pct_shot_accuracy,
                PERCENT_RANK() OVER (ORDER BY progressive_passes_per90) * 100.0 AS pct_prog_passes,
                PERCENT_RANK() OVER (ORDER BY open_play_key_passes_per90) * 100.0 AS pct_key_passes,
                PERCENT_RANK() OVER (ORDER BY pass_accuracy) * 100.0 AS pct_pass_acc,
                PERCENT_RANK() OVER (ORDER BY dribbles_made_per90) * 100.0 AS pct_dribbles,
                PERCENT_RANK() OVER (ORDER BY defensive_actions_per90) * 100.0 AS pct_def_actions,
                PERCENT_RANK() OVER (ORDER BY tackles_attempted_per90) * 100.0 AS pct_tackles,
                PERCENT_RANK() OVER (ORDER BY interceptions_per90) * 100.0 AS pct_interceptions,
                PERCENT_RANK() OVER (ORDER BY possession_won_per90) * 100.0 AS pct_poss_won,
                PERCENT_RANK() OVER (ORDER BY possession_lost_per90 DESC) * 100.0 AS pct_poss_lost,
                PERCENT_RANK() OVER (ORDER BY aerial_duels_won_per90) * 100.0 AS pct_aerials_won,
                PERCENT_RANK() OVER (ORDER BY heading_accuracy) * 100.0 AS pct_heading_acc
              FROM (
                SELECT 
                  s.player_key,
                  s.non_penalty_xGoals_per90,
                  s.xAssits_per90,
                  s.shots_per90,
                  s.xGoals_per_shot,
                  CAST(s.shots_on_target_per90 / NULLIF(s.shots_per90, 0) AS DECIMAL(7,4)) AS shot_on_target_rate,
                  s.progressive_passes_per90,
                  s.open_play_key_passes_per90,
                  s.pass_accuracy,
                  s.dribbles_made_per90,
                  CAST(((s.tackles_attempted_per90 * s.tackle_accuracy) + s.interceptions_per90 + s.blocks_per90 + s.clearances_per90) AS DECIMAL(7,4)) AS defensive_actions_per90,
                  s.tackles_attempted_per90,
                  s.interceptions_per90,
                  s.possession_won_per90,
                  s.possession_lost_per90,
                  CAST(s.headers_attempted_per90 * s.heading_accuracy AS DECIMAL(7,4)) AS aerial_duels_won_per90,
                  s.heading_accuracy
                FROM gold.fact_outfield_player_stats s
                INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
                WHERE p.position = @pos AND s.season = @season AND (s.minutes_played >= 450 OR s.player_key = @playerKey)
              ) sub
            )
            SELECT * FROM GroupStats WHERE player_key = @playerKey;
            `,
            { pos: player.position, season: player.season, playerKey }
          );
          if (pctRows.length) percentiles = pctRows[0];
        }
      }
    } else {
      // Goalkeeper stats
      const gkStats = await executeQuery<Record<string, unknown>>(
        `SELECT * FROM gold.mart_gk_scouting_summary WHERE player_key = @playerKey`,
        { playerKey }
      );

      if (gkStats.length) {
        stats = gkStats[0];

        const pctRows = await executeQuery<Record<string, number>>(
          `
          WITH GKGroupStats AS (
            SELECT 
              player_key,
              PERCENT_RANK() OVER (ORDER BY xgp_per_shot_shrunk) * 100.0 AS pct_xgp_per_shot,
              PERCENT_RANK() OVER (ORDER BY xgp_per_season) * 100.0 AS pct_xgp_season,
              PERCENT_RANK() OVER (ORDER BY shot_stopping_delta) * 100.0 AS pct_shot_stopping_delta,
              PERCENT_RANK() OVER (ORDER BY xGoals_prevented_per90) * 100.0 AS pct_xg_prevented,
              PERCENT_RANK() OVER (ORDER BY saves_made_per90) * 100.0 AS pct_saves_made,
              PERCENT_RANK() OVER (ORDER BY saves_percentage) * 100.0 AS pct_save_pct,
              PERCENT_RANK() OVER (ORDER BY goals_vs_xgot_ratio DESC) * 100.0 AS pct_goals_vs_xgot_inv,
              PERCENT_RANK() OVER (ORDER BY goals_conceded_per90 DESC) * 100.0 AS pct_goals_conceded_inv,
              PERCENT_RANK() OVER (ORDER BY parry_rate DESC) * 100.0 AS pct_parry_rate_inv,
              PERCENT_RANK() OVER (ORDER BY hold_rate_over_expected) * 100.0 AS pct_hold_rate_oe,
              PERCENT_RANK() OVER (ORDER BY hold_rate_pct) * 100.0 AS pct_hold_rate,
              PERCENT_RANK() OVER (ORDER BY saves_held_per90) * 100.0 AS pct_saves_held,
              PERCENT_RANK() OVER (ORDER BY sweeper_actions_per90) * 100.0 AS pct_sweeper_actions,
              PERCENT_RANK() OVER (ORDER BY sweeper_index) * 100.0 AS pct_sweeper_idx,
              PERCENT_RANK() OVER (ORDER BY clearances_per90) * 100.0 AS pct_clearances,
              PERCENT_RANK() OVER (ORDER BY interceptions_per90) * 100.0 AS pct_interceptions,
              PERCENT_RANK() OVER (ORDER BY completed_passes_per90) * 100.0 AS pct_completed_passes,
              PERCENT_RANK() OVER (ORDER BY giveaways_per90 DESC) * 100.0 AS pct_giveaways_inv,
              PERCENT_RANK() OVER (ORDER BY distribution_value) * 100.0 AS pct_distribution,
              PERCENT_RANK() OVER (ORDER BY pass_accuracy) * 100.0 AS pct_pass_acc,
              PERCENT_RANK() OVER (ORDER BY pen_save_shrunk) * 100.0 AS pct_pen_save,
              PERCENT_RANK() OVER (ORDER BY errors_leading_to_goal_per90 DESC) * 100.0 AS pct_errors_inv
            FROM gold.mart_gk_scouting_summary
            WHERE (minutes_played >= 450 OR player_key = @playerKey)
          )
          SELECT * FROM GKGroupStats WHERE player_key = @playerKey;
          `,
          { playerKey }
        );

        if (pctRows.length) {
          percentiles = pctRows[0];
        }
      } else {
        // Fallback to fact_gk_stats
        const factGKStats = await executeQuery<Record<string, unknown>>(
          `
          SELECT
            p.player_key,
            p.player_id,
            p.player_name,
            p.age,
            p.position,
            t.club_name,
            t.league,
            COALESCE(l.league_difficulty_coef, 1.00) AS league_difficulty_coef,
            p.is_own_squad,
            p.contracted,
            s.minutes_played,
            v.wage_per_week_k,
            v.transfer_value_m,
            s.goals_conceded_per90,
            s.saves_made_per90,
            s.xGoals_prevented_per90,
            s.xSave_rate,
            s.saves_percentage,
            s.passes_attempted_per90,
            s.pass_accuracy,
            s.possession_won_per90,
            s.possession_lost_per90,
            s.interceptions_per90,
            s.clearances_per90,
            CAST(s.saves_parried_per90 / NULLIF(s.saves_made_per90, 0) AS DECIMAL(7,4)) AS parry_rate,
            CAST(s.saves_held_per90 / NULLIF(s.saves_made_per90, 0) AS DECIMAL(7,4)) AS hold_rate,
            CAST(s.interceptions_per90 + s.clearances_per90 AS DECIMAL(7,4)) AS sweeper_actions_per90,
            CAST(s.passes_attempted_per90 * s.pass_accuracy AS DECIMAL(7,4)) AS completed_passes_per90
          FROM gold.fact_gk_stats s
          INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
          INNER JOIN gold.dim_team_info t ON s.team_key = t.team_key
          LEFT JOIN gold.dim_league l ON t.league = l.league
          OUTER APPLY (
            SELECT TOP 1 transfer_value_m, wage_per_week_k
            FROM gold.fact_players_value v
            WHERE v.player_key = s.player_key
            ORDER BY v.dwh_current_validity DESC, v.dwh_create_date DESC
          ) v
          WHERE s.player_key = @playerKey
          `,
          { playerKey }
        );

        if (factGKStats.length) {
          stats = factGKStats[0];
          const pctRows = await executeQuery<Record<string, number>>(
            `
            WITH GKGroupStats AS (
              SELECT 
                player_key,
                PERCENT_RANK() OVER (ORDER BY saves_percentage) * 100.0 AS pct_save_pct,
                PERCENT_RANK() OVER (ORDER BY xGoals_prevented_per90) * 100.0 AS pct_xg_prevented,
                PERCENT_RANK() OVER (ORDER BY parry_rate DESC) * 100.0 AS pct_parry_rate_inv,
                PERCENT_RANK() OVER (ORDER BY sweeper_actions_per90) * 100.0 AS pct_sweeper_actions,
                PERCENT_RANK() OVER (ORDER BY completed_passes_per90) * 100.0 AS pct_completed_passes,
                PERCENT_RANK() OVER (ORDER BY pass_accuracy) * 100.0 AS pct_pass_acc,
                PERCENT_RANK() OVER (ORDER BY interceptions_per90) * 100.0 AS pct_interceptions,
                PERCENT_RANK() OVER (ORDER BY clearances_per90) * 100.0 AS pct_clearances
              FROM (
                SELECT 
                  s.player_key,
                  s.saves_percentage,
                  s.xGoals_prevented_per90,
                  CAST(s.saves_parried_per90 / NULLIF(s.saves_made_per90, 0) AS DECIMAL(7,4)) AS parry_rate,
                  CAST(s.interceptions_per90 + s.clearances_per90 AS DECIMAL(7,4)) AS sweeper_actions_per90,
                  CAST(s.passes_attempted_per90 * s.pass_accuracy AS DECIMAL(7,4)) AS completed_passes_per90,
                  s.pass_accuracy,
                  s.interceptions_per90,
                  s.clearances_per90
                FROM gold.fact_gk_stats s
                WHERE s.season = @season AND (s.minutes_played >= 450 OR s.player_key = @playerKey)
              ) sub
            )
            SELECT * FROM GKGroupStats WHERE player_key = @playerKey;
            `,
            { season: player.season, playerKey }
          );
          if (pctRows.length) percentiles = pctRows[0];
        }
      }
    }


    // Historical records (career snapshots via SCD Type 2)
    const history = await executeQuery<Record<string, unknown>>(
      `
      SELECT 
        p.season,
        p.age,
        t.club_name,
        t.league,
        v.wage_per_week_k,
        v.transfer_value_m,
        COALESCE(s.minutes_played, g.minutes_played, 0) AS minutes_played,
        -- Attacking & Goal threat
        s.goals,
        s.goals_per90,
        s.assists,
        s.assists_per90,
        s.xGoals_per90,
        s.non_penalty_xGoals_per90,
        s.xAssits_per90,
        CAST(s.non_penalty_xGoals_per90 + s.xAssits_per90 AS DECIMAL(5,2)) AS npxg_plus_xa_per90,
        s.shots_per90,
        s.shot_accuracy,
        s.conversion_rate,
        -- Passing & Progression
        s.passes_attempted_per90,
        s.pass_accuracy,
        s.progressive_passes_per90,
        s.open_play_key_passes_per90,
        s.chances_created_per90,
        s.dribbles_made_per90,
        s.cross_accuracy,
        -- Defending & Physical
        s.tackles_attempted_per90,
        s.tackle_accuracy,
        s.interceptions_per90,
        s.clearances_per90,
        s.blocks_per90,
        CAST(((s.tackles_attempted_per90 * s.tackle_accuracy) + s.interceptions_per90 + s.blocks_per90 + s.clearances_per90) AS DECIMAL(7,2)) AS defensive_actions_per90,
        s.headers_attempted_per90,
        s.heading_accuracy,
        s.possession_won_per90,
        s.possession_lost_per90,
        CAST(s.possession_won_per90 - s.possession_lost_per90 AS DECIMAL(7,2)) AS possession_differential,
        -- Goalkeeper Metrics
        g.saves_percentage,
        g.xGoals_prevented_per90,
        g.goals_conceded_per90,
        g.saves_made_per90,
        CAST(g.saves_held_per90 / NULLIF(g.saves_made_per90, 0) AS DECIMAL(5,2)) AS hold_rate,
        g.pass_accuracy AS gk_pass_accuracy,
        CAST(g.interceptions_per90 + g.clearances_per90 AS DECIMAL(7,2)) AS sweeper_actions_per90
      FROM gold.dim_player_info p
      INNER JOIN gold.dim_team_info t ON p.team_key = t.team_key
      OUTER APPLY (
        SELECT TOP 1 transfer_value_m, wage_per_week_k
        FROM gold.fact_players_value v
        WHERE v.player_key = p.player_key
        ORDER BY v.dwh_current_validity DESC, v.dwh_create_date DESC
      ) v
      OUTER APPLY (
        SELECT TOP 1 *
        FROM gold.fact_outfield_player_stats s
        WHERE s.player_key = p.player_key
        ORDER BY s.dwh_current_validity DESC, s.dwh_create_date DESC
      ) s
      OUTER APPLY (
        SELECT TOP 1 *
        FROM gold.fact_gk_stats g
        WHERE g.player_key = p.player_key
        ORDER BY g.dwh_current_validity DESC, g.dwh_create_date DESC
      ) g
      WHERE p.player_id = @playerId
      ORDER BY p.dwh_create_date ASC, p.season ASC
      `,
      { playerId: player.player_id }
    );

    // 4. Team tactical system & outlier context (directional heuristic estimate)
    const tacticalRows = await executeQuery<{
      tactical_archetype: string;
      tactical_framework: string;
      tactical_description: string;
      passes_attempted_per90: number;
      pass_accuracy: number;
      pressures_attempted_per90: number;
      headers_attempted_per90: number;
      clearances_per90: number;
      crosses_attempted_per90: number;
      blocks_per90: number;
      outlier_tag: string | null;
      outlier_rationale: string | null;
    }>(
      `
      SELECT TOP 1
        ta.tactical_archetype,
        ta.tactical_framework,
        ta.tactical_description,
        ta.passes_attempted_per90,
        ta.pass_accuracy,
        ta.pressures_attempted_per90,
        ta.headers_attempted_per90,
        ta.clearances_per90,
        ta.crosses_attempted_per90,
        ta.blocks_per90,
        o.outlier_tag,
        o.outlier_rationale
      FROM gold.dim_player_info p
      INNER JOIN gold.dim_team_info t ON p.team_key = t.team_key AND t.dwh_current_validity = 1
      LEFT JOIN gold.dim_team_tactical_archetypes ta ON t.club_name = ta.club_name AND ta.dwh_current_validity = 1
      LEFT JOIN gold.vw_player_system_outliers o ON p.player_key = o.player_key
      WHERE p.player_key = @playerKey
      `,
      { playerKey }
    );

    const tacticalContext = tacticalRows.length > 0 && tacticalRows[0].tactical_archetype ? tacticalRows[0] : null;

    // Fetch user managed club info to benchmark relative translation
    const managedTeamRows = await executeQuery<{
      club_name: string;
      league: string;
      league_difficulty_coef: number;
    }>(
      `
      SELECT TOP 1
        t.club_name,
        t.league,
        COALESCE(l.league_difficulty_coef, 1.00) AS league_difficulty_coef
      FROM gold.dim_player_info p
      INNER JOIN gold.dim_team_info t ON p.team_key = t.team_key
      LEFT JOIN gold.dim_league l ON t.league = l.league
      WHERE p.is_own_squad = 1
      `
    );

    const managedClub = managedTeamRows.length > 0
      ? managedTeamRows[0]
      : {
          club_name: 'Own Club',
          league: player.league,
          league_difficulty_coef: 1.00,
        };

    return NextResponse.json({
      player,
      stats,
      percentiles,
      history,
      tacticalContext,
      managedClub,
    });
  } catch (error) {
    console.error('Player detail API error:', error);
    return NextResponse.json({ error: 'Failed to fetch player details' }, { status: 500 });
  }
}
