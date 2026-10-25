import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const p1Id = parseInt(searchParams.get('p1') || '', 10);
    const p2Id = parseInt(searchParams.get('p2') || '', 10);

    if (isNaN(p1Id) || isNaN(p2Id)) {
      return NextResponse.json({ error: 'Please provide valid p1 and p2 query parameters' }, { status: 400 });
    }

    const fetchPlayerFull = async (playerKey: number) => {
      const base = await executeQuery<{
        player_key: number;
        player_id: number;
        player_name: string;
        age: number;
        position: string;
        club_name: string;
        league: string;
        league_difficulty_coef: number;
        wage_per_week_k: number;
        transfer_value_m: number;
        is_own_squad: number;
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
          t.club_name,
          t.league,
          COALESCE(l.league_difficulty_coef, 1.00) AS league_difficulty_coef,
          v.wage_per_week_k,
          v.transfer_value_m,
          p.is_own_squad,
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

      if (!base.length) return null;

      const player = base[0];
      const isGK = player.position?.toLowerCase().includes('goalkeeper');

      let stats: Record<string, unknown> | null = null;
      let percentiles: Record<string, number> = {};

      if (!isGK) {
        // 1. Try active outfield mart
        const martRows = await executeQuery<Record<string, unknown>>(
          `SELECT * FROM gold.mart_outfield_scouting_summary WHERE player_key = @playerKey`,
          { playerKey }
        );

        if (martRows.length > 0) {
          stats = martRows[0];
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
                PERCENT_RANK() OVER (ORDER BY pressure_efficiency) * 100.0 AS pct_pressure_eff,
                PERCENT_RANK() OVER (ORDER BY aerial_duels_won_per90) * 100.0 AS pct_aerials_won,
                PERCENT_RANK() OVER (ORDER BY heading_accuracy) * 100.0 AS pct_heading_acc
              FROM gold.mart_outfield_scouting_summary
              WHERE position = @pos AND (minutes_played >= 450 OR player_key = @playerKey)
            )
            SELECT * FROM GroupStats WHERE player_key = @playerKey;
            `,
            { pos: player.position, playerKey }
          );
          if (pctRows.length) percentiles = pctRows[0];
        } else {
          // Fallback to fact_outfield_player_stats for historical iterations or low minutes
          const factRows = await executeQuery<Record<string, unknown>>(
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

          if (factRows.length > 0) {
            stats = factRows[0];
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
        // Goalkeeper handling
        const gkMartRows = await executeQuery<Record<string, unknown>>(
          `SELECT * FROM gold.mart_gk_scouting_summary WHERE player_key = @playerKey`,
          { playerKey }
        );

        if (gkMartRows.length > 0) {
          stats = gkMartRows[0];
          const pctRows = await executeQuery<Record<string, number>>(
            `
            WITH GKGroupStats AS (
              SELECT 
                player_key,
                PERCENT_RANK() OVER (ORDER BY saves_percentage) * 100.0 AS pct_save_pct,
                PERCENT_RANK() OVER (ORDER BY xGoals_prevented_per90) * 100.0 AS pct_xg_prevented,
                PERCENT_RANK() OVER (ORDER BY parry_rate DESC) * 100.0 AS pct_parry_rate_inv,
                PERCENT_RANK() OVER (ORDER BY hold_rate_over_expected) * 100.0 AS pct_hold_rate_oe,
                PERCENT_RANK() OVER (ORDER BY sweeper_actions_per90) * 100.0 AS pct_sweeper_actions,
                PERCENT_RANK() OVER (ORDER BY completed_passes_per90) * 100.0 AS pct_completed_passes,
                PERCENT_RANK() OVER (ORDER BY pass_accuracy) * 100.0 AS pct_pass_acc,
                PERCENT_RANK() OVER (ORDER BY pen_save_shrunk) * 100.0 AS pct_pen_save,
                PERCENT_RANK() OVER (ORDER BY interceptions_per90) * 100.0 AS pct_interceptions,
                PERCENT_RANK() OVER (ORDER BY clearances_per90) * 100.0 AS pct_clearances
              FROM gold.mart_gk_scouting_summary
              WHERE (minutes_played >= 450 OR player_key = @playerKey)
            )
            SELECT * FROM GKGroupStats WHERE player_key = @playerKey;
            `,
            { playerKey }
          );
          if (pctRows.length) percentiles = pctRows[0];
        } else {
          // Fallback to fact_gk_stats
          const factGKRows = await executeQuery<Record<string, unknown>>(
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

          if (factGKRows.length > 0) {
            stats = factGKRows[0];
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

      return { player, stats, percentiles };
    };

    const [player1, player2] = await Promise.all([
      fetchPlayerFull(p1Id),
      fetchPlayerFull(p2Id),
    ]);

    if (!player1 || !player2) {
      return NextResponse.json({ error: 'One or both players could not be found in warehouse' }, { status: 404 });
    }

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
      WHERE p.is_own_squad = 1 AND p.dwh_current_validity = 1
      `
    );

    const managedClub = managedTeamRows.length > 0
      ? managedTeamRows[0]
      : {
          club_name: 'Own Club',
          league: player1.player.league,
          league_difficulty_coef: 1.00,
        };

    return NextResponse.json({ player1, player2, managedClub });
  } catch (error) {
    console.error('Compare API error:', error);
    return NextResponse.json({ error: 'Comparison computation failed' }, { status: 500 });
  }
}

