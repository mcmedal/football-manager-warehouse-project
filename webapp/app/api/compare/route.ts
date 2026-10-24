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
        player_name: string;
        age: number;
        position: string;
        club_name: string;
        league: string;
        league_difficulty_coef: number;
        wage_per_week_k: number;
        transfer_value_m: number;
        is_own_squad: number;
      }>(
        `
        SELECT
          p.player_key,
          p.player_name,
          p.age,
          p.position,
          t.club_name,
          t.league,
          COALESCE(l.league_difficulty_coef, 1.00) AS league_difficulty_coef,
          v.wage_per_week_k,
          v.transfer_value_m,
          p.is_own_squad
        FROM gold.dim_player_info p
        INNER JOIN gold.dim_team_info t ON p.team_key = t.team_key
        LEFT JOIN gold.dim_league l ON t.league = l.league
        LEFT JOIN gold.fact_players_value v ON p.player_key = v.player_key
        WHERE p.player_key = @playerKey
        `,
        { playerKey }
      );

      if (!base.length) return null;

      const player = base[0];
      const stats = (
        await executeQuery<Record<string, unknown>>(
          `SELECT * FROM gold.mart_outfield_scouting_summary WHERE player_key = @playerKey`,
          { playerKey }
        )
      )[0] || null;

      let percentiles: Record<string, number> = {};
      if (stats) {
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
            WHERE position = @pos AND minutes_played >= 450
          )
          SELECT * FROM GroupStats WHERE player_key = @playerKey;
          `,
          { pos: player.position, playerKey }
        );

        if (pctRows.length) percentiles = pctRows[0];
      }

      return { player, stats, percentiles };
    };

    const [player1, player2] = await Promise.all([
      fetchPlayerFull(p1Id),
      fetchPlayerFull(p2Id),
    ]);

    if (!player1 || !player2) {
      return NextResponse.json({ error: 'One or both players could not be found' }, { status: 404 });
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
      WHERE p.is_own_squad = 1
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
    return NextResponse.json({ error: 'Comparison failed' }, { status: 500 });
  }
}
