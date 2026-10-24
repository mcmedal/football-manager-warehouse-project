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

    // Check player base info and position
    const baseInfo = await executeQuery<{ position: string }>(
      `SELECT position FROM gold.dim_player_info WHERE player_key = @playerKey`,
      { playerKey }
    );

    if (!baseInfo.length) {
      return NextResponse.json({ similarPlayers: [] });
    }

    const isGK = baseInfo[0].position?.toLowerCase().includes('goalkeeper');

    if (isGK) {
      // 1. Get target GK metrics
      const targetRows = await executeQuery<{
        player_key: number;
        player_name: string;
        shot_stopping_delta: number;
        xGoals_prevented_per90: number;
        hold_rate_pct: number;
        sweeper_index: number;
        distribution_value: number;
        pass_accuracy: number;
      }>(
        `
        SELECT TOP 1
          player_key,
          player_name,
          shot_stopping_delta,
          xGoals_prevented_per90,
          hold_rate_pct,
          sweeper_index,
          distribution_value,
          pass_accuracy
        FROM gold.mart_gk_scouting_summary
        WHERE player_key = @playerKey
        `,
        { playerKey }
      );

      if (!targetRows.length) {
        return NextResponse.json({ similarPlayers: [] });
      }

      const target = targetRows[0];

      // 2. Query nearest GK neighbors
      const similarRows = await executeQuery<{
        player_key: number;
        player_name: string;
        age: number;
        club_name: string;
        league: string;
        wage_per_week_k: number;
        transfer_value_m: number;
        minutes_played: number;
        non_penalty_xGoals_per90: number;
        xAssits_per90: number;
        progressive_passes_per90: number;
        defensive_actions_per90: number;
        distance: number;
      }>(
        `
        SELECT TOP 6
          s.player_key,
          s.player_name,
          s.age,
          s.club_name,
          s.league,
          s.wage_per_week_k,
          s.transfer_value_m,
          s.minutes_played,
          s.xGoals_prevented_per90 AS non_penalty_xGoals_per90, -- mapped for UI card
          s.saves_percentage AS xAssits_per90,
          s.sweeper_index AS progressive_passes_per90,
          s.hold_rate_pct AS defensive_actions_per90,
          CAST(SQRT(
            SQUARE((COALESCE(s.shot_stopping_delta, 0.0) - @delta) * 5.0) +
            SQUARE((COALESCE(s.xGoals_prevented_per90, 0.0) - @xgPrev) * 3.0) +
            SQUARE(((COALESCE(s.hold_rate_pct, 40.0) - @hold) / 10.0)) +
            SQUARE(((COALESCE(s.sweeper_index, 0.0) - @sweep) / 3.0)) +
            SQUARE(((COALESCE(s.distribution_value, 0.0) - @dist) / 10.0)) +
            SQUARE((COALESCE(s.pass_accuracy, 0.70) - @passAcc) * 5.0)
          ) AS DECIMAL(5,2)) AS distance
        FROM gold.mart_gk_scouting_summary s
        WHERE s.player_key <> @playerKey AND s.minutes_played >= 450
        ORDER BY distance ASC
        `,
        {
          playerKey,
          delta: target.shot_stopping_delta || 0,
          xgPrev: target.xGoals_prevented_per90 || 0,
          hold: target.hold_rate_pct || 40.0,
          sweep: target.sweeper_index || 0,
          dist: target.distribution_value || 0,
          passAcc: target.pass_accuracy || 0.70,
        }
      );

      const formatted = similarRows.map((sim) => {
        const simScore = Math.max(60, Math.min(99, Math.round(100 - sim.distance * 14)));
        return {
          ...sim,
          similarity_score: simScore,
        };
      });

      return NextResponse.json({ similarPlayers: formatted });
    }

    // 1. Get target outfield player details
    const targetRows = await executeQuery<{
      player_key: number;
      player_name: string;
      position: string;
      non_penalty_xGoals_per90: number;
      xAssits_per90: number;
      progressive_passes_per90: number;
      dribbles_made_per90: number;
      defensive_actions_per90: number;
      aerial_duels_won_per90: number;
      pressure_efficiency: number;
    }>(
      `
      SELECT TOP 1
        player_key,
        player_name,
        position,
        non_penalty_xGoals_per90,
        xAssits_per90,
        progressive_passes_per90,
        dribbles_made_per90,
        defensive_actions_per90,
        aerial_duels_won_per90,
        pressure_efficiency
      FROM gold.mart_outfield_scouting_summary
      WHERE player_key = @playerKey
      `,
      { playerKey }
    );

    if (!targetRows.length) {
      return NextResponse.json({ similarPlayers: [] });
    }

    const p = targetRows[0];

    // 2. Query nearest outfield neighbors
    const similarRows = await executeQuery<{
      player_key: number;
      player_name: string;
      age: number;
      club_name: string;
      league: string;
      wage_per_week_k: number;
      transfer_value_m: number;
      minutes_played: number;
      non_penalty_xGoals_per90: number;
      xAssits_per90: number;
      progressive_passes_per90: number;
      defensive_actions_per90: number;
      distance: number;
    }>(
      `
      SELECT TOP 6
        s.player_key,
        s.player_name,
        s.age,
        s.club_name,
        s.league,
        s.wage_per_week_k,
        s.transfer_value_m,
        s.minutes_played,
        s.non_penalty_xGoals_per90,
        s.xAssits_per90,
        s.progressive_passes_per90,
        s.defensive_actions_per90,
        CAST(SQRT(
          SQUARE((s.non_penalty_xGoals_per90 - @npxg) * 3.0) +
          SQUARE((s.xAssits_per90 - @xa) * 3.0) +
          SQUARE((s.progressive_passes_per90 - @prog) * 1.5) +
          SQUARE((s.dribbles_made_per90 - @drib) * 2.0) +
          SQUARE((s.defensive_actions_per90 - @def) * 1.2) +
          SQUARE((s.aerial_duels_won_per90 - @aer) * 1.5)
        ) AS DECIMAL(5,2)) AS distance
      FROM gold.mart_outfield_scouting_summary s
      WHERE s.position = @pos 
        AND s.player_key <> @playerKey 
        AND s.minutes_played >= 450
      ORDER BY distance ASC
      `,
      {
        pos: p.position,
        playerKey,
        npxg: p.non_penalty_xGoals_per90 || 0,
        xa: p.xAssits_per90 || 0,
        prog: p.progressive_passes_per90 || 0,
        drib: p.dribbles_made_per90 || 0,
        def: p.defensive_actions_per90 || 0,
        aer: p.aerial_duels_won_per90 || 0,
      }
    );

    const formatted = similarRows.map((sim) => {
      const simScore = Math.max(60, Math.min(99, Math.round(100 - sim.distance * 12)));
      return {
        ...sim,
        similarity_score: simScore,
      };
    });

    return NextResponse.json({ similarPlayers: formatted });
  } catch (error) {
    console.error('Similar players API error:', error);
    return NextResponse.json({ error: 'Failed to compute similarity' }, { status: 500 });
  }
}
