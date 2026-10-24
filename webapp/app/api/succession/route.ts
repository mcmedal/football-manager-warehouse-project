import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

interface SquadMember {
  player_key: number;
  player_name: string;
  position: string;
  age: number;
  minutes_played: number;
  transfer_value_m: number | null;
  wage_per_week_k: number | null;
  is_gk: boolean;
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const action = searchParams.get('action');

    // Action 1: Return own squad options sorted by age
    if (action === 'squad_options') {
      const outfieldSql = `
        SELECT player_key, player_name, position, age, minutes_played, transfer_value_m, wage_per_week_k, 0 as is_gk
        FROM gold.mart_outfield_scouting_summary
        WHERE is_own_squad = 1
      `;
      const gkSql = `
        SELECT player_key, player_name, position, age, minutes_played, transfer_value_m, wage_per_week_k, 1 as is_gk
        FROM gold.mart_gk_scouting_summary
        WHERE is_own_squad = 1
      `;

      const [outfield, gk] = await Promise.all([
        executeQuery<SquadMember>(outfieldSql),
        executeQuery<SquadMember>(gkSql),
      ]);

      const all = [...outfield, ...gk].sort((a, b) => b.age - a.age);
      return NextResponse.json({ squad: all });
    }

    // Action 2: Succession analysis for a chosen incumbent
    const incumbentKeyStr = searchParams.get('incumbentKey');
    if (!incumbentKeyStr) {
      return NextResponse.json({ error: 'incumbentKey is required' }, { status: 400 });
    }

    const incumbentKey = parseInt(incumbentKeyStr, 10);
    const positionMode = searchParams.get('positionMode') || 'exact'; // 'exact' | 'compatible' | 'any'
    const maxAge = searchParams.get('maxAge') ? parseInt(searchParams.get('maxAge')!, 10) : 25;
    const maxVal = searchParams.get('maxVal') ? parseFloat(searchParams.get('maxVal')!) : null;
    const maxWage = searchParams.get('maxWage') ? parseFloat(searchParams.get('maxWage')!) : null;
    const minMinutes = parseInt(searchParams.get('minMinutes') || '450', 10);
    const limit = Math.min(parseInt(searchParams.get('limit') || '15', 10), 30);

    // Check whether incumbent is GK or Outfield
    const gkCheckSql = `SELECT TOP 1 * FROM gold.mart_gk_scouting_summary WHERE player_key = @key`;
    const gkRows = await executeQuery<any>(gkCheckSql, { key: incumbentKey });
    const isGk = gkRows.length > 0;

    if (isGk) {
      const incumbent = gkRows[0];
      const params: Record<string, unknown> = {
        key: incumbentKey,
        minMinutes,
        maxAge,
        limit,
      };

      const filters: string[] = [
        's.player_key <> @key',
        's.minutes_played >= @minMinutes',
        's.age <= @maxAge',
        's.xGoals_prevented_per90 IS NOT NULL',
        's.saves_percentage IS NOT NULL',
      ];

      if (maxVal !== null && !isNaN(maxVal)) {
        filters.push('s.transfer_value_m <= @maxVal');
        params.maxVal = maxVal;
      }
      if (maxWage !== null && !isNaN(maxWage)) {
        filters.push('s.wage_per_week_k <= @maxWage');
        params.maxWage = maxWage;
      }

      // External candidate query
      const candidateQuery = `
        SELECT TOP (@limit)
          s.player_key,
          s.player_name,
          s.position,
          s.club_name,
          s.league,
          s.age,
          s.minutes_played,
          s.transfer_value_m,
          s.wage_per_week_k,
          s.value_gap,
          s.is_own_squad,
          CAST(s.saves_percentage AS float) AS save_pct,
          CAST(s.xGoals_prevented_per90 AS float) AS psxg_net_per90,
          CAST(s.hold_rate_pct AS float) AS hold_rate_pct,
          CAST(s.sweeper_actions_per90 AS float) AS sweeper_actions_per90,
          CAST(s.completed_passes_per90 AS float) AS completed_passes_per90,
          CAST(s.pass_accuracy AS float) AS pass_accuracy,
          CAST(s.distribution_value AS float) AS distribution_value
        FROM gold.mart_gk_scouting_summary s
        WHERE ${filters.join(' AND ')} AND s.is_own_squad = 0
        ORDER BY 
          s.xGoals_prevented_per90 DESC,
          s.minutes_played DESC
      `;

      // In-house squad query (same position: Goalkeeper)
      const inHouseQuery = `
        SELECT
          s.player_key,
          s.player_name,
          s.position,
          s.club_name,
          s.league,
          s.age,
          s.minutes_played,
          s.transfer_value_m,
          s.wage_per_week_k,
          s.value_gap,
          s.is_own_squad,
          CAST(s.saves_percentage AS float) AS save_pct,
          CAST(s.xGoals_prevented_per90 AS float) AS psxg_net_per90,
          CAST(s.hold_rate_pct AS float) AS hold_rate_pct,
          CAST(s.sweeper_actions_per90 AS float) AS sweeper_actions_per90,
          CAST(s.completed_passes_per90 AS float) AS completed_passes_per90,
          CAST(s.pass_accuracy AS float) AS pass_accuracy,
          CAST(s.distribution_value AS float) AS distribution_value
        FROM gold.mart_gk_scouting_summary s
        WHERE s.is_own_squad = 1 AND s.player_key <> @key
        ORDER BY s.minutes_played DESC, s.age ASC
      `;

      const [candidates, inHouseRows] = await Promise.all([
        executeQuery<any>(candidateQuery, params),
        executeQuery<any>(inHouseQuery, { key: incumbentKey }),
      ]);

      const incPsxgNet = parseFloat(incumbent.xGoals_prevented_per90 || 0);
      const incSavePct = parseFloat(incumbent.saves_percentage || 0);
      const incSweeper = parseFloat(incumbent.sweeper_actions_per90 || 0);
      const incHoldPct = parseFloat(incumbent.hold_rate_pct || 0);
      const incWage = parseFloat(incumbent.wage_per_week_k || 0);
      const incVal = parseFloat(incumbent.transfer_value_m || 0);

      const processGk = (c: any) => {
        const cPsxgNet = parseFloat(c.psxg_net_per90 || 0);
        const cSavePct = parseFloat(c.save_pct || 0);
        const cSweeper = parseFloat(c.sweeper_actions_per90 || 0);
        const cHoldPct = parseFloat(c.hold_rate_pct || 0);
        const cWage = parseFloat(c.wage_per_week_k || 0);
        const cVal = parseFloat(c.transfer_value_m || 0);

        const d1 = Math.pow((cPsxgNet - incPsxgNet) / 0.35, 2);
        const d2 = Math.pow((cSavePct - incSavePct) / 10, 2);
        const d3 = Math.pow((cSweeper - incSweeper) / 1.0, 2);
        const d4 = Math.pow((cHoldPct - incHoldPct) / 10, 2);
        const dist = Math.sqrt(d1 + d2 + d3 + d4);
        const similarityPct = Math.max(10, Math.min(99, Math.round(100 / (1 + dist * 0.4))));

        const deltaPsxgNet = cPsxgNet - incPsxgNet;
        const seasonGoalsPrevented = deltaPsxgNet * 38;
        const deltaSavePct = cSavePct - incSavePct;
        const deltaSweeper = cSweeper - incSweeper;

        const wageSavingsAnnualK = Math.round((incWage - cWage) * 52);
        const valueDiffM = +(cVal - incVal).toFixed(2);

        return {
          player_key: c.player_key,
          player_name: c.player_name,
          position: c.position,
          club_name: c.club_name,
          league: c.league,
          age: c.age,
          minutes_played: c.minutes_played,
          transfer_value_m: c.transfer_value_m,
          wage_per_week_k: c.wage_per_week_k,
          value_gap: c.value_gap,
          is_own_squad: c.is_own_squad === 1 || c.is_own_squad === true,
          similarity_pct: similarityPct,
          metrics: {
            save_pct: cSavePct,
            psxg_net_per90: cPsxgNet,
            sweeper_actions_per90: cSweeper,
            hold_rate_pct: cHoldPct,
          },
          deltas: {
            delta_primary_stat: deltaPsxgNet,
            season_goal_delta: +seasonGoalsPrevented.toFixed(1),
            primary_label: 'PSxG Net / 90',
            season_label: 'Goals Prevented / 38 Gms',
            delta_stat_2: +deltaSavePct.toFixed(1),
            label_stat_2: 'Save % Delta',
            delta_stat_3: +deltaSweeper.toFixed(2),
            label_stat_3: 'Sweeper / 90 Delta',
          },
          financial: {
            wage_savings_annual_k: wageSavingsAnnualK,
            value_diff_m: valueDiffM,
          },
        };
      };

      const processedCandidates = candidates.map(processGk).sort((a, b) => b.deltas.season_goal_delta - a.deltas.season_goal_delta);
      const processedInHouse = inHouseRows.map(processGk).sort((a, b) => b.deltas.season_goal_delta - a.deltas.season_goal_delta);

      return NextResponse.json({
        isGk: true,
        incumbentPosition: incumbent.position,
        positionMode,
        incumbent: {
          player_key: incumbent.player_key,
          player_name: incumbent.player_name,
          position: incumbent.position,
          club_name: incumbent.club_name,
          league: incumbent.league,
          age: incumbent.age,
          minutes_played: incumbent.minutes_played,
          transfer_value_m: incumbent.transfer_value_m,
          wage_per_week_k: incumbent.wage_per_week_k,
          value_gap: incumbent.value_gap,
          metrics: {
            save_pct: incSavePct,
            psxg_net_per90: incPsxgNet,
            sweeper_actions_per90: incSweeper,
            hold_rate_pct: incHoldPct,
          },
        },
        candidates: processedCandidates,
        inHouseOptions: processedInHouse,
      });
    }

    // Outfield Succession
    const outfieldSql = `SELECT TOP 1 * FROM gold.mart_outfield_scouting_summary WHERE player_key = @key`;
    const outfieldRows = await executeQuery<any>(outfieldSql, { key: incumbentKey });
    if (outfieldRows.length === 0) {
      return NextResponse.json({ error: 'Player not found in scouting database' }, { status: 404 });
    }

    const incumbent = outfieldRows[0];
    const pos = (incumbent.position || '').trim();

    // Build Position Filter
    let posCondition = '';
    const params: Record<string, unknown> = {
      key: incumbentKey,
      minMinutes,
      maxAge,
      limit,
      exactPos: pos,
    };

    if (positionMode === 'exact') {
      // Exact Position Matching (supporting flank aliases like Left Defender + Left Wingback)
      if (pos === 'Left Defender' || pos === 'Left Wingback') {
        posCondition = "s.position IN ('Left Defender', 'Left Wingback')";
      } else if (pos === 'Right Defender' || pos === 'Right Wingback') {
        posCondition = "s.position IN ('Right Defender', 'Right Wingback')";
      } else {
        posCondition = 's.position = @exactPos';
      }
    } else if (positionMode === 'compatible') {
      // Broader tactical positional family
      if (pos.includes('Central Defender')) {
        posCondition = "s.position = 'Central Defender'";
      } else if (pos.includes('Defender') || pos.includes('Wingback')) {
        posCondition = "s.position IN ('Left Defender', 'Right Defender', 'Left Wingback', 'Right Wingback')";
      } else if (pos === 'Defensive Midfielder' || pos === 'Central Midfielder') {
        posCondition = "s.position IN ('Central Midfielder', 'Defensive Midfielder')";
      } else if (pos.includes('Attacking Midfielder') || pos.includes('Midfielder')) {
        posCondition = "s.position IN ('Central Attacking Midfielder', 'Left Attacking Midfielder', 'Right Attacking Midfielder', 'Right Midfielder', 'Left Midfielder')";
      } else if (pos.includes('Striker') || pos.includes('Forward')) {
        posCondition = "s.position IN ('Striker', 'Central Attacking Midfielder')";
      } else {
        posCondition = '1=1';
      }
    } else {
      posCondition = '1=1';
    }

    const candidateFilters: string[] = [
      's.player_key <> @key',
      's.is_own_squad = 0',
      's.minutes_played >= @minMinutes',
      's.age <= @maxAge',
      posCondition,
    ];

    if (maxVal !== null && !isNaN(maxVal)) {
      candidateFilters.push('s.transfer_value_m <= @maxVal');
      params.maxVal = maxVal;
    }
    if (maxWage !== null && !isNaN(maxWage)) {
      candidateFilters.push('s.wage_per_week_k <= @maxWage');
      params.maxWage = maxWage;
    }

    // External Candidates Query
    const candidateQuery = `
      SELECT TOP (@limit)
        s.player_key,
        s.player_name,
        s.position,
        s.club_name,
        s.league,
        s.age,
        s.minutes_played,
        s.transfer_value_m,
        s.wage_per_week_k,
        s.value_gap,
        s.is_own_squad,
        CAST(s.npxg_plus_xa_per90 AS float) AS npxg_plus_xa_per90,
        CAST(s.non_penalty_xGoals_per90 AS float) AS non_penalty_xGoals_per90,
        CAST(s.xAssists_per90 AS float) AS xAssists_per90,
        CAST(s.progressive_actions_per90 AS float) AS progressive_actions_per90,
        CAST(s.open_play_key_passes_per90 AS float) AS open_play_key_passes_per90,
        CAST(s.defensive_actions_per90 AS float) AS defensive_actions_per90,
        CAST(s.net_ball_security AS float) AS net_ball_security,
        CAST(s.turnover_rate AS float) AS turnover_rate
      FROM gold.mart_outfield_scouting_summary s
      WHERE ${candidateFilters.join(' AND ')}
      ORDER BY 
        s.npxg_plus_xa_per90 DESC,
        s.minutes_played DESC
    `;

    // In-House Squad Options Query (Same Position or department)
    const inHouseQuery = `
      SELECT
        s.player_key,
        s.player_name,
        s.position,
        s.club_name,
        s.league,
        s.age,
        s.minutes_played,
        s.transfer_value_m,
        s.wage_per_week_k,
        s.value_gap,
        s.is_own_squad,
        CAST(s.npxg_plus_xa_per90 AS float) AS npxg_plus_xa_per90,
        CAST(s.non_penalty_xGoals_per90 AS float) AS non_penalty_xGoals_per90,
        CAST(s.xAssists_per90 AS float) AS xAssists_per90,
        CAST(s.progressive_actions_per90 AS float) AS progressive_actions_per90,
        CAST(s.open_play_key_passes_per90 AS float) AS open_play_key_passes_per90,
        CAST(s.defensive_actions_per90 AS float) AS defensive_actions_per90,
        CAST(s.net_ball_security AS float) AS net_ball_security,
        CAST(s.turnover_rate AS float) AS turnover_rate
      FROM gold.mart_outfield_scouting_summary s
      WHERE s.is_own_squad = 1 AND s.player_key <> @key AND ${posCondition}
      ORDER BY s.minutes_played DESC, s.age ASC
    `;

    const [candidates, inHouseRows] = await Promise.all([
      executeQuery<any>(candidateQuery, params),
      executeQuery<any>(inHouseQuery, { key: incumbentKey, exactPos: pos }),
    ]);

    const incNpxgi = parseFloat(incumbent.npxg_plus_xa_per90 || 0);
    const incProg = parseFloat(incumbent.progressive_actions_per90 || 0);
    const incDef = parseFloat(incumbent.defensive_actions_per90 || 0);
    const incSecurity = parseFloat(incumbent.net_ball_security || 0);
    const incWage = parseFloat(incumbent.wage_per_week_k || 0);
    const incVal = parseFloat(incumbent.transfer_value_m || 0);

    const processOutfield = (c: any) => {
      const cNpxgi = parseFloat(c.npxg_plus_xa_per90 || 0);
      const cProg = parseFloat(c.progressive_actions_per90 || 0);
      const cDef = parseFloat(c.defensive_actions_per90 || 0);
      const cSecurity = parseFloat(c.net_ball_security || 0);
      const cWage = parseFloat(c.wage_per_week_k || 0);
      const cVal = parseFloat(c.transfer_value_m || 0);

      const d1 = Math.pow((cNpxgi - incNpxgi) / 0.25, 2);
      const d2 = Math.pow((cProg - incProg) / 3.0, 2);
      const d3 = Math.pow((cDef - incDef) / 3.0, 2);
      const d4 = Math.pow((cSecurity - incSecurity) / 5.0, 2);
      const dist = Math.sqrt(d1 + d2 + d3 + d4);
      const similarityPct = Math.max(10, Math.min(99, Math.round(100 / (1 + dist * 0.35))));

      const deltaNpxgi = cNpxgi - incNpxgi;
      const seasonGoalContributions = deltaNpxgi * 38;
      const deltaProg = cProg - incProg;
      const deltaDef = cDef - incDef;

      const wageSavingsAnnualK = Math.round((incWage - cWage) * 52);
      const valueDiffM = +(cVal - incVal).toFixed(2);

      return {
        player_key: c.player_key,
        player_name: c.player_name,
        position: c.position,
        club_name: c.club_name,
        league: c.league,
        age: c.age,
        minutes_played: c.minutes_played,
        transfer_value_m: c.transfer_value_m,
        wage_per_week_k: c.wage_per_week_k,
        value_gap: c.value_gap,
        is_own_squad: c.is_own_squad === 1 || c.is_own_squad === true,
        similarity_pct: similarityPct,
        metrics: {
          npxg_plus_xa_per90: cNpxgi,
          progressive_actions_per90: cProg,
          defensive_actions_per90: cDef,
          net_ball_security: cSecurity,
        },
        deltas: {
          delta_primary_stat: +deltaNpxgi.toFixed(2),
          season_goal_delta: +seasonGoalContributions.toFixed(1),
          primary_label: 'NP-xGI / 90',
          season_label: 'Season Goal Involvement Δ (38 Gms)',
          delta_stat_2: +deltaProg.toFixed(1),
          label_stat_2: 'Prog Actions / 90 Δ',
          delta_stat_3: +deltaDef.toFixed(1),
          label_stat_3: 'Def Actions / 90 Δ',
        },
        financial: {
          wage_savings_annual_k: wageSavingsAnnualK,
          value_diff_m: valueDiffM,
        },
      };
    };

    const processedCandidates = candidates.map(processOutfield).sort((a, b) => b.deltas.season_goal_delta - a.deltas.season_goal_delta);
    const processedInHouse = inHouseRows.map(processOutfield).sort((a, b) => b.deltas.season_goal_delta - a.deltas.season_goal_delta);

    return NextResponse.json({
      isGk: false,
      incumbentPosition: incumbent.position,
      positionMode,
      incumbent: {
        player_key: incumbent.player_key,
        player_name: incumbent.player_name,
        position: incumbent.position,
        club_name: incumbent.club_name,
        league: incumbent.league,
        age: incumbent.age,
        minutes_played: incumbent.minutes_played,
        transfer_value_m: incumbent.transfer_value_m,
        wage_per_week_k: incumbent.wage_per_week_k,
        value_gap: incumbent.value_gap,
        metrics: {
          npxg_plus_xa_per90: incNpxgi,
          progressive_actions_per90: incProg,
          defensive_actions_per90: incDef,
          net_ball_security: incSecurity,
        },
      },
      candidates: processedCandidates,
      inHouseOptions: processedInHouse,
    });
  } catch (error) {
    console.error('Succession API error:', error);
    return NextResponse.json({ error: 'Failed to compute succession' }, { status: 500 });
  }
}
