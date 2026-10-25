import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

const OUTFIELD_METRICS: Record<string, string> = {
  goals_per90: 's.goals_per90',
  assists_per90: 's.assists_per90',
  non_penalty_xGoals_per90: 's.non_penalty_xGoals_per90',
  xAssits_per90: 's.xAssits_per90',
  xAssists_per90: 's.xAssists_per90',
  npxg_plus_xa_per90: 's.npxg_plus_xa_per90',
  goals_minus_xg_per90: 's.goals_minus_xg_per90',
  finishing_overperf_shrunk: 's.finishing_overperf_shrunk',
  assist_luck: 's.assist_luck',
  assist_luck_shrunk: 's.assist_luck_shrunk',
  assists_per_op_kp: 's.assists_per_op_kp',
  npxg_per_shot: 's.npxg_per_shot',
  box_presence_proxy: 's.box_presence_proxy',
  long_range_goal_share: 's.long_range_goal_share',
  shots_per90: 's.shots_per90',
  shot_on_target_rate: 's.shot_on_target_rate',
  conversion_rate: 's.conversion_rate',
  passes_attempted_per90: 's.passes_attempted_per90',
  pass_accuracy: 's.pass_accuracy',
  completed_passes_per90: 's.completed_passes_per90',
  progressive_passes_per90: 's.progressive_passes_per90',
  progressive_actions_per90: 's.progressive_actions_per90',
  open_play_key_passes_per90: 's.open_play_key_passes_per90',
  completed_crosses_per90: 's.completed_crosses_per90',
  crosses_share_of_creation: 's.crosses_share_of_creation',
  dribbles_made_per90: 's.dribbles_made_per90',
  turnover_rate: 's.turnover_rate',
  net_ball_security: 's.net_ball_security',
  defensive_actions_per90: 's.defensive_actions_per90',
  non_clearance_def_actions_per90: 's.non_clearance_def_actions_per90',
  tackles_attempted_per90: 's.tackles_attempted_per90',
  tackle_accuracy: 's.tackle_accuracy',
  interceptions_per90: 's.interceptions_per90',
  blocks_per90: 's.blocks_per90',
  clearances_per90: 's.clearances_per90',
  aerial_duels_won_per90: 's.aerial_duels_won_per90',
  pressures_attempted_per90: 's.pressures_attempted_per90',
  pressure_efficiency: 's.pressure_efficiency',
  distance_covered_km_per90: 's.distance_covered_km_per90',
  sprints_per90: 's.sprints_per90',
  value_gap: 's.value_gap',
  transfer_value_m: 's.transfer_value_m',
  wage_per_week_k: 's.wage_per_week_k',
  league_difficulty_coef: 's.league_difficulty_coef',
  minutes_played: 's.minutes_played',
  age: 's.age',
};

const GK_METRICS: Record<string, string> = {
  save_pct: 's.saves_percentage',
  saves_percentage: 's.saves_percentage',
  psxg_net_per90: 's.xGoals_prevented_per90',
  xGoals_prevented_per90: 's.xGoals_prevented_per90',
  xgot_faced_per90: 's.xgot_faced_per90',
  xgp_per_shot: 's.xgp_per_shot',
  shot_difficulty: 's.shot_difficulty',
  hold_rate_pct: 's.hold_rate_pct',
  hold_rate_shrunk: 's.hold_rate_shrunk',
  hold_rate: 's.hold_rate',
  sweeper_actions_per90: 's.sweeper_actions_per90',
  sweeper_share: 's.sweeper_share',
  interceptions_per90: 's.interceptions_per90',
  clearances_per90: 's.clearances_per90',
  completed_passes_per90: 's.completed_passes_per90',
  passes_attempted_per90: 's.passes_attempted_per90',
  pass_accuracy: 's.pass_accuracy',
  turnovers_per90: 's.giveaways_per90',
  giveaways_per90: 's.giveaways_per90',
  distribution_value: 's.distribution_value',
  penalties_save_percentage: 's.penalties_save_percentage',
  pen_save_shrunk: 's.pen_save_shrunk',
  errors_leading_to_goal_per90: 's.errors_leading_to_goal_per90',
  value_gap: 's.value_gap',
  transfer_value_m: 's.transfer_value_m',
  wage_per_week_k: 's.wage_per_week_k',
  league_difficulty_coef: 's.league_difficulty_coef',
  minutes_played: 's.minutes_played',
  age: 's.age',
};

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const mode = searchParams.get('mode') === 'gk' ? 'gk' : 'outfield';
    const isGk = mode === 'gk';

    const metricsMap = isGk ? GK_METRICS : OUTFIELD_METRICS;
    const defaultX = isGk ? 'psxg_net_per90' : 'non_penalty_xGoals_per90';
    const defaultY = isGk ? 'save_pct' : 'goals_per90';

    const rawX = searchParams.get('x') || defaultX;
    const rawY = searchParams.get('y') || defaultY;

    const xField = metricsMap[rawX] ? rawX : defaultX;
    const yField = metricsMap[rawY] ? rawY : defaultY;

    const xCol = metricsMap[xField];
    const yCol = metricsMap[yField];

    const posGroup = searchParams.get('posGroup') || 'ALL';
    const league = searchParams.get('league') || '';
    const minMinutes = parseInt(searchParams.get('minMinutes') || '450', 10);
    const maxAge = searchParams.get('maxAge') ? parseInt(searchParams.get('maxAge')!, 10) : null;
    const rawMaxPrice = searchParams.get('maxPrice') || searchParams.get('maxVal');
    const maxPrice = rawMaxPrice ? parseFloat(rawMaxPrice) : null;
    const maxWage = searchParams.get('maxWage') ? parseFloat(searchParams.get('maxWage')!) : null;
    const minCoef = searchParams.get('minCoef') ? parseFloat(searchParams.get('minCoef')!) : null;
    const searchParam = (searchParams.get('search') || '').trim();
    const limit = Math.min(parseInt(searchParams.get('limit') || '500', 10), 1000);

    const conditions: string[] = ['s.minutes_played >= @minMinutes'];
    const params: Record<string, unknown> = { minMinutes };

    // Mode-specific tables
    const table = isGk ? 'gold.mart_gk_scouting_summary' : 'gold.mart_outfield_scouting_summary';

    // Must have non-null values for the chosen axes
    conditions.push(`${xCol} IS NOT NULL`);
    conditions.push(`${yCol} IS NOT NULL`);

    // Position filters for outfield
    if (!isGk && posGroup !== 'ALL') {
      if (posGroup === 'DEF') {
        conditions.push("(s.position LIKE '%Defender%' OR s.position LIKE '%Wingback%' OR s.position LIKE '%Wing Back%')");
      } else if (posGroup === 'MID') {
        conditions.push("(s.position LIKE '%Midfielder%' AND s.position NOT LIKE '%Attacking Midfielder%')");
      } else if (posGroup === 'ATT') {
        conditions.push("(s.position LIKE '%Striker%' OR s.position LIKE '%Attacking Midfielder%')");
      }
    }

    if (league) {
      conditions.push('s.league = @league');
      params.league = league;
    }

    if (maxAge !== null && !isNaN(maxAge)) {
      conditions.push('s.age <= @maxAge');
      params.maxAge = maxAge;
    }

    if (maxPrice !== null && !isNaN(maxPrice)) {
      conditions.push('s.transfer_value_m <= @maxPrice');
      params.maxPrice = maxPrice;
    }

    if (maxWage !== null && !isNaN(maxWage)) {
      conditions.push('s.wage_per_week_k <= @maxWage');
      params.maxWage = maxWage;
    }

    if (minCoef !== null && !isNaN(minCoef)) {
      conditions.push('s.league_difficulty_coef >= @minCoef');
      params.minCoef = minCoef;
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const countQuery = `
      SELECT COUNT(*) AS total_count
      FROM ${table} s
      ${whereClause}
    `;

    const query = `
      SELECT TOP (@limit)
        s.player_key,
        s.player_name,
        s.position,
        s.club_name,
        s.league,
        s.league_difficulty_coef,
        s.age,
        s.minutes_played,
        s.transfer_value_m,
        s.wage_per_week_k,
        s.value_gap,
        s.is_own_squad,
        CAST(${xCol} AS float) AS x,
        CAST(${yCol} AS float) AS y
      FROM ${table} s
      ${whereClause}
      ORDER BY 
        s.is_own_squad DESC,
        ${searchParam ? `CASE WHEN s.player_name LIKE @searchParam THEN 0 ELSE 1 END,` : ''}
        ABS(CHECKSUM(s.player_key)) % 1000,
        s.player_key
    `;

    params.limit = limit;
    if (searchParam) {
      params.searchParam = `%${searchParam}%`;
    }

    interface ScatterRow {
      player_key: number;
      player_name: string;
      position: string;
      club_name: string;
      league: string;
      league_difficulty_coef: number;
      age: number;
      minutes_played: number;
      transfer_value_m: number | null;
      wage_per_week_k: number | null;
      value_gap: number | null;
      is_own_squad: number;
      x: number;
      y: number;
    }

    const [rows, countRows] = await Promise.all([
      executeQuery<ScatterRow>(query, params),
      executeQuery<{ total_count: number }>(countQuery, params)
    ]);

    const totalCohortSize = countRows[0]?.total_count ?? rows.length;

    if (rows.length === 0) {
      return NextResponse.json({
        xField,
        yField,
        cohortSize: 0,
        sampleSize: 0,
        xMedian: 0,
        yMedian: 0,
        xMean: 0,
        yMean: 0,
        xMin: 0,
        xMax: 1,
        yMin: 0,
        yMax: 1,
        players: [],
      });
    }

    // Calculate stats
    const xVals = rows.map((r) => r.x).sort((a, b) => a - b);
    const yVals = rows.map((r) => r.y).sort((a, b) => a - b);

    const getMedian = (arr: number[]) => {
      const mid = Math.floor(arr.length / 2);
      return arr.length % 2 !== 0 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
    };

    const xMedian = getMedian(xVals);
    const yMedian = getMedian(yVals);
    const xMean = xVals.reduce((sum, v) => sum + v, 0) / xVals.length;
    const yMean = yVals.reduce((sum, v) => sum + v, 0) / yVals.length;
    const xMin = xVals[0];
    const xMax = xVals[xVals.length - 1];
    const yMin = yVals[0];
    const yMax = yVals[yVals.length - 1];

    return NextResponse.json({
      xField,
      yField,
      cohortSize: totalCohortSize,
      sampleSize: rows.length,
      xMedian,
      yMedian,
      xMean,
      yMean,
      xMin,
      xMax,
      yMin,
      yMax,
      players: rows,
    });
  } catch (error) {
    console.error('Scatter API error:', error);
    return NextResponse.json({ error: 'Failed to fetch scatter data' }, { status: 500 });
  }
}
