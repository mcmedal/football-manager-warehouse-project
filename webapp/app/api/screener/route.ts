import { NextRequest, NextResponse } from 'next/server';
import { executeQuery } from '@/lib/db';

export const dynamic = 'force-dynamic';

export interface ScreenerFilter {
  field: string;
  operator: '>' | '<' | '>=' | '<=' | '=' | 'between' | 'in';
  value: number | string | number[] | string[];
  value2?: number;
}

// Outfield Whitelist
const OUTFIELD_FIELDS: Record<string, string> = {
  player_name: 's.player_name',
  position: 's.position',
  club_name: 's.club_name',
  league: 's.league',
  age: 's.age',
  minutes_played: 's.minutes_played',
  transfer_value_m: 's.transfer_value_m',
  wage_per_week_k: 's.wage_per_week_k',
  is_own_squad: 's.is_own_squad',
  league_difficulty_coef: 's.league_difficulty_coef',
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
  shots_outside_box_ratio: 's.shots_outside_box_ratio',
  shots_per90: 's.shots_per90',
  xGoals_per_shot: 's.xGoals_per_shot',
  shot_on_target_rate: 's.shot_on_target_rate',
  conversion_rate: 's.conversion_rate',
  passes_attempted_per90: 's.passes_attempted_per90',
  pass_accuracy: 's.pass_accuracy',
  completed_passes_per90: 's.completed_passes_per90',
  progressive_passes_per90: 's.progressive_passes_per90',
  progressive_actions_per90: 's.progressive_actions_per90',
  open_play_key_passes_per90: 's.open_play_key_passes_per90',
  completed_crosses_per90: 's.completed_crosses_per90',
  open_play_completed_crosses_per90: 's.open_play_completed_crosses_per90',
  crosses_share_of_creation: 's.crosses_share_of_creation',
  dribbles_made_per90: 's.dribbles_made_per90',
  turnover_rate: 's.turnover_rate',
  net_ball_security: 's.net_ball_security',
  defensive_actions_per90: 's.defensive_actions_per90',
  non_clearance_def_actions_per90: 's.non_clearance_def_actions_per90',
  tackles_attempted_per90: 's.tackles_attempted_per90',
  tackle_accuracy: 's.tackle_accuracy',
  interceptions_per90: 's.interceptions_per90',
  clearances_per90: 's.clearances_per90',
  blocks_per90: 's.blocks_per90',
  shots_blocked_per90: 's.shots_blocked_per90',
  duels_won_per90: 's.duels_won_per90',
  foul_per_duel: 's.foul_per_duel',
  cards_per_foul: 's.cards_per_foul',
  pressure_efficiency: 's.pressure_efficiency',
  pressure_accuracy: 's.pressure_accuracy',
  aerial_duels_won_per90: 's.aerial_duels_won_per90',
  heading_accuracy: 's.heading_accuracy',
  sprints_per90: 's.sprints_per90',
  distance_covered_km_per90: 's.distance_covered_km_per90',
  sprints_per_km: 's.sprints_per_km',
  high_press_load: 's.high_press_load',
  fouls_won_net: 's.fouls_won_net',
  cost_per_xgi: 's.cost_per_xgi',
  xgi_per_cost_m: 's.xgi_per_cost_m',
  goals_per90_adj: 's.goals_per90_adj',
  xGoals_per90_adj: 's.xGoals_per90_adj',
  non_penalty_xGoals_per90_adj: 's.non_penalty_xGoals_per90_adj',
  xAssists_per90_adj: 's.xAssists_per90_adj',
  npxg_plus_xa_per90_adj: 's.npxg_plus_xa_per90_adj',
  on_pitch_gd_shrunk: 's.on_pitch_gd_shrunk',
  value_gap: 's.value_gap',
};

// Goalkeeper Whitelist
const GK_FIELDS: Record<string, string> = {
  player_name: 's.player_name',
  position: 's.position',
  club_name: 's.club_name',
  league: 's.league',
  age: 's.age',
  minutes_played: 's.minutes_played',
  transfer_value_m: 's.transfer_value_m',
  wage_per_week_k: 's.wage_per_week_k',
  is_own_squad: 's.is_own_squad',
  league_difficulty_coef: 's.league_difficulty_coef',
  shots_faced_per90: 's.shots_faced_per90',
  shots_faced_total: 's.shots_faced_total',
  xgot_faced_per90: 's.xgot_faced_per90',
  shot_difficulty: 's.shot_difficulty',
  xgp_per_shot: 's.xgp_per_shot',
  xgp_per_shot_shrunk: 's.xgp_per_shot_shrunk',
  goals_vs_xgot_ratio: 's.goals_vs_xgot_ratio',
  xgp_per_season: 's.xgp_per_season',
  xgp_per_season_shrunk: 's.xgp_per_season_shrunk',
  shot_stopping_delta: 's.shot_stopping_delta',
  shot_stopping_delta_shrunk: 's.shot_stopping_delta_shrunk',
  xGoals_prevented_per90: 's.xGoals_prevented_per90',
  saves_made_per90: 's.saves_made_per90',
  saves_percentage: 's.saves_percentage',
  xSave_rate: 's.xSave_rate',
  goals_conceded_per90: 's.goals_conceded_per90',
  parry_rate: 's.parry_rate',
  tip_rate: 's.tip_rate',
  hold_rate: 's.hold_rate',
  hold_rate_shrunk: 's.hold_rate_shrunk',
  hold_rate_over_expected: 's.hold_rate_over_expected',
  hold_rate_pct: 's.hold_rate_pct',
  saves_tipped_per90: 's.saves_tipped_per90',
  saves_parried_per90: 's.saves_parried_per90',
  saves_held_per90: 's.saves_held_per90',
  sweeper_actions_per90: 's.sweeper_actions_per90',
  sweeper_share: 's.sweeper_share',
  sweeper_index: 's.sweeper_index',
  completed_passes_per90: 's.completed_passes_per90',
  giveaways_per90: 's.giveaways_per90',
  passing_involvement: 's.passing_involvement',
  distribution_value: 's.distribution_value',
  passes_attempted_per90: 's.passes_attempted_per90',
  pass_accuracy: 's.pass_accuracy',
  interceptions_per90: 's.interceptions_per90',
  clearances_per90: 's.clearances_per90',
  penalties_faced_per90: 's.penalties_faced_per90',
  penalties_save_percentage: 's.penalties_save_percentage',
  pen_save_shrunk: 's.pen_save_shrunk',
  errors_leading_to_goal_per90: 's.errors_leading_to_goal_per90',
  errors_leading_to_goal_shrunk: 's.errors_leading_to_goal_shrunk',
  error_rate: 's.error_rate',
  possession_differential: 's.possession_differential',
  is_eligible: 's.is_eligible',
  sample_reliability: 's.sample_reliability',
  value_gap: 's.value_gap',
};

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const targetType: 'outfield' | 'goalkeeper' = body.targetType === 'goalkeeper' ? 'goalkeeper' : 'outfield';
    const isGK = targetType === 'goalkeeper';
    const allowedFields = isGK ? GK_FIELDS : OUTFIELD_FIELDS;
    const sourceTable = isGK ? 'gold.mart_gk_scouting_summary' : 'gold.mart_outfield_scouting_summary';

    const filters: ScreenerFilter[] = Array.isArray(body.filters) ? body.filters : [];
    const search: string = body.search || '';
    const sortByParam: string = body.sortBy || (isGK ? 'xGoals_prevented_per90' : 'npxg_plus_xa_per90');
    const sortOrderParam: string = (body.sortOrder || 'DESC').toUpperCase();
    const sortOrder = sortOrderParam === 'ASC' ? 'ASC' : 'DESC';
    const page: number = Math.max(1, parseInt(body.page || '1', 10));
    const pageSize: number = Math.min(100, Math.max(10, parseInt(body.pageSize || '30', 10)));
    const offset = (page - 1) * pageSize;

    const sortColumn = allowedFields[sortByParam] || (isGK ? 's.xGoals_prevented_per90' : 's.npxg_plus_xa_per90');

    const sqlConditions: string[] = [];
    const sqlParams: Record<string, unknown> = {
      offset,
      pageSize,
    };

    if (search.trim()) {
      sqlConditions.push('(s.player_name LIKE @search OR s.club_name LIKE @search)');
      sqlParams.search = `%${search.trim()}%`;
    }

    // Process filters
    filters.forEach((f, idx) => {
      const dbCol = allowedFields[f.field];
      if (!dbCol) return;

      const pKey = `p_${idx}`;
      const pKey2 = `p2_${idx}`;

      if (f.operator === 'between' && typeof f.value === 'number' && typeof f.value2 === 'number') {
        sqlConditions.push(`${dbCol} BETWEEN @${pKey} AND @${pKey2}`);
        sqlParams[pKey] = f.value;
        sqlParams[pKey2] = f.value2;
      } else if (f.operator === 'in' && Array.isArray(f.value)) {
        if (f.value.length > 0) {
          const inPlaceholders = f.value.map((v, vIdx) => {
            const inKey = `${pKey}_${vIdx}`;
            sqlParams[inKey] = v;
            return `@${inKey}`;
          });
          sqlConditions.push(`${dbCol} IN (${inPlaceholders.join(', ')})`);
        }
      } else if (['>', '<', '>=', '<=', '='].includes(f.operator)) {
        sqlConditions.push(`${dbCol} ${f.operator} @${pKey}`);
        sqlParams[pKey] = f.value;
      }
    });

    const whereClause = sqlConditions.length ? `WHERE ${sqlConditions.join(' AND ')}` : '';

    // Count query
    const countQuery = `
      SELECT COUNT(*) AS total
      FROM ${sourceTable} s
      ${whereClause}
    `;

    // Data query
    const dataQuery = isGK
      ? `
      SELECT
        s.player_key,
        s.player_name,
        s.age,
        s.position,
        s.club_name,
        s.league,
        s.league_difficulty_coef,
        s.is_own_squad,
        s.minutes_played,
        s.wage_per_week_k,
        s.transfer_value_m,
        s.shots_faced_per90,
        s.shots_faced_total,
        s.xgot_faced_per90,
        s.shot_difficulty,
        s.xgp_per_shot,
        s.xgp_per_shot_shrunk,
        s.goals_vs_xgot_ratio,
        s.xgp_per_season,
        s.xgp_per_season_shrunk,
        s.shot_stopping_delta,
        s.shot_stopping_delta_shrunk,
        s.xGoals_prevented_per90,
        s.saves_made_per90,
        s.saves_percentage,
        s.xSave_rate,
        s.goals_conceded_per90,
        s.parry_rate,
        s.tip_rate,
        s.hold_rate,
        s.hold_rate_shrunk,
        s.hold_rate_over_expected,
        s.hold_rate_pct,
        s.saves_held_per90,
        s.saves_tipped_per90,
        s.saves_parried_per90,
        s.sweeper_actions_per90,
        s.sweeper_share,
        s.sweeper_index,
        s.completed_passes_per90,
        s.giveaways_per90,
        s.passing_involvement,
        s.distribution_value,
        s.passes_attempted_per90,
        s.pass_accuracy,
        s.interceptions_per90,
        s.clearances_per90,
        s.penalties_faced_per90,
        s.penalties_save_percentage,
        s.pen_save_shrunk,
        s.errors_leading_to_goal_per90,
        s.errors_leading_to_goal_shrunk,
        s.error_rate,
        s.possession_differential,
        s.is_eligible,
        s.sample_reliability,
        s.value_gap
      FROM gold.mart_gk_scouting_summary s
      ${whereClause}
      ORDER BY ${sortColumn} ${sortOrder}
      OFFSET @offset ROWS
      FETCH NEXT @pageSize ROWS ONLY
      `
      : `
      SELECT
        s.player_key,
        s.player_name,
        s.age,
        s.position,
        s.club_name,
        s.league,
        s.league_difficulty_coef,
        s.is_own_squad,
        s.minutes_played,
        s.wage_per_week_k,
        s.transfer_value_m,
        s.goals_per90,
        s.assists_per90,
        s.xGoals_per90,
        s.non_penalty_xGoals_per90,
        s.xAssits_per90,
        s.xAssists_per90,
        s.npxg_plus_xa_per90,
        s.goals_minus_xg_per90,
        s.finishing_overperf_shrunk,
        s.assist_luck,
        s.assist_luck_shrunk,
        s.assists_per_op_kp,
        s.npxg_per_shot,
        s.box_presence_proxy,
        s.long_range_goal_share,
        s.shots_outside_box_ratio,
        s.shots_per90,
        s.xGoals_per_shot,
        s.shot_on_target_rate,
        s.conversion_rate,
        s.passes_attempted_per90,
        s.pass_accuracy,
        s.completed_passes_per90,
        s.progressive_passes_per90,
        s.progressive_actions_per90,
        s.open_play_key_passes_per90,
        s.completed_crosses_per90,
        s.open_play_completed_crosses_per90,
        s.crosses_share_of_creation,
        s.dribbles_made_per90,
        s.turnover_rate,
        s.net_ball_security,
        s.defensive_actions_per90,
        s.non_clearance_def_actions_per90,
        s.tackles_attempted_per90,
        s.tackle_accuracy,
        s.interceptions_per90,
        s.clearances_per90,
        s.blocks_per90,
        s.shots_blocked_per90,
        s.duels_won_per90,
        s.foul_per_duel,
        s.cards_per_foul,
        s.pressure_efficiency,
        s.pressure_accuracy,
        s.aerial_duels_won_per90,
        s.heading_accuracy,
        s.sprints_per90,
        s.distance_covered_km_per90,
        s.sprints_per_km,
        s.high_press_load,
        s.fouls_won_net,
        s.cost_per_xgi,
        s.xgi_per_cost_m,
        s.goals_per90_adj,
        s.xGoals_per90_adj,
        s.non_penalty_xGoals_per90_adj,
        s.xAssists_per90_adj,
        s.npxg_plus_xa_per90_adj,
        s.on_pitch_gd_shrunk,
        s.value_gap
      FROM gold.mart_outfield_scouting_summary s
      ${whereClause}
      ORDER BY ${sortColumn} ${sortOrder}
      OFFSET @offset ROWS
      FETCH NEXT @pageSize ROWS ONLY
    `;

    const [countRows, dataRows] = await Promise.all([
      executeQuery<{ total: number }>(countQuery, sqlParams),
      executeQuery(dataQuery, sqlParams),
    ]);

    const total = countRows[0]?.total || 0;
    const totalPages = Math.ceil(total / pageSize);

    return NextResponse.json({
      targetType,
      players: dataRows,
      pagination: {
        page,
        pageSize,
        total,
        totalPages,
      },
    });
  } catch (error) {
    console.error('Screener API error:', error);
    return NextResponse.json({ error: 'Screener query failed' }, { status: 500 });
  }
}
