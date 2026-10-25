'use client';

import React, { useState, useEffect, useRef, Suspense } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';
import {
  Search,
  Filter,
  Plus,
  X,
  ChevronDown,
  RotateCcw,
  Download,
  Table as TableIcon,
  Sparkles,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Shield,
  Award,
} from 'lucide-react';
import { ScreenerFilter } from '@/app/api/screener/route';

interface PlayerRow {
  player_key: number;
  player_name: string;
  age: number;
  position: string;
  club_name: string;
  league: string;
  league_difficulty_coef: number;
  is_own_squad: number;
  minutes_played: number;
  wage_per_week_k: number;
  transfer_value_m: number;
  // Outfield Core & Advanced Metrics
  goals_per90?: number;
  assists_per90?: number;
  xGoals_per90?: number;
  non_penalty_xGoals_per90?: number;
  xAssits_per90?: number;
  xAssists_per90?: number;
  npxg_plus_xa_per90?: number;
  goals_minus_xg_per90?: number;
  finishing_overperf_shrunk?: number;
  assist_luck?: number;
  assist_luck_shrunk?: number;
  assists_per_op_kp?: number;
  npxg_per_shot?: number;
  box_presence_proxy?: number;
  long_range_goal_share?: number;
  shots_outside_box_ratio?: number;
  shots_per90?: number;
  xGoals_per_shot?: number;
  shot_on_target_rate?: number;
  conversion_rate?: number;
  passes_attempted_per90?: number;
  pass_accuracy?: number;
  completed_passes_per90?: number;
  progressive_passes_per90?: number;
  progressive_actions_per90?: number;
  open_play_key_passes_per90?: number;
  completed_crosses_per90?: number;
  open_play_completed_crosses_per90?: number;
  crosses_share_of_creation?: number;
  dribbles_made_per90?: number;
  turnover_rate?: number;
  net_ball_security?: number;
  defensive_actions_per90?: number;
  non_clearance_def_actions_per90?: number;
  tackles_attempted_per90?: number;
  tackle_accuracy?: number;
  interceptions_per90?: number;
  clearances_per90?: number;
  blocks_per90?: number;
  shots_blocked_per90?: number;
  duels_won_per90?: number;
  foul_per_duel?: number;
  cards_per_foul?: number;
  pressure_efficiency?: number;
  pressure_accuracy?: number;
  aerial_duels_won_per90?: number;
  heading_accuracy?: number;
  sprints_per90?: number;
  distance_covered_km_per90?: number;
  sprints_per_km?: number;
  high_press_load?: number;
  fouls_won_net?: number;
  cost_per_xgi?: number;
  xgi_per_cost_m?: number;
  goals_per90_adj?: number;
  xGoals_per90_adj?: number;
  non_penalty_xGoals_per90_adj?: number;
  xAssists_per90_adj?: number;
  npxg_plus_xa_per90_adj?: number;
  on_pitch_gd_shrunk?: number;
  // Goalkeeper
  shots_faced_per90?: number;
  shots_faced_total?: number;
  xgot_faced_per90?: number;
  shot_difficulty?: number;
  xgp_per_shot?: number;
  xgp_per_shot_shrunk?: number;
  goals_vs_xgot_ratio?: number;
  xgp_per_season?: number;
  xgp_per_season_shrunk?: number;
  shot_stopping_delta?: number;
  shot_stopping_delta_shrunk?: number;
  xGoals_prevented_per90?: number;
  saves_made_per90?: number;
  saves_percentage?: number;
  xSave_rate?: number;
  goals_conceded_per90?: number;
  parry_rate?: number;
  tip_rate?: number;
  hold_rate?: number;
  hold_rate_shrunk?: number;
  hold_rate_over_expected?: number;
  hold_rate_pct?: number;
  saves_held_per90?: number;
  saves_tipped_per90?: number;
  saves_parried_per90?: number;
  sweeper_actions_per90?: number;
  sweeper_share?: number;
  sweeper_index?: number;
  giveaways_per90?: number;
  passing_involvement?: number;
  distribution_value?: number;
  penalties_faced_per90?: number;
  penalties_save_percentage?: number;
  pen_save_shrunk?: number;
  errors_leading_to_goal_per90?: number;
  errors_leading_to_goal_shrunk?: number;
  error_rate?: number;
  possession_differential?: number;
  is_eligible?: number;
  sample_reliability?: number;
  value_gap?: number;
}

// Outfield Metric Catalog
const OUTFIELD_CATALOG = [
  { field: 'league_difficulty_coef', label: 'League Difficulty Coef', category: 'Competition', defaultOp: '>=', defaultVal: 0.70, unit: 'x' },
  { field: 'age', label: 'Age', category: 'General', defaultOp: '<=', defaultVal: 23, unit: 'yrs' },
  { field: 'minutes_played', label: 'Minutes Played', category: 'General', defaultOp: '>=', defaultVal: 900, unit: 'mins' },
  { field: 'transfer_value_m', label: 'Transfer Value', category: 'Financials', defaultOp: '<=', defaultVal: 15, unit: '£M' },
  { field: 'wage_per_week_k', label: 'Wage Per Week', category: 'Financials', defaultOp: '<=', defaultVal: 20, unit: '£k' },
  { field: 'cost_per_xgi', label: 'Cost per xGI (Moneyball)', category: 'Financials', defaultOp: '<=', defaultVal: 20, unit: '£k' },
  { field: 'xgi_per_cost_m', label: 'xGI Output per £M (Moneyball)', category: 'Financials', defaultOp: '>=', defaultVal: 50.0, unit: '/£M' },
  { field: 'value_gap', label: 'Value Gap Index (Moneyball Gap)', category: 'Financials', defaultOp: '>=', defaultVal: 40.0, unit: '' },
  { field: 'npxg_plus_xa_per90', label: 'NP-xG + xA / 90', category: 'Attacking', defaultOp: '>=', defaultVal: 0.45, unit: '/90' },
  { field: 'non_penalty_xGoals_per90', label: 'Non-Penalty xG / 90', category: 'Attacking', defaultOp: '>=', defaultVal: 0.35, unit: '/90' },
  { field: 'xAssists_per90', label: 'Expected Assists (xA) / 90', category: 'Attacking', defaultOp: '>=', defaultVal: 0.20, unit: '/90' },
  { field: 'goals_minus_xg_per90', label: 'Goals - xG (Shrunk Delta)', category: 'Attacking', defaultOp: '>=', defaultVal: 0.05, unit: '/90' },
  { field: 'npxg_per_shot', label: 'NP-xG per Shot', category: 'Attacking', defaultOp: '>=', defaultVal: 0.12, unit: '' },
  { field: 'shots_per90', label: 'Shots / 90', category: 'Attacking', defaultOp: '>=', defaultVal: 2.5, unit: '/90' },
  { field: 'shot_on_target_rate', label: 'Shot on Target %', category: 'Attacking', defaultOp: '>=', defaultVal: 0.40, unit: '%' },
  { field: 'long_range_goal_share', label: 'Long Range Goal Share %', category: 'Attacking', defaultOp: '>=', defaultVal: 0.20, unit: '%' },
  { field: 'npxg_plus_xa_per90_adj', label: 'League-Adj NP-xGI / 90', category: 'Attacking', defaultOp: '>=', defaultVal: 0.35, unit: '/90' },
  { field: 'progressive_passes_per90', label: 'Prog. Passes / 90', category: 'Passing', defaultOp: '>=', defaultVal: 3.5, unit: '/90' },
  { field: 'progressive_actions_per90', label: 'Prog. Actions (Pass+Drib) / 90', category: 'Passing', defaultOp: '>=', defaultVal: 5.0, unit: '/90' },
  { field: 'open_play_key_passes_per90', label: 'Key Passes / 90', category: 'Passing', defaultOp: '>=', defaultVal: 0.8, unit: '/90' },
  { field: 'completed_crosses_per90', label: 'Completed Crosses / 90', category: 'Passing', defaultOp: '>=', defaultVal: 0.5, unit: '/90' },
  { field: 'completed_passes_per90', label: 'Completed Passes / 90', category: 'Passing', defaultOp: '>=', defaultVal: 35.0, unit: '/90' },
  { field: 'pass_accuracy', label: 'Pass Accuracy %', category: 'Passing', defaultOp: '>=', defaultVal: 0.82, unit: '%' },
  { field: 'dribbles_made_per90', label: 'Dribbles Made / 90', category: 'Possession', defaultOp: '>=', defaultVal: 1.5, unit: '/90' },
  { field: 'net_ball_security', label: 'Net Ball Security / 90', category: 'Possession', defaultOp: '>=', defaultVal: 20.0, unit: '/90' },
  { field: 'turnover_rate', label: 'Turnover Rate', category: 'Possession', defaultOp: '<=', defaultVal: 0.35, unit: '' },
  { field: 'defensive_actions_per90', label: 'Defensive Actions / 90', category: 'Defending', defaultOp: '>=', defaultVal: 3.5, unit: '/90' },
  { field: 'non_clearance_def_actions_per90', label: 'Non-Clearance Def Act / 90', category: 'Defending', defaultOp: '>=', defaultVal: 3.0, unit: '/90' },
  { field: 'duels_won_per90', label: 'Duels Won / 90 (Ground+Air)', category: 'Defending', defaultOp: '>=', defaultVal: 4.0, unit: '/90' },
  { field: 'tackles_attempted_per90', label: 'Tackles Attempted / 90', category: 'Defending', defaultOp: '>=', defaultVal: 2.0, unit: '/90' },
  { field: 'interceptions_per90', label: 'Interceptions / 90', category: 'Defending', defaultOp: '>=', defaultVal: 1.2, unit: '/90' },
  { field: 'pressure_accuracy', label: 'Press Success %', category: 'Defending', defaultOp: '>=', defaultVal: 25.0, unit: '%' },
  { field: 'aerial_duels_won_per90', label: 'Aerial Duels Won / 90', category: 'Aerial', defaultOp: '>=', defaultVal: 2.5, unit: '/90' },
  { field: 'heading_accuracy', label: 'Heading Accuracy %', category: 'Aerial', defaultOp: '>=', defaultVal: 0.50, unit: '%' },
  { field: 'sprints_per_km', label: 'Sprints per km (Intensity)', category: 'Physical', defaultOp: '>=', defaultVal: 0.60, unit: '/km' },
  { field: 'high_press_load', label: 'High Press Load Index', category: 'Physical', defaultOp: '>=', defaultVal: 8.0, unit: '' },
  { field: 'fouls_won_net', label: 'Net Fouls Drawn / 90', category: 'Physical', defaultOp: '>=', defaultVal: 0.5, unit: '/90' },
  { field: 'distance_covered_km_per90', label: 'Distance Covered / 90', category: 'Physical', defaultOp: '>=', defaultVal: 11.5, unit: 'km' },
];

// Goalkeeper Metric Catalog
const GK_CATALOG = [
  { field: 'league_difficulty_coef', label: 'League Difficulty Coef', category: 'Competition', defaultOp: '>=', defaultVal: 0.70, unit: 'x' },
  { field: 'age', label: 'Age', category: 'General', defaultOp: '<=', defaultVal: 28, unit: 'yrs' },
  { field: 'minutes_played', label: 'Minutes Played', category: 'General', defaultOp: '>=', defaultVal: 900, unit: 'mins' },
  { field: 'transfer_value_m', label: 'Transfer Value', category: 'Financials', defaultOp: '<=', defaultVal: 10, unit: '£M' },
  { field: 'wage_per_week_k', label: 'Wage Per Week', category: 'Financials', defaultOp: '<=', defaultVal: 20, unit: '£k' },
  { field: 'xgp_per_shot', label: 'xGP per Shot (Shot Prevention)', category: 'Shot Stopping', defaultOp: '>=', defaultVal: 0.05, unit: '/shot' },
  { field: 'xgp_per_shot_shrunk', label: 'xGP per Shot (Shrunk k=300)', category: 'Shot Stopping', defaultOp: '>=', defaultVal: 0.02, unit: '/shot' },
  { field: 'goals_vs_xgot_ratio', label: 'Goals Conceded / xGOT Ratio', category: 'Shot Stopping', defaultOp: '<=', defaultVal: 0.85, unit: '' },
  { field: 'xgp_per_season', label: 'Projected Goals Saved / Season', category: 'Shot Stopping', defaultOp: '>=', defaultVal: 4.0, unit: 'goals' },
  { field: 'xGoals_prevented_per90', label: 'xG Prevented / 90', category: 'Shot Stopping', defaultOp: '>=', defaultVal: 0.15, unit: '/90' },
  { field: 'saves_percentage', label: 'Save Percentage', category: 'Shot Stopping', defaultOp: '>=', defaultVal: 0.72, unit: '%' },
  { field: 'shot_difficulty', label: 'Shot Difficulty Faced (xGOT/Shot)', category: 'Workload & Quality', defaultOp: '>=', defaultVal: 0.35, unit: '' },
  { field: 'shots_faced_total', label: 'Shots Faced Total (Exposure)', category: 'Workload & Quality', defaultOp: '>=', defaultVal: 40, unit: 'shots' },
  { field: 'parry_rate', label: 'Parry Rate (Rebound Risk)', category: 'Handling', defaultOp: '<=', defaultVal: 0.65, unit: '' },
  { field: 'hold_rate_shrunk', label: 'Hold Rate (Shrunk k=150)', category: 'Handling', defaultOp: '>=', defaultVal: 0.30, unit: '' },
  { field: 'hold_rate_over_expected', label: 'Hold Rate Over Expected', category: 'Handling', defaultOp: '>=', defaultVal: 0.02, unit: '' },
  { field: 'hold_rate_pct', label: 'Hold Rate % (Rebound Control)', category: 'Handling', defaultOp: '>=', defaultVal: 30.0, unit: '%' },
  { field: 'sweeper_actions_per90', label: 'Sweeper Actions / 90 (Int+Clr)', category: 'Sweeping & Aerial', defaultOp: '>=', defaultVal: 7.5, unit: '/90' },
  { field: 'sweeper_share', label: 'Proactive Sweeper Share', category: 'Sweeping & Aerial', defaultOp: '>=', defaultVal: 0.05, unit: '' },
  { field: 'completed_passes_per90', label: 'Completed Passes / 90', category: 'Distribution', defaultOp: '>=', defaultVal: 12.0, unit: '/90' },
  { field: 'giveaways_per90', label: 'Giveaways / 90 (Lost Buildup)', category: 'Distribution', defaultOp: '<=', defaultVal: 8.0, unit: '/90' },
  { field: 'pass_accuracy', label: 'Pass Accuracy %', category: 'Distribution', defaultOp: '>=', defaultVal: 0.65, unit: '%' },
  { field: 'pen_save_shrunk', label: 'Penalty Save % (Shrunk k=20)', category: 'Specialist', defaultOp: '>=', defaultVal: 0.20, unit: '%' },
  { field: 'sample_reliability', label: 'Sample Reliability Factor', category: 'General', defaultOp: '>=', defaultVal: 0.25, unit: '' },
  { field: 'value_gap', label: 'Value Gap Index (Moneyball Gap)', category: 'Financials', defaultOp: '>=', defaultVal: 40.0, unit: '' },
];


// Presets
const OUTFIELD_PRESETS: Record<string, { label: string; filters: ScreenerFilter[]; defaultSort: string }> = {
  default: {
    label: 'Default (All Qualified)',
    filters: [{ field: 'minutes_played', operator: '>=', value: 450 }],
    defaultSort: 'npxg_plus_xa_per90',
  },
  wonderkids: {
    label: '🌟 U21 Wonderkids (Age ≤ 21, xGI ≥ 0.35)',
    filters: [
      { field: 'age', operator: '<=', value: 21 },
      { field: 'npxg_plus_xa_per90', operator: '>=', value: 0.35 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'npxg_plus_xa_per90',
  },
  moneyball: {
    label: '💎 Moneyball Value Gems (Cost/xGI ≤ £5k, Val ≤ £15M)',
    filters: [
      { field: 'cost_per_xgi', operator: '<=', value: 5000 },
      { field: 'transfer_value_m', operator: '<=', value: 15 },
      { field: 'npxg_plus_xa_per90', operator: '>=', value: 0.40 },
      { field: 'minutes_played', operator: '>=', value: 900 },
    ],
    defaultSort: 'cost_per_xgi',
  },
  value_gap: {
    label: '💎 Extreme Value Gaps (Value Gap ≥ +50, Val ≤ £15M)',
    filters: [
      { field: 'value_gap', operator: '>=', value: 50.0 },
      { field: 'transfer_value_m', operator: '<=', value: 15.0 },
      { field: 'minutes_played', operator: '>=', value: 900 },
    ],
    defaultSort: 'value_gap',
  },
  playmakers: {
    label: '🎯 Elite Playmakers (xA ≥ 0.20, PrPass ≥ 3.5)',
    filters: [
      { field: 'xAssists_per90', operator: '>=', value: 0.20 },
      { field: 'progressive_passes_per90', operator: '>=', value: 3.5 },
      { field: 'minutes_played', operator: '>=', value: 900 },
    ],
    defaultSort: 'xAssists_per90',
  },
  pressing: {
    label: '⚔️ High-Press Outfielders (Press Acc ≥ 25%, DefAct ≥ 3.5)',
    filters: [
      { field: 'pressure_accuracy', operator: '>=', value: 25.0 },
      { field: 'defensive_actions_per90', operator: '>=', value: 3.5 },
      { field: 'minutes_played', operator: '>=', value: 900 },
    ],
    defaultSort: 'defensive_actions_per90',
  },
  finishing: {
    label: '🎯 Clinical Finishers (Goals-xG Shrunk ≥ 0.05, NP-xG ≥ 0.25)',
    filters: [
      { field: 'goals_minus_xg_per90', operator: '>=', value: 0.05 },
      { field: 'non_penalty_xGoals_per90', operator: '>=', value: 0.25 },
      { field: 'minutes_played', operator: '>=', value: 900 },
    ],
    defaultSort: 'goals_minus_xg_per90',
  },
  ball_winners: {
    label: '🛡️ Ball Winners & Duelists (Duels Won ≥ 5.0, Non-Clr DefAct ≥ 3.0)',
    filters: [
      { field: 'duels_won_per90', operator: '>=', value: 5.0 },
      { field: 'non_clearance_def_actions_per90', operator: '>=', value: 3.0 },
      { field: 'minutes_played', operator: '>=', value: 900 },
    ],
    defaultSort: 'duels_won_per90',
  },
  aerial: {
    label: '🏰 Aerial Monsters (Aerial Won ≥ 2.5, Hdr% ≥ 50%)',
    filters: [
      { field: 'aerial_duels_won_per90', operator: '>=', value: 2.5 },
      { field: 'heading_accuracy', operator: '>=', value: 0.50 },
      { field: 'minutes_played', operator: '>=', value: 900 },
    ],
    defaultSort: 'aerial_duels_won_per90',
  },
  flank_progression: {
    label: '⚡ Flank Progression & Crosses (Prog Actions ≥ 5.0, Crosses ≥ 0.4)',
    filters: [
      { field: 'progressive_actions_per90', operator: '>=', value: 5.0 },
      { field: 'completed_crosses_per90', operator: '>=', value: 0.4 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'progressive_actions_per90',
  },
  elite_competition: {
    label: '🏆 Tier 1 Elite Leagues (League Coef ≥ 0.90x)',
    filters: [
      { field: 'league_difficulty_coef', operator: '>=', value: 0.90 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'npxg_plus_xa_per90',
  },
};

const GK_PRESETS: Record<string, { label: string; filters: ScreenerFilter[]; defaultSort: string }> = {
  gk_default: {
    label: 'Default (All Qualified Keepers)',
    filters: [{ field: 'minutes_played', operator: '>=', value: 450 }],
    defaultSort: 'xgp_per_shot_shrunk',
  },
  gk_shotstoppers: {
    label: '🧤 Elite Shot-Stoppers (xGP/Shot Shrunk ≥ 0.02, Goals/xGOT ≤ 0.85)',
    filters: [
      { field: 'xgp_per_shot_shrunk', operator: '>=', value: 0.02 },
      { field: 'goals_vs_xgot_ratio', operator: '<=', value: 0.85 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'xgp_per_shot_shrunk',
  },
  gk_sweepers: {
    label: '🧹 Proactive Sweeper-Keepers (Sweeper Actions ≥ 7.0/90, Pass Acc ≥ 70%)',
    filters: [
      { field: 'sweeper_actions_per90', operator: '>=', value: 7.0 },
      { field: 'pass_accuracy', operator: '>=', value: 0.70 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'sweeper_actions_per90',
  },
  gk_bargains: {
    label: '💎 Bargain Keepers (Valuation ≤ £5M, Projected Goals Saved ≥ 3.0)',
    filters: [
      { field: 'transfer_value_m', operator: '<=', value: 5.0 },
      { field: 'xgp_per_season', operator: '>=', value: 3.0 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'xgp_per_season',
  },
  gk_rebound: {
    label: '🛡️ Safe Hands & Handling (Hold Over Exp ≥ 0, Parry Rate ≤ 0.65)',
    filters: [
      { field: 'hold_rate_over_expected', operator: '>=', value: 0.0 },
      { field: 'parry_rate', operator: '<=', value: 0.65 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'hold_rate_over_expected',
  },
  gk_penalties: {
    label: '🧤 Penalty Specialists (Pen Save Shrunk ≥ 20%)',
    filters: [
      { field: 'pen_save_shrunk', operator: '>=', value: 0.20 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'pen_save_shrunk',
  },
  gk_elite_competition: {
    label: '🏆 Tier 1 Elite Leagues (League Coef ≥ 0.90x)',
    filters: [
      { field: 'league_difficulty_coef', operator: '>=', value: 0.90 },
      { field: 'minutes_played', operator: '>=', value: 450 },
    ],
    defaultSort: 'xgp_per_shot_shrunk',
  },
};

const OUTFIELD_TABS = ['Overview', 'Attacking', 'Creation', 'Defending', 'Physical', 'Valuation'];
const GK_TABS = ['Overview', 'Shot Stopping', 'Handling & Sweeping', 'Distribution', 'Valuation'];

function ScreenerContent() {
  const searchParams = useSearchParams();
  const modeParam = searchParams.get('mode');
  const presetParam = searchParams.get('preset');
  const posParam = searchParams.get('position');
  const sortParam = searchParams.get('sortBy');
  const sortOrderParam = searchParams.get('sortOrder');
  const tabParam = searchParams.get('tab');
  const fromParam = searchParams.get('from');

  const initialIsGK = modeParam === 'gk';
  const [targetType, setTargetType] = useState<'outfield' | 'goalkeeper'>(
    initialIsGK ? 'goalkeeper' : 'outfield'
  );
  const isGK = targetType === 'goalkeeper';

  const presets = isGK ? GK_PRESETS : OUTFIELD_PRESETS;
  const metricCatalog = isGK ? GK_CATALOG : OUTFIELD_CATALOG;
  const viewTabs = isGK ? GK_TABS : OUTFIELD_TABS;

  const [currentPreset, setCurrentPreset] = useState<string>(() => {
    if (initialIsGK) {
      return presetParam && GK_PRESETS[presetParam] ? presetParam : 'gk_default';
    }
    return presetParam && OUTFIELD_PRESETS[presetParam] ? presetParam : 'default';
  });

  const [activeFilters, setActiveFilters] = useState<ScreenerFilter[]>(() => {
    if (initialIsGK) {
      const pKey = presetParam && GK_PRESETS[presetParam] ? presetParam : 'gk_default';
      return [...GK_PRESETS[pKey].filters];
    }
    const pKey = presetParam && OUTFIELD_PRESETS[presetParam] ? presetParam : 'default';
    return [...OUTFIELD_PRESETS[pKey].filters];
  });

  const [selectedPosition, setSelectedPosition] = useState<string>(
    posParam || (initialIsGK ? 'Goalkeeper' : 'ALL')
  );
  const [selectedCoef, setSelectedCoef] = useState<string>('ALL');
  const [search, setSearch] = useState<string>('');
  const [activeTab, setActiveTab] = useState<string>(tabParam || 'Overview');
  const [sortBy, setSortBy] = useState<string>(() => {
    if (sortParam) return sortParam;
    if (initialIsGK) {
      const pKey = presetParam && GK_PRESETS[presetParam] ? presetParam : 'gk_default';
      return GK_PRESETS[pKey].defaultSort;
    }
    const pKey = presetParam && OUTFIELD_PRESETS[presetParam] ? presetParam : 'default';
    return OUTFIELD_PRESETS[pKey].defaultSort;
  });
  const [sortOrder, setSortOrder] = useState<string>(sortOrderParam || 'DESC');
  const [diagnosisContext, setDiagnosisContext] = useState<string | null>(fromParam || null);
  const [page, setPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(30);

  const [players, setPlayers] = useState<PlayerRow[]>([]);
  const [totalCount, setTotalCount] = useState<number>(0);
  const [totalPages, setTotalPages] = useState<number>(1);
  const [loading, setLoading] = useState<boolean>(true);

  // Synchronize when URL searchParams update
  useEffect(() => {
    const mode = searchParams.get('mode');
    const preset = searchParams.get('preset');
    const pos = searchParams.get('position');
    const sort = searchParams.get('sortBy');
    const order = searchParams.get('sortOrder');
    const tab = searchParams.get('tab');
    const from = searchParams.get('from');

    if (mode || preset || pos || from) {
      const isModeGK = mode === 'gk';
      setTargetType(isModeGK ? 'goalkeeper' : 'outfield');

      const pKey = isModeGK
        ? preset && GK_PRESETS[preset] ? preset : 'gk_default'
        : preset && OUTFIELD_PRESETS[preset] ? preset : 'default';

      setCurrentPreset(pKey);
      const pObj = isModeGK ? GK_PRESETS[pKey] : OUTFIELD_PRESETS[pKey];
      if (pObj) {
        setActiveFilters([...pObj.filters]);
        setSortBy(sort || pObj.defaultSort);
      }
      setSelectedPosition(pos || (isModeGK ? 'Goalkeeper' : 'ALL'));
      if (order) setSortOrder(order);
      if (tab) setActiveTab(tab);
      setDiagnosisContext(from || null);
      setPage(1);
    }
  }, [searchParams]);

  // Popover & modal
  const [showAddFilterModal, setShowAddFilterModal] = useState<boolean>(false);
  const [editingFilterIdx, setEditingFilterIdx] = useState<number | null>(null);
  const [editOp, setEditOp] = useState<string>('>=');
  const [editVal, setEditVal] = useState<string>('');
  const popoverRef = useRef<HTMLDivElement>(null);

  // Close editing popover on outside click
  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        setEditingFilterIdx(null);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Fetch screened data
  const fetchScreenedPlayers = async () => {
    setLoading(true);
    try {
      const effectiveFilters = [...activeFilters];
      if (!isGK && selectedPosition !== 'ALL') {
        if (selectedPosition === 'FLANK_DEF') {
          effectiveFilters.push({
            field: 'position',
            operator: 'in',
            value: ['Left Defender', 'Right Defender', 'Left Wingback', 'Right Wingback'],
          });
        } else {
          effectiveFilters.push({
            field: 'position',
            operator: '=',
            value: selectedPosition,
          });
        }
      }

      if (selectedCoef !== 'ALL') {
        effectiveFilters.push({
          field: 'league_difficulty_coef',
          operator: '>=',
          value: parseFloat(selectedCoef),
        });
      }

      const res = await fetch('/api/screener', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          targetType,
          filters: effectiveFilters,
          search,
          sortBy,
          sortOrder,
          page,
          pageSize,
        }),
      });

      const json = await res.json();
      setPlayers(json.players || []);
      if (json.pagination) {
        setTotalCount(json.pagination.total);
        setTotalPages(json.pagination.totalPages);
      }
    } catch (err) {
      console.error('Screener fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const timer = setTimeout(() => {
      fetchScreenedPlayers();
    }, 200);
    return () => clearTimeout(timer);
  }, [targetType, activeFilters, selectedPosition, selectedCoef, search, sortBy, sortOrder, page, pageSize]);

  // Handle Target Type Switch (Outfield vs Goalkeeper)
  const handleSwitchTargetType = (type: 'outfield' | 'goalkeeper') => {
    setTargetType(type);
    const newIsGK = type === 'goalkeeper';
    const newPresetKey = newIsGK ? 'gk_default' : 'default';
    const newPreset = newIsGK ? GK_PRESETS.gk_default : OUTFIELD_PRESETS.default;

    setCurrentPreset(newPresetKey);
    setActiveFilters([...newPreset.filters]);
    setSortBy(newPreset.defaultSort);
    setSortOrder('DESC');
    setActiveTab('Overview');
    setSelectedPosition(newIsGK ? 'Goalkeeper' : 'ALL');
    setSelectedCoef('ALL');
    setPage(1);
  };

  // Handle Preset selection
  const handleSelectPreset = (presetKey: string) => {
    setCurrentPreset(presetKey);
    const preset = presets[presetKey];
    if (preset) {
      setActiveFilters([...preset.filters]);
      setSortBy(preset.defaultSort);
      setSortOrder('DESC');
      setPage(1);
    }
  };

  // Add new filter
  const handleAddFilter = (metric: typeof OUTFIELD_CATALOG[0]) => {
    const exists = activeFilters.some((f) => f.field === metric.field);
    if (!exists) {
      setActiveFilters([
        ...activeFilters,
        {
          field: metric.field,
          operator: metric.defaultOp as any,
          value: metric.defaultVal,
        },
      ]);
    }
    setShowAddFilterModal(false);
    setPage(1);
  };

  // Remove filter
  const handleRemoveFilter = (index: number) => {
    setActiveFilters(activeFilters.filter((_, i) => i !== index));
    setPage(1);
  };

  // Open edit popover
  const handleStartEditFilter = (index: number) => {
    const f = activeFilters[index];
    setEditingFilterIdx(index);
    setEditOp(f.operator);
    setEditVal(f.value.toString());
  };

  // Save edit filter
  const handleSaveEditFilter = () => {
    if (editingFilterIdx === null) return;
    const num = parseFloat(editVal);
    if (!isNaN(num)) {
      const updated = [...activeFilters];
      updated[editingFilterIdx] = {
        ...updated[editingFilterIdx],
        operator: editOp as any,
        value: num,
      };
      setActiveFilters(updated);
    }
    setEditingFilterIdx(null);
    setPage(1);
  };

  // Sorting
  const handleSort = (col: string) => {
    if (sortBy === col) {
      setSortOrder(sortOrder === 'DESC' ? 'ASC' : 'DESC');
    } else {
      setSortBy(col);
      setSortOrder('DESC');
    }
    setPage(1);
  };

  const renderSortTh = (
    colKey: string,
    label: string,
    align: 'left' | 'right' | 'center' = 'right',
    minWidth?: string
  ) => {
    const isSorted = sortBy === colKey;
    return (
      <th
        onClick={() => handleSort(colKey)}
        style={{
          textAlign: align,
          cursor: 'pointer',
          color: isSorted ? '#fbbf24' : 'var(--text-muted)',
          fontWeight: isSorted ? 700 : 600,
          background: isSorted ? 'rgba(251, 191, 36, 0.08)' : undefined,
          borderBottom: isSorted ? '2px solid #fbbf24' : undefined,
          minWidth: minWidth,
          userSelect: 'none',
          transition: 'all 0.15s ease',
        }}
        title={`Sort by ${label} (${isSorted ? (sortOrder === 'DESC' ? 'High to Low (click to reverse)' : 'Low to High (click to reverse)') : 'Click to sort'})`}
      >
        <div
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.35rem',
            justifyContent: align === 'right' ? 'flex-end' : align === 'center' ? 'center' : 'flex-start',
            width: '100%',
          }}
        >
          <span>{label}</span>
          {isSorted ? (
            sortOrder === 'DESC' ? (
              <ArrowDown size={12} color="#fbbf24" />
            ) : (
              <ArrowUp size={12} color="#fbbf24" />
            )
          ) : (
            <ArrowUpDown size={11} style={{ opacity: 0.3 }} />
          )}
        </div>
      </th>
    );
  };

  const getTdSortStyle = (colKey: string, defaultColor?: string) => {
    const isSorted = sortBy === colKey;
    return {
      textAlign: 'right' as const,
      fontWeight: isSorted ? 700 : undefined,
      color: isSorted ? '#fbbf24' : (defaultColor || undefined),
      background: isSorted ? 'rgba(251, 191, 36, 0.05)' : undefined,
    };
  };

  // CSV Export
  const handleExportCSV = () => {
    if (players.length === 0) return;
    const headers = isGK
      ? ['Player', 'Club', 'League', 'Age', 'Minutes', 'Value (£M)', 'Wage (£k)', 'xG Prevented/90', 'Save %', 'Shot-Stop Delta', 'Hold Rate %', 'Sweeper Index', 'Pass Acc %']
      : ['Player', 'Position', 'Club', 'League', 'Age', 'Minutes', 'Value (£M)', 'Wage (£k)', 'NP-xG/90', 'xA/90', 'NP-xG+xA/90', 'Prog Passes/90', 'Def Actions/90'];

    const rows = players.map((p) =>
      isGK
        ? [
            `"${p.player_name}"`,
            `"${p.club_name}"`,
            `"${p.league}"`,
            p.age,
            p.minutes_played,
            p.transfer_value_m || 0,
            p.wage_per_week_k || 0,
            p.xGoals_prevented_per90 || 0,
            p.saves_percentage || 0,
            p.shot_stopping_delta || 0,
            p.hold_rate_pct || 0,
            p.sweeper_index || 0,
            p.pass_accuracy || 0,
          ]
        : [
            `"${p.player_name}"`,
            `"${p.position}"`,
            `"${p.club_name}"`,
            `"${p.league}"`,
            p.age,
            p.minutes_played,
            p.transfer_value_m || 0,
            p.wage_per_week_k || 0,
            p.non_penalty_xGoals_per90 || 0,
            p.xAssits_per90 || 0,
            p.npxg_plus_xa_per90 || 0,
            p.progressive_passes_per90 || 0,
            p.defensive_actions_per90 || 0,
          ]
    );

    const csvContent = 'data:text/csv;charset=utf-8,' + [headers.join(','), ...rows.map((r) => r.join(','))].join('\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    link.setAttribute('download', `fm_scouted_${targetType}_${activeTab.toLowerCase()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const getMetricLabel = (field: string) => {
    const found = metricCatalog.find((m) => m.field === field);
    return found ? found.label : field.replace(/_/g, ' ');
  };

  return (
    <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '1.25rem' }}>
      {/* 1. TOP BAR: Mode Switcher (Outfield vs GK), Screen Presets, Actions */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
          gap: '1rem',
          borderBottom: '1px solid rgba(255, 255, 255, 0.08)',
          paddingBottom: '0.85rem',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '1.25rem', flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              PRO QUANT TERMINAL
            </div>
            <h1 style={{ fontSize: '1.6rem', fontWeight: 800, color: '#fff', letterSpacing: '-0.02em', display: 'flex', alignItems: 'center', gap: '0.45rem' }}>
              <span>Player Screener</span>
            </h1>
          </div>

          {/* TARGET TYPE SWITCHER (Outfield vs Goalkeeper) */}
          <div style={{ display: 'flex', background: 'rgba(255, 255, 255, 0.06)', padding: '3px', borderRadius: '8px', border: '1px solid rgba(255, 255, 255, 0.1)' }}>
            <button
              onClick={() => handleSwitchTargetType('outfield')}
              style={{
                padding: '0.4rem 0.85rem',
                fontSize: '0.8rem',
                fontWeight: 700,
                borderRadius: '6px',
                color: !isGK ? '#fff' : 'var(--text-muted)',
                background: !isGK ? '#0284c7' : 'transparent',
                display: 'flex',
                alignItems: 'center',
                gap: '0.35rem',
                transition: 'all 0.15s ease',
              }}
            >
              <span>⚽ Outfield Players</span>
              <span style={{ fontSize: '0.68rem', opacity: 0.8 }}>10,504</span>
            </button>
            <button
              onClick={() => handleSwitchTargetType('goalkeeper')}
              style={{
                padding: '0.4rem 0.85rem',
                fontSize: '0.8rem',
                fontWeight: 700,
                borderRadius: '6px',
                color: isGK ? '#fff' : 'var(--text-muted)',
                background: isGK ? 'linear-gradient(135deg, #10b981, #059669)' : 'transparent',
                display: 'flex',
                alignItems: 'center',
                gap: '0.35rem',
                transition: 'all 0.15s ease',
              }}
            >
              <span>🧤 Goalkeepers</span>
              <span style={{ fontSize: '0.68rem', opacity: 0.8 }}>855</span>
            </button>
          </div>

          {/* Preset Selector Dropdown */}
          <div style={{ position: 'relative' }}>
            <select
              value={currentPreset}
              onChange={(e) => handleSelectPreset(e.target.value)}
              style={{
                background: 'rgba(255, 255, 255, 0.05)',
                border: '1px solid rgba(255, 255, 255, 0.15)',
                borderRadius: '8px',
                padding: '0.45rem 2rem 0.45rem 0.85rem',
                color: '#fff',
                fontWeight: 700,
                fontSize: '0.85rem',
                cursor: 'pointer',
                appearance: 'none',
              }}
            >
              {Object.entries(presets).map(([k, p]) => (
                <option key={k} value={k} style={{ background: '#131722', color: '#fff' }}>
                  {p.label}
                </option>
              ))}
            </select>
            <ChevronDown size={14} style={{ position: 'absolute', right: '0.75rem', top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', color: 'var(--text-dim)' }} />
          </div>
        </div>

        {/* Right Top Actions */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <button
            onClick={() => {
              setDiagnosisContext(null);
              setSelectedPosition(isGK ? 'Goalkeeper' : 'ALL');
              setSelectedCoef('ALL');
              handleSelectPreset(isGK ? 'gk_default' : 'default');
            }}
            className="btn-secondary"
            style={{ padding: '0.4rem 0.75rem', fontSize: '0.78rem' }}
            title="Reset filters to default"
          >
            <RotateCcw size={14} />
            <span>Reset</span>
          </button>
          <button
            onClick={handleExportCSV}
            className="btn-primary"
            style={{ padding: '0.4rem 0.85rem', fontSize: '0.78rem' }}
            title="Export filtered players to CSV"
          >
            <Download size={14} />
            <span>Export CSV</span>
          </button>
        </div>
      </div>

      {/* TACTICAL DIAGNOSIS SOLUTION BANNER */}
      {diagnosisContext && (
        <div
          style={{
            background: 'linear-gradient(135deg, rgba(2, 132, 199, 0.16), rgba(99, 102, 241, 0.16))',
            border: '1px solid rgba(56, 189, 248, 0.35)',
            borderRadius: '10px',
            padding: '0.85rem 1.25rem',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '0.75rem',
            boxShadow: '0 4px 20px rgba(0, 0, 0, 0.25)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem' }}>
            <div
              style={{
                width: '36px',
                height: '36px',
                borderRadius: '8px',
                background: 'rgba(56, 189, 248, 0.2)',
                color: '#38bdf8',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexShrink: 0,
              }}
            >
              <Sparkles size={18} />
            </div>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
                <span style={{ fontSize: '0.72rem', fontWeight: 800, color: '#38bdf8', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  ⚡ Tactical Diagnosis Solution Active
                </span>
                <span style={{ fontSize: '0.72rem', padding: '1px 8px', borderRadius: '4px', background: 'rgba(56, 189, 248, 0.2)', color: '#bae6fd', fontWeight: 700 }}>
                  {diagnosisContext}
                </span>
              </div>
              <div style={{ fontSize: '0.825rem', color: '#e2e8f0', marginTop: '2px' }}>
                Target Profile: <strong style={{ color: '#fff' }}>{presets[currentPreset]?.label || currentPreset}</strong>
                {selectedPosition !== 'ALL' && (
                  <span> • Position: <strong style={{ color: '#38bdf8' }}>{selectedPosition === 'FLANK_DEF' ? 'Fullbacks & Wingbacks' : selectedPosition}</strong></span>
                )}
                {sortBy && (
                  <span> • Sorted by: <strong style={{ color: '#fbbf24' }}>{getMetricLabel(sortBy)}</strong> ({sortOrder === 'DESC' ? 'High → Low' : 'Low → High'})</span>
                )}
              </div>
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
            <Link
              href="/squad-diagnosis"
              className="btn-secondary"
              style={{ padding: '0.4rem 0.85rem', fontSize: '0.78rem', textDecoration: 'none' }}
            >
              ← Back to Diagnosis
            </Link>
            <button
              onClick={() => {
                setDiagnosisContext(null);
                setSelectedPosition(isGK ? 'Goalkeeper' : 'ALL');
                setSelectedCoef('ALL');
                handleSelectPreset(isGK ? 'gk_default' : 'default');
              }}
              className="btn-secondary"
              style={{ padding: '0.4rem 0.85rem', fontSize: '0.78rem', background: 'rgba(255,255,255,0.08)' }}
              title="Reset all filters to default screener"
            >
              <RotateCcw size={13} />
              <span>Reset Filters</span>
            </button>
          </div>
        </div>
      )}

      {/* 2. TRADINGVIEW FILTER CHIP PILLS BAR */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem',
          overflowX: 'auto',
          paddingBottom: '0.65rem',
          scrollbarWidth: 'thin',
        }}
        id="tradingview-filter-bar"
      >
        {/* Search Input pill */}
        <div style={{ position: 'relative', minWidth: '180px' }}>
          <Search size={13} style={{ position: 'absolute', left: '0.65rem', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-dim)' }} />
          <input
            type="text"
            placeholder={isGK ? 'Search keeper or club...' : 'Search player or club...'}
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setPage(1);
            }}
            style={{
              width: '100%',
              background: 'rgba(255, 255, 255, 0.04)',
              border: '1px solid rgba(255, 255, 255, 0.12)',
              borderRadius: '9999px',
              padding: '0.35rem 0.65rem 0.35rem 1.85rem',
              color: '#fff',
              fontSize: '0.78rem',
            }}
          />
        </div>

        {/* Position Dropdown Chip (only in Outfield mode) */}
        {!isGK && (
          <div style={{ position: 'relative' }}>
            <select
              value={selectedPosition}
              onChange={(e) => {
                setSelectedPosition(e.target.value);
                setPage(1);
              }}
              style={{
                background: selectedPosition !== 'ALL' ? 'rgba(56, 189, 248, 0.15)' : 'rgba(255, 255, 255, 0.05)',
                border: selectedPosition !== 'ALL' ? '1px solid #38bdf8' : '1px solid rgba(255, 255, 255, 0.12)',
                borderRadius: '9999px',
                padding: '0.35rem 1.75rem 0.35rem 0.85rem',
                color: selectedPosition !== 'ALL' ? '#38bdf8' : 'var(--text-muted)',
                fontSize: '0.78rem',
                fontWeight: 600,
                cursor: 'pointer',
                appearance: 'none',
                whiteSpace: 'nowrap',
              }}
            >
              <option value="ALL" style={{ background: '#131722', color: '#fff' }}>Position: All Outfield</option>
              <option value="FLANK_DEF" style={{ background: '#131722', color: '#38bdf8', fontWeight: 700 }}>⚡ Fullbacks &amp; Wingbacks (L/R DF &amp; WB)</option>
              <optgroup label="Defenders" style={{ background: '#131722', color: '#94a3b8' }}>
                <option value="Central Defender" style={{ background: '#131722', color: '#fff' }}>Central Defender</option>
                <option value="Left Defender" style={{ background: '#131722', color: '#fff' }}>Left Defender</option>
                <option value="Right Defender" style={{ background: '#131722', color: '#fff' }}>Right Defender</option>
                <option value="Left Wingback" style={{ background: '#131722', color: '#fff' }}>Left Wingback</option>
                <option value="Right Wingback" style={{ background: '#131722', color: '#fff' }}>Right Wingback</option>
              </optgroup>
              <optgroup label="Midfielders" style={{ background: '#131722', color: '#94a3b8' }}>
                <option value="Defensive Midfielder" style={{ background: '#131722', color: '#fff' }}>Defensive Midfielder</option>
                <option value="Central Midfielder" style={{ background: '#131722', color: '#fff' }}>Central Midfielder</option>
                <option value="Central Attacking Midfielder" style={{ background: '#131722', color: '#fff' }}>Central Attacking Midfielder</option>
                <option value="Left Midfielder" style={{ background: '#131722', color: '#fff' }}>Left Midfielder</option>
                <option value="Right Midfielder" style={{ background: '#131722', color: '#fff' }}>Right Midfielder</option>
              </optgroup>
              <optgroup label="Attackers &amp; Wingers" style={{ background: '#131722', color: '#94a3b8' }}>
                <option value="Striker" style={{ background: '#131722', color: '#fff' }}>Striker</option>
                <option value="Left Attacking Midfielder" style={{ background: '#131722', color: '#fff' }}>Left Winger / AM</option>
                <option value="Right Attacking Midfielder" style={{ background: '#131722', color: '#fff' }}>Right Winger / AM</option>
              </optgroup>
            </select>
            <ChevronDown size={12} style={{ position: 'absolute', right: '0.65rem', top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', color: 'var(--text-dim)' }} />
          </div>
        )}

        {/* League Difficulty Coef Dropdown Chip */}
        <div style={{ position: 'relative' }}>
          <select
            value={selectedCoef}
            onChange={(e) => {
              setSelectedCoef(e.target.value);
              setPage(1);
            }}
            style={{
              background: selectedCoef !== 'ALL' ? 'rgba(16, 185, 129, 0.15)' : 'rgba(255, 255, 255, 0.05)',
              border: selectedCoef !== 'ALL' ? '1px solid #10b981' : '1px solid rgba(255, 255, 255, 0.12)',
              borderRadius: '9999px',
              padding: '0.35rem 1.75rem 0.35rem 0.85rem',
              color: selectedCoef !== 'ALL' ? '#34d399' : 'var(--text-muted)',
              fontSize: '0.78rem',
              fontWeight: 600,
              cursor: 'pointer',
              appearance: 'none',
              whiteSpace: 'nowrap',
            }}
          >
            <option value="ALL" style={{ background: '#131722', color: '#fff' }}>League Coef: All Levels</option>
            <option value="0.9" style={{ background: '#131722', color: '#34d399', fontWeight: 700 }}>🏆 Tier 1 Elite (≥ 0.90x)</option>
            <option value="0.7" style={{ background: '#131722', color: '#fff' }}>🥈 Tier 2 &amp; Above (≥ 0.70x)</option>
            <option value="0.6" style={{ background: '#131722', color: '#fff' }}>🥉 Tier 3 &amp; Above (≥ 0.60x)</option>
            <option value="0.4" style={{ background: '#131722', color: '#fff' }}>⚽ Semi-Pro &amp; Above (≥ 0.40x)</option>
          </select>
          <ChevronDown size={12} style={{ position: 'absolute', right: '0.65rem', top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', color: 'var(--text-dim)' }} />
        </div>

        {/* ACTIVE FILTER PILLS */}
        {activeFilters.map((f, idx) => (
          <div
            key={`${f.field}-${idx}`}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '0.35rem',
              background: '#1a2233',
              border: '1px solid rgba(255, 255, 255, 0.4)',
              borderRadius: '9999px',
              padding: '0.3rem 0.5rem 0.3rem 0.75rem',
              fontSize: '0.78rem',
              color: '#fff',
              whiteSpace: 'nowrap',
              boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
            }}
          >
            <span
              onClick={() => handleStartEditFilter(idx)}
              style={{ cursor: 'pointer', fontWeight: 600 }}
              title="Click to edit threshold"
            >
              {getMetricLabel(f.field)}{' '}
              <strong style={{ color: isGK ? '#34d399' : '#38bdf8' }}>
                {f.operator} {f.value}
              </strong>
            </span>
            <button
              onClick={() => handleRemoveFilter(idx)}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: '16px',
                height: '16px',
                borderRadius: '50%',
                background: 'rgba(255, 255, 255, 0.1)',
                color: 'var(--text-muted)',
                cursor: 'pointer',
                marginLeft: '2px',
              }}
              title="Remove filter"
            >
              <X size={10} />
            </button>
          </div>
        ))}

        {/* '+ Add Filter' Button Chip */}
        <button
          onClick={() => setShowAddFilterModal(true)}
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.3rem',
            background: 'rgba(255, 255, 255, 0.05)',
            border: '1px dashed rgba(255, 255, 255, 0.25)',
            borderRadius: '9999px',
            padding: '0.35rem 0.75rem',
            fontSize: '0.78rem',
            fontWeight: 600,
            color: isGK ? '#34d399' : '#38bdf8',
            cursor: 'pointer',
            whiteSpace: 'nowrap',
          }}
        >
          <Plus size={13} />
          <span>{isGK ? 'Add GK Metric' : 'Add Outfield Metric'}</span>
        </button>
      </div>

      {/* FILTER EDIT POPOVER */}
      {editingFilterIdx !== null && (
        <div
          ref={popoverRef}
          style={{
            position: 'absolute',
            zIndex: 100,
            background: '#161e31',
            border: `1px solid ${isGK ? '#10b981' : '#38bdf8'}`,
            borderRadius: '10px',
            padding: '1rem',
            boxShadow: '0 12px 32px rgba(0,0,0,0.6)',
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
          }}
        >
          <span style={{ fontSize: '0.8rem', fontWeight: 600, color: '#fff' }}>
            {getMetricLabel(activeFilters[editingFilterIdx].field)}:
          </span>
          <select
            value={editOp}
            onChange={(e) => setEditOp(e.target.value)}
            style={{
              background: 'var(--bg-input)',
              border: '1px solid var(--border-subtle)',
              borderRadius: '6px',
              color: '#fff',
              padding: '0.3rem 0.5rem',
              fontSize: '0.8rem',
            }}
          >
            <option value=">=">&gt;=</option>
            <option value="<=">&lt;=</option>
            <option value=">">&gt;</option>
            <option value="<">&lt;</option>
            <option value="=">=</option>
          </select>
          <input
            type="number"
            step="any"
            value={editVal}
            onChange={(e) => setEditVal(e.target.value)}
            style={{
              width: '80px',
              background: 'var(--bg-input)',
              border: '1px solid var(--border-subtle)',
              borderRadius: '6px',
              color: '#fff',
              padding: '0.3rem 0.5rem',
              fontSize: '0.8rem',
            }}
          />
          <button
            onClick={handleSaveEditFilter}
            className="btn-primary"
            style={{ padding: '0.35rem 0.65rem', fontSize: '0.78rem' }}
          >
            Apply
          </button>
          <button
            onClick={() => setEditingFilterIdx(null)}
            className="btn-secondary"
            style={{ padding: '0.35rem 0.65rem', fontSize: '0.78rem' }}
          >
            Cancel
          </button>
        </div>
      )}

      {/* '+ ADD FILTER' CATALOG MODAL */}
      {showAddFilterModal && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(0, 0, 0, 0.75)',
            backdropFilter: 'blur(8px)',
            zIndex: 1000,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: '1.5rem',
          }}
          onClick={() => setShowAddFilterModal(false)}
        >
          <div
            className="glass-card"
            style={{
              width: '100%',
              maxWidth: '640px',
              maxHeight: '80vh',
              overflowY: 'auto',
              background: '#101522',
              border: '1px solid rgba(255, 255, 255, 0.15)',
              padding: '1.75rem',
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.25rem' }}>
              <div>
                <h3 style={{ fontSize: '1.2rem', fontWeight: 800, color: '#fff' }}>
                  {isGK ? 'Add Goalkeeper Metric Filter' : 'Add Outfield Metric Filter'}
                </h3>
                <p style={{ fontSize: '0.78rem', color: 'var(--text-dim)' }}>
                  {isGK
                    ? 'Screen across shot stopping, sweeping mobility, rebound control, and distribution'
                    : 'Select from available outfield stats and valuation indices'}
                </p>
              </div>
              <button
                onClick={() => setShowAddFilterModal(false)}
                style={{ color: 'var(--text-muted)', cursor: 'pointer' }}
              >
                <X size={20} />
              </button>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '1.25rem' }}>
              {Array.from(new Set(metricCatalog.map((m) => m.category))).map((category) => {
                const catMetrics = metricCatalog.filter((m) => m.category === category);
                return (
                  <div key={category}>
                    <div
                      style={{
                        fontSize: '0.72rem',
                        fontWeight: 700,
                        color: 'var(--text-dim)',
                        textTransform: 'uppercase',
                        marginBottom: '0.5rem',
                        letterSpacing: '0.05em',
                      }}
                    >
                      {category}
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: '0.5rem' }}>
                      {catMetrics.map((m) => {
                        const isAlreadyAdded = activeFilters.some((f) => f.field === m.field);
                        return (
                          <button
                            key={m.field}
                            onClick={() => handleAddFilter(m)}
                            disabled={isAlreadyAdded}
                            style={{
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'space-between',
                              padding: '0.55rem 0.75rem',
                              borderRadius: '6px',
                              background: isAlreadyAdded ? 'rgba(255, 255, 255, 0.02)' : 'rgba(255, 255, 255, 0.05)',
                              border: isAlreadyAdded ? '1px solid rgba(255, 255, 255, 0.04)' : '1px solid var(--border-subtle)',
                              color: isAlreadyAdded ? 'var(--text-dim)' : '#fff',
                              fontSize: '0.78rem',
                              fontWeight: 500,
                              textAlign: 'left',
                              cursor: isAlreadyAdded ? 'not-allowed' : 'pointer',
                              transition: 'all 0.15s ease',
                            }}
                          >
                            <span>{m.label}</span>
                            <Plus size={12} color="var(--text-dim)" />
                          </button>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* 3. VIEW TABS ROW (Overview, Attacking / Shot Stopping, Creation / Handling, etc.) */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
          gap: '0.75rem',
          borderBottom: '1px solid rgba(255, 255, 255, 0.08)',
          paddingBottom: '0.5rem',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', overflowX: 'auto' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', color: 'var(--text-dim)', fontSize: '0.78rem', paddingRight: '0.5rem' }}>
            <TableIcon size={14} />
            <span>View:</span>
          </div>
          {viewTabs.map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              style={{
                padding: '0.35rem 0.75rem',
                borderRadius: '6px',
                fontSize: '0.8rem',
                fontWeight: 600,
                color: activeTab === tab ? '#fff' : 'var(--text-muted)',
                background: activeTab === tab ? (isGK ? 'rgba(16, 185, 129, 0.2)' : 'rgba(6, 182, 212, 0.2)') : 'transparent',
                border: activeTab === tab ? `1px solid ${isGK ? '#10b981' : '#06b6d4'}` : '1px solid transparent',
                transition: 'all 0.15s ease',
                cursor: 'pointer',
              }}
            >
              {tab}
            </button>
          ))}
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', fontSize: '0.8rem', color: 'var(--text-dim)' }}>
          <span>
            Screened:{' '}
            <strong style={{ color: isGK ? '#34d399' : '#38bdf8' }} className="mono-number">
              {totalCount.toLocaleString()}
            </strong>{' '}
            {isGK ? 'goalkeepers' : 'outfield players'}
          </span>
          <select
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setPage(1);
            }}
            style={{
              background: 'transparent',
              border: '1px solid var(--border-subtle)',
              borderRadius: '4px',
              color: 'var(--text-muted)',
              fontSize: '0.75rem',
              padding: '0.2rem 0.4rem',
            }}
          >
            <option value="25" style={{ background: '#131722' }}>25 rows</option>
            <option value="50" style={{ background: '#131722' }}>50 rows</option>
            <option value="100" style={{ background: '#131722' }}>100 rows</option>
          </select>
        </div>
      </div>

      {/* 4. DENSE DATA GRID (TradingView Style with Goalkeeper & Outfield support) */}
      <div className="fbref-table-container">
        <table className="fbref-table" id="screener-results-grid">
          <thead>
            <tr>
              {renderSortTh('player_name', isGK ? 'Goalkeeper' : 'Player', 'left', '160px')}
              <th>Position</th>
              <th>Club</th>
              {renderSortTh('age', 'Age')}
              {renderSortTh('minutes_played', 'Mins')}

              {/* ================= GOALKEEPER COLUMNS ================= */}
              {isGK && activeTab === 'Overview' && (
                <>
                  {renderSortTh('transfer_value_m', 'Value')}
                  {renderSortTh('wage_per_week_k', 'Wage')}
                  {renderSortTh('xgp_per_shot_shrunk', 'xGP/Shot (Shrunk)')}
                  {renderSortTh('xgp_per_season', 'xGP / Season')}
                  {renderSortTh('saves_percentage', 'Save %')}
                  {renderSortTh('parry_rate', 'Parry Rate')}
                  {renderSortTh('sweeper_actions_per90', 'Sweeper Act/90')}
                  {renderSortTh('sample_reliability', 'Reliability')}
                </>
              )}

              {isGK && activeTab === 'Shot Stopping' && (
                <>
                  {renderSortTh('shots_faced_per90', 'Shots/90')}
                  {renderSortTh('shot_difficulty', 'Shot Diff')}
                  {renderSortTh('xgp_per_shot', 'xGP/Shot')}
                  {renderSortTh('xgp_per_shot_shrunk', 'xGP/Shot (Shrunk)')}
                  {renderSortTh('goals_vs_xgot_ratio', 'Goals/xGOT')}
                  {renderSortTh('xgp_per_season', 'xGP / Season')}
                  {renderSortTh('saves_percentage', 'Save %')}
                  {renderSortTh('goals_conceded_per90', 'GA/90')}
                </>
              )}

              {isGK && activeTab === 'Handling & Sweeping' && (
                <>
                  {renderSortTh('parry_rate', 'Parry Rate')}
                  {renderSortTh('hold_rate_pct', 'Hold %')}
                  {renderSortTh('hold_rate_over_expected', 'Hold Over Exp')}
                  {renderSortTh('sweeper_actions_per90', 'Sweeper Act/90')}
                  {renderSortTh('sweeper_share', 'Sweeper Share')}
                  {renderSortTh('clearances_per90', 'Clr/90')}
                  {renderSortTh('errors_leading_to_goal_per90', 'Errors/90')}
                </>
              )}

              {isGK && activeTab === 'Distribution' && (
                <>
                  {renderSortTh('passes_attempted_per90', 'Pass Att/90')}
                  {renderSortTh('pass_accuracy', 'Pass Acc %')}
                  {renderSortTh('completed_passes_per90', 'Comp Pass/90')}
                  {renderSortTh('giveaways_per90', 'Giveaways/90')}
                </>
              )}

              {/* ================= OUTFIELD COLUMNS ================= */}
              {!isGK && activeTab === 'Overview' && (
                <>
                  {renderSortTh('transfer_value_m', 'Value')}
                  {renderSortTh('wage_per_week_k', 'Wage')}
                  {renderSortTh('npxg_plus_xa_per90', 'NP-xG+xA/90')}
                  {renderSortTh('progressive_passes_per90', 'PrPass/90')}
                  {renderSortTh('defensive_actions_per90', 'DefAct/90')}
                  {renderSortTh('cost_per_xgi', 'Cost/xGI')}
                </>
              )}

              {!isGK && activeTab === 'Attacking' && (
                <>
                  {renderSortTh('goals_per90', 'Goals/90')}
                  {renderSortTh('goals_minus_xg_per90', 'Goals-xG (Shrunk)')}
                  {renderSortTh('non_penalty_xGoals_per90', 'NP-xG/90')}
                  {renderSortTh('shots_per90', 'Shots/90')}
                  {renderSortTh('npxg_per_shot', 'NP-xG/Shot')}
                  {renderSortTh('shot_on_target_rate', 'SoT %')}
                  {renderSortTh('conversion_rate', 'Conv %')}
                </>
              )}

              {!isGK && activeTab === 'Creation' && (
                <>
                  {renderSortTh('assists_per90', 'Assists/90')}
                  {renderSortTh('xAssists_per90', 'xA/90')}
                  {renderSortTh('progressive_passes_per90', 'PrPass/90')}
                  {renderSortTh('progressive_actions_per90', 'PrActions/90')}
                  {renderSortTh('open_play_key_passes_per90', 'KeyPass/90')}
                  {renderSortTh('completed_crosses_per90', 'Cross/90')}
                  {renderSortTh('net_ball_security', 'Net Security')}
                </>
              )}

              {!isGK && activeTab === 'Defending' && (
                <>
                  {renderSortTh('defensive_actions_per90', 'DefAct/90')}
                  {renderSortTh('non_clearance_def_actions_per90', 'Non-Clr DefAct')}
                  {renderSortTh('duels_won_per90', 'Duels Won/90')}
                  {renderSortTh('tackle_accuracy', 'Tck %')}
                  {renderSortTh('interceptions_per90', 'Int/90')}
                  {renderSortTh('pressure_accuracy', 'Press Acc %')}
                </>
              )}

              {!isGK && activeTab === 'Physical' && (
                <>
                  {renderSortTh('aerial_duels_won_per90', 'Aerial Won/90')}
                  {renderSortTh('heading_accuracy', 'Hdr %')}
                  {renderSortTh('sprints_per_km', 'Sprints/km')}
                  {renderSortTh('high_press_load', 'Press Load')}
                  {renderSortTh('fouls_won_net', 'Net Fouls')}
                  {renderSortTh('distance_covered_km_per90', 'Dist km/90')}
                </>
              )}

              {activeTab === 'Valuation' && (
                <>
                  {renderSortTh('transfer_value_m', 'Transfer Val')}
                  {renderSortTh('wage_per_week_k', 'Wage/Wk')}
                  {!isGK && renderSortTh('cost_per_xgi', 'Cost/xGI')}
                  {!isGK && renderSortTh('xgi_per_cost_m', 'xGI / £M')}
                  {renderSortTh('value_gap', 'Value Gap')}
                  {renderSortTh('league_difficulty_coef', 'League Coef')}
                </>
              )}

              <th style={{ textAlign: 'center', width: '80px' }}>Action</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={14} style={{ textAlign: 'center', padding: '3.5rem', color: 'var(--text-dim)' }}>
                  Screening warehouse {isGK ? 'goalkeepers' : 'outfield players'}...
                </td>
              </tr>
            ) : players.length === 0 ? (
              <tr>
                <td colSpan={14} style={{ textAlign: 'center', padding: '3.5rem', color: 'var(--text-dim)' }}>
                  No {isGK ? 'goalkeepers' : 'players'} matched the screener criteria. Try relaxing your filters.
                </td>
              </tr>
            ) : (
              players.map((p) => (
                <tr key={p.player_key}>
                  <td>
                    <Link
                      href={`/player/${p.player_key}`}
                      style={{ fontWeight: 600, color: '#fff', display: 'flex', alignItems: 'center', gap: '0.45rem' }}
                    >
                      <span>{p.player_name}</span>
                      {p.is_own_squad === 1 && (
                        <span className="badge badge-own-squad" style={{ fontSize: '0.62rem' }}>
                          SQUAD
                        </span>
                      )}
                    </Link>
                  </td>
                  <td>
                    <span className="badge badge-position">{p.position}</span>
                  </td>
                  <td>
                    <div style={{ color: '#cbd5e1', fontSize: '0.82rem' }}>{p.club_name}</div>
                    <div style={{ fontSize: '0.68rem', color: 'var(--text-dim)', display: 'flex', alignItems: 'center', gap: '0.35rem', marginTop: '2px' }}>
                      <span>{p.league}</span>
                      {p.league_difficulty_coef !== undefined && p.league_difficulty_coef !== null && (
                        <span
                          title={`League Difficulty Coefficient: ${p.league_difficulty_coef}x`}
                          style={{
                            fontSize: '0.62rem',
                            padding: '0 4px',
                            borderRadius: '3px',
                            background: p.league_difficulty_coef >= 0.9 ? 'rgba(52, 211, 153, 0.15)' : 'rgba(255, 255, 255, 0.08)',
                            color: p.league_difficulty_coef >= 0.9 ? '#34d399' : '#94a3b8',
                            fontWeight: 600,
                          }}
                        >
                          {p.league_difficulty_coef}x
                        </span>
                      )}
                    </div>
                  </td>
                  <td style={getTdSortStyle('age')} className="mono-number">{p.age}</td>
                  <td style={getTdSortStyle('minutes_played')} className="mono-number">{p.minutes_played?.toLocaleString()}</td>

                  {/* ================= GK DATA CELLS ================= */}
                  {isGK && activeTab === 'Overview' && (
                    <>
                      <td style={getTdSortStyle('transfer_value_m', '#38bdf8')} className="mono-number">
                        {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                      </td>
                      <td style={getTdSortStyle('wage_per_week_k')} className="mono-number">
                        {p.wage_per_week_k ? `£${p.wage_per_week_k}k` : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'xgp_per_shot_shrunk',
                          (p.xgp_per_shot_shrunk || 0) >= 0.02 ? '#34d399' : (p.xgp_per_shot_shrunk || 0) > 0 ? '#fbbf24' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.xgp_per_shot_shrunk !== undefined ? (p.xgp_per_shot_shrunk > 0 ? `+${p.xgp_per_shot_shrunk.toFixed(3)}` : p.xgp_per_shot_shrunk.toFixed(3)) : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'xgp_per_season',
                          (p.xgp_per_season || 0) >= 4.0 ? '#38bdf8' : (p.xgp_per_season || 0) >= 0 ? '#cbd5e1' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.xgp_per_season !== undefined ? (p.xgp_per_season > 0 ? `+${p.xgp_per_season.toFixed(1)}` : p.xgp_per_season.toFixed(1)) : '-'}
                      </td>
                      <td style={getTdSortStyle('saves_percentage', '#fbbf24')} className="mono-number">
                        {p.saves_percentage ? `${Math.round(p.saves_percentage * 100)}%` : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'parry_rate',
                          (p.parry_rate || 0) <= 0.60 ? '#34d399' : (p.parry_rate || 0) <= 0.72 ? '#fbbf24' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.parry_rate !== undefined ? `${(p.parry_rate * 100).toFixed(0)}%` : '-'}
                      </td>
                      <td style={getTdSortStyle('sweeper_actions_per90', '#a78bfa')} className="mono-number">
                        {p.sweeper_actions_per90 ? p.sweeper_actions_per90.toFixed(2) : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'sample_reliability',
                          (p.sample_reliability || 0) >= 0.35 ? '#34d399' : '#94a3b8'
                        )}
                        className="mono-number"
                      >
                        {p.sample_reliability !== undefined ? `${(p.sample_reliability * 100).toFixed(0)}%` : '-'}
                      </td>
                    </>
                  )}

                  {isGK && activeTab === 'Shot Stopping' && (
                    <>
                      <td style={getTdSortStyle('shots_faced_per90')} className="mono-number">{p.shots_faced_per90?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('shot_difficulty', '#fbbf24')} className="mono-number">
                        {p.shot_difficulty ? p.shot_difficulty.toFixed(3) : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'xgp_per_shot',
                          (p.xgp_per_shot || 0) > 0 ? '#34d399' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.xgp_per_shot !== undefined ? (p.xgp_per_shot > 0 ? `+${p.xgp_per_shot.toFixed(3)}` : p.xgp_per_shot.toFixed(3)) : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'xgp_per_shot_shrunk',
                          (p.xgp_per_shot_shrunk || 0) >= 0.02 ? '#34d399' : (p.xgp_per_shot_shrunk || 0) > 0 ? '#fbbf24' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.xgp_per_shot_shrunk !== undefined ? (p.xgp_per_shot_shrunk > 0 ? `+${p.xgp_per_shot_shrunk.toFixed(3)}` : p.xgp_per_shot_shrunk.toFixed(3)) : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'goals_vs_xgot_ratio',
                          (p.goals_vs_xgot_ratio || 0) < 0.90 ? '#38bdf8' : (p.goals_vs_xgot_ratio || 0) <= 1.05 ? '#fbbf24' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.goals_vs_xgot_ratio ? p.goals_vs_xgot_ratio.toFixed(2) : '-'}
                      </td>
                      <td style={getTdSortStyle('xgp_per_season')} className="mono-number">
                        {p.xgp_per_season !== undefined ? (p.xgp_per_season > 0 ? `+${p.xgp_per_season.toFixed(1)}` : p.xgp_per_season.toFixed(1)) : '-'}
                      </td>
                      <td style={getTdSortStyle('saves_percentage', '#fbbf24')} className="mono-number">
                        {p.saves_percentage ? `${Math.round(p.saves_percentage * 100)}%` : '-'}
                      </td>
                      <td style={getTdSortStyle('goals_conceded_per90')} className="mono-number">{p.goals_conceded_per90?.toFixed(2) || '0.00'}</td>
                    </>
                  )}

                  {isGK && activeTab === 'Handling & Sweeping' && (
                    <>
                      <td
                        style={getTdSortStyle(
                          'parry_rate',
                          (p.parry_rate || 0) <= 0.60 ? '#34d399' : (p.parry_rate || 0) <= 0.72 ? '#fbbf24' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.parry_rate !== undefined ? `${(p.parry_rate * 100).toFixed(0)}%` : '-'}
                      </td>
                      <td style={getTdSortStyle('hold_rate_pct')} className="mono-number">
                        {p.hold_rate_pct ? `${p.hold_rate_pct.toFixed(1)}%` : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'hold_rate_over_expected',
                          (p.hold_rate_over_expected || 0) > 0 ? '#34d399' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.hold_rate_over_expected !== undefined ? (p.hold_rate_over_expected > 0 ? `+${p.hold_rate_over_expected.toFixed(3)}` : p.hold_rate_over_expected.toFixed(3)) : '-'}
                      </td>
                      <td style={getTdSortStyle('sweeper_actions_per90', '#34d399')} className="mono-number">
                        {p.sweeper_actions_per90 ? p.sweeper_actions_per90.toFixed(2) : '-'}
                      </td>
                      <td style={getTdSortStyle('sweeper_share')} className="mono-number">
                        {p.sweeper_share !== undefined ? `${(p.sweeper_share * 100).toFixed(0)}%` : '-'}
                      </td>
                      <td style={getTdSortStyle('clearances_per90')} className="mono-number">{p.clearances_per90?.toFixed(2) || '0.00'}</td>
                      <td
                        style={getTdSortStyle(
                          'errors_leading_to_goal_per90',
                          (p.errors_leading_to_goal_per90 || 0) > 0 ? '#fb7185' : 'var(--text-dim)'
                        )}
                        className="mono-number"
                      >
                        {p.errors_leading_to_goal_per90?.toFixed(2) || '0.00'}
                      </td>
                    </>
                  )}

                  {isGK && activeTab === 'Distribution' && (
                    <>
                      <td style={getTdSortStyle('passes_attempted_per90')} className="mono-number">{p.passes_attempted_per90?.toFixed(1) || '0.0'}</td>
                      <td style={getTdSortStyle('pass_accuracy', '#38bdf8')} className="mono-number">
                        {p.pass_accuracy ? `${Math.round(p.pass_accuracy * 100)}%` : '-'}
                      </td>
                      <td style={getTdSortStyle('completed_passes_per90', '#34d399')} className="mono-number">
                        {p.completed_passes_per90 ? p.completed_passes_per90.toFixed(1) : '-'}
                      </td>
                      <td style={getTdSortStyle('giveaways_per90', '#fb7185')} className="mono-number">
                        {p.giveaways_per90 ? p.giveaways_per90.toFixed(1) : '-'}
                      </td>
                    </>
                  )}

                  {/* ================= OUTFIELD DATA CELLS ================= */}
                  {!isGK && activeTab === 'Overview' && (
                    <>
                      <td style={getTdSortStyle('transfer_value_m', '#38bdf8')} className="mono-number">
                        {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                      </td>
                      <td style={getTdSortStyle('wage_per_week_k')} className="mono-number">
                        {p.wage_per_week_k ? `£${p.wage_per_week_k}k` : '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'npxg_plus_xa_per90',
                          (p.npxg_plus_xa_per90 || 0) >= 0.5 ? '#34d399' : (p.npxg_plus_xa_per90 || 0) >= 0.35 ? '#fbbf24' : '#fff'
                        )}
                        className="mono-number"
                      >
                        {p.npxg_plus_xa_per90?.toFixed(2) || '0.00'}
                      </td>
                      <td style={getTdSortStyle('progressive_passes_per90')} className="mono-number">{p.progressive_passes_per90?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('defensive_actions_per90')} className="mono-number">{p.defensive_actions_per90?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('cost_per_xgi', '#34d399')} className="mono-number">
                        {p.cost_per_xgi ? `£${(p.cost_per_xgi / 1000).toFixed(1)}k` : '-'}
                      </td>
                    </>
                  )}

                  {!isGK && activeTab === 'Attacking' && (
                    <>
                      <td style={getTdSortStyle('goals_per90')} className="mono-number">{p.goals_per90?.toFixed(2) || '0.00'}</td>
                      <td
                        style={getTdSortStyle(
                          'goals_minus_xg_per90',
                          (p.goals_minus_xg_per90 || 0) > 0 ? '#34d399' : (p.goals_minus_xg_per90 || 0) < 0 ? '#fb7185' : 'var(--text-dim)'
                        )}
                        className="mono-number"
                      >
                        {p.goals_minus_xg_per90 !== undefined
                          ? (p.goals_minus_xg_per90 > 0 ? `+${p.goals_minus_xg_per90.toFixed(2)}` : p.goals_minus_xg_per90.toFixed(2))
                          : '0.00'}
                      </td>
                      <td style={getTdSortStyle('non_penalty_xGoals_per90', '#fbbf24')} className="mono-number">
                        {p.non_penalty_xGoals_per90?.toFixed(2) || '0.00'}
                      </td>
                      <td style={getTdSortStyle('shots_per90')} className="mono-number">{p.shots_per90?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('npxg_per_shot')} className="mono-number">{p.npxg_per_shot?.toFixed(2) || p.xGoals_per_shot?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('shot_on_target_rate')} className="mono-number">
                        {p.shot_on_target_rate ? `${Math.round(p.shot_on_target_rate * 100)}%` : '-'}
                      </td>
                      <td style={getTdSortStyle('conversion_rate')} className="mono-number">
                        {p.conversion_rate ? `${Math.round(p.conversion_rate * 100)}%` : '-'}
                      </td>
                    </>
                  )}

                  {!isGK && activeTab === 'Creation' && (
                    <>
                      <td style={getTdSortStyle('assists_per90')} className="mono-number">{p.assists_per90?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('xAssists_per90', '#38bdf8')} className="mono-number">
                        {(p.xAssists_per90 || p.xAssits_per90)?.toFixed(2) || '0.00'}
                      </td>
                      <td style={getTdSortStyle('progressive_passes_per90')} className="mono-number">{p.progressive_passes_per90?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('progressive_actions_per90', '#a78bfa')} className="mono-number">
                        {p.progressive_actions_per90?.toFixed(2) || p.progressive_passes_per90?.toFixed(2) || '0.00'}
                      </td>
                      <td style={getTdSortStyle('open_play_key_passes_per90')} className="mono-number">{p.open_play_key_passes_per90?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('completed_crosses_per90')} className="mono-number">{p.completed_crosses_per90?.toFixed(2) || '0.00'}</td>
                      <td
                        style={getTdSortStyle(
                          'net_ball_security',
                          (p.net_ball_security || 0) >= 0 ? '#34d399' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.net_ball_security !== undefined
                          ? (p.net_ball_security > 0 ? `+${p.net_ball_security.toFixed(1)}` : p.net_ball_security.toFixed(1))
                          : (p.pass_accuracy ? `${Math.round(p.pass_accuracy * 100)}%` : '-')}
                      </td>
                    </>
                  )}

                  {!isGK && activeTab === 'Defending' && (
                    <>
                      <td style={getTdSortStyle('defensive_actions_per90', '#fb7185')} className="mono-number">
                        {p.defensive_actions_per90?.toFixed(2) || '0.00'}
                      </td>
                      <td style={getTdSortStyle('non_clearance_def_actions_per90', '#f43f5e')} className="mono-number">
                        {p.non_clearance_def_actions_per90?.toFixed(2) || p.defensive_actions_per90?.toFixed(2) || '0.00'}
                      </td>
                      <td style={getTdSortStyle('duels_won_per90', '#34d399')} className="mono-number">
                        {p.duels_won_per90?.toFixed(2) || '-'}
                      </td>
                      <td style={getTdSortStyle('tackle_accuracy')} className="mono-number">
                        {p.tackle_accuracy ? `${Math.round(p.tackle_accuracy * 100)}%` : '-'}
                      </td>
                      <td style={getTdSortStyle('interceptions_per90')} className="mono-number">{p.interceptions_per90?.toFixed(2) || '0.00'}</td>
                      <td style={getTdSortStyle('pressure_accuracy')} className="mono-number">
                        {p.pressure_accuracy ? `${Math.round(p.pressure_accuracy)}%` : (p.pressure_efficiency ? `${Math.round(p.pressure_efficiency * 100)}%` : '-')}
                      </td>
                    </>
                  )}

                  {!isGK && activeTab === 'Physical' && (
                    <>
                      <td style={getTdSortStyle('aerial_duels_won_per90', '#a78bfa')} className="mono-number">
                        {p.aerial_duels_won_per90?.toFixed(2) || '0.00'}
                      </td>
                      <td style={getTdSortStyle('heading_accuracy')} className="mono-number">
                        {p.heading_accuracy ? `${Math.round(p.heading_accuracy * 100)}%` : '-'}
                      </td>
                      <td style={getTdSortStyle('sprints_per_km', '#38bdf8')} className="mono-number">
                        {p.sprints_per_km?.toFixed(2) || (p.sprints_per90 ? p.sprints_per90.toFixed(1) : '-')}
                      </td>
                      <td style={getTdSortStyle('high_press_load', '#f59e0b')} className="mono-number">
                        {p.high_press_load?.toFixed(1) || '-'}
                      </td>
                      <td
                        style={getTdSortStyle(
                          'fouls_won_net',
                          (p.fouls_won_net || 0) > 0 ? '#34d399' : (p.fouls_won_net || 0) < 0 ? '#fb7185' : 'var(--text-dim)'
                        )}
                        className="mono-number"
                      >
                        {p.fouls_won_net !== undefined
                          ? (p.fouls_won_net > 0 ? `+${p.fouls_won_net.toFixed(2)}` : p.fouls_won_net.toFixed(2))
                          : '-'}
                      </td>
                      <td style={getTdSortStyle('distance_covered_km_per90')} className="mono-number">{p.distance_covered_km_per90?.toFixed(1) || '-'} km</td>
                    </>
                  )}

                  {activeTab === 'Valuation' && (
                    <>
                      <td style={getTdSortStyle('transfer_value_m', '#38bdf8')} className="mono-number">
                        {p.transfer_value_m ? `£${p.transfer_value_m}M` : '-'}
                      </td>
                      <td style={getTdSortStyle('wage_per_week_k')} className="mono-number">
                        {p.wage_per_week_k ? `£${p.wage_per_week_k}k` : '-'}
                      </td>
                      {!isGK && (
                        <td style={getTdSortStyle('cost_per_xgi', '#34d399')} className="mono-number">
                          {p.cost_per_xgi ? `£${(p.cost_per_xgi).toFixed(1)}k` : '-'}
                        </td>
                      )}
                      {!isGK && (
                        <td style={getTdSortStyle('xgi_per_cost_m', '#fbbf24')} className="mono-number">
                          {p.xgi_per_cost_m?.toFixed(1) || '-'}
                        </td>
                      )}
                      <td
                        style={getTdSortStyle(
                          'value_gap',
                          (p.value_gap || 0) >= 40 ? '#34d399' : (p.value_gap || 0) > 0 ? '#fbbf24' : '#fb7185'
                        )}
                        className="mono-number"
                      >
                        {p.value_gap !== undefined ? (p.value_gap > 0 ? `+${p.value_gap.toFixed(1)}` : p.value_gap.toFixed(1)) : '-'}
                      </td>
                      <td style={getTdSortStyle('league_difficulty_coef')} className="mono-number">{p.league_difficulty_coef}x</td>
                    </>
                  )}

                  <td style={{ textAlign: 'center' }}>
                    <Link
                      href={`/player/${p.player_key}`}
                      className="btn-primary"
                      style={{ padding: '0.25rem 0.55rem', fontSize: '0.72rem' }}
                    >
                      Dossier
                    </Link>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* PAGINATION */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.5rem 0' }}>
        <div style={{ fontSize: '0.8rem', color: 'var(--text-dim)' }}>
          Showing page <span className="mono-number" style={{ color: '#fff' }}>{page}</span> of{' '}
          <span className="mono-number" style={{ color: '#fff' }}>{totalPages}</span> ({totalCount} {isGK ? 'goalkeepers' : 'players'})
        </div>

        <div style={{ display: 'flex', gap: '0.4rem' }}>
          <button
            onClick={() => setPage(Math.max(1, page - 1))}
            disabled={page === 1}
            className="btn-secondary"
            style={{ opacity: page === 1 ? 0.4 : 1, padding: '0.35rem 0.75rem', fontSize: '0.78rem' }}
          >
            Previous
          </button>
          <button
            onClick={() => setPage(Math.min(totalPages, page + 1))}
            disabled={page >= totalPages}
            className="btn-secondary"
            style={{ opacity: page >= totalPages ? 0.4 : 1, padding: '0.35rem 0.75rem', fontSize: '0.78rem' }}
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}

export default function ScreenerPage() {
  return (
    <Suspense
      fallback={
        <div className="card" style={{ padding: '4rem', textAlign: 'center', color: 'var(--text-dim)' }}>
          <div style={{ width: '32px', height: '32px', border: '3px solid rgba(56, 189, 248, 0.2)', borderTopColor: '#38bdf8', borderRadius: '50%', animation: 'spin 0.8s linear infinite', margin: '0 auto 1rem auto' }} />
          Loading tactical screener terminal...
        </div>
      }
    >
      <ScreenerContent />
    </Suspense>
  );
}
