/*
======================================================================
Gold Views & Analytical Scouting Marts (DW)
======================================================================
Script Purpose:
    Defines Gold Layer analytical views and unified summary marts:
    1. gold.mart_outfield_scouting_summary: Wide summary mart for all outfield players.
    2. gold.mart_gk_scouting_summary: Wide summary mart for goalkeepers.
    3. gold.vw_transfer_targets_outfield: Filtered scouting target pool with percentiles.
    4. gold.vw_squad_central_defender_detailed_reports: Core CD squad detailed report (TDD verified).
    5. gold.vw_squad_central_defender_reports: Central defender squad report.
    6. gold.vw_squad_flank_defender_reports: Fullback & wingback squad report.
    7. gold.vw_squad_midfielder_reports: Central midfielder squad report.
    8. gold.vw_squad_attacking_midfielder_reports: Attacking midfielder squad report.
    9. gold.vw_squad_striker_reports: Striker squad report.
    9. Positional Replacement Similarity Engines:
       - gold.vw_similar_strikers (ST)
       - gold.vw_similar_central_defenders (CD)
       - gold.vw_similar_flank_defenders (RB/LB/RWB/LWB)
       - gold.vw_similar_defensive_midfielders (DM)
       - gold.vw_similar_central_midfielders & vw_similar_midfielders (CM)
       - gold.vw_similar_wingers (RW/LW/RM/LM)
       - gold.vw_similar_attacking_midfielders (CAM / #10)
       - gold.vw_similar_goalkeepers (GK)
    10. Multi-Season Growth & Development Tracking:
       - gold.mart_outfield_historical_snapshots: Full historical multi-season mart.
       - gold.vw_player_growth_tracking: Season-over-season outfield development deltas.
       - gold.vw_gk_growth_tracking: Season-over-season goalkeeper development deltas.
    11. Advanced Tactical & Recruitment Intelligence:
       - gold.vw_squad_succession_plan: Aging squad replacement & U23 successor matching.
       - gold.vw_bargain_watch: Value efficiency & contract bargain recruitment engine.
       - gold.vw_tactical_archetypes: Tactical role profiling (BPD, DLP, BWM, Poacher, etc.).
======================================================================
*/

USE DW;
GO

-- ======================================================================
-- 1. UNIFIED OUTFIELD SCOUTING SUMMARY MART
-- ======================================================================
CREATE OR ALTER VIEW gold.mart_outfield_scouting_summary AS
SELECT
    -- Identity & Club Information
    p.player_key,
    p.player_id,
    p.player_name,
    p.age,
    p.position,
    t.club_name,
    t.league,
    COALESCE(l.league_difficulty_coef, 1.00) AS league_difficulty_coef,
    p.is_own_squad,                      -- 1 = Own squad, 0 = Potential buy
    p.contracted,
    s.minutes_played,

    -- Financials & Valuation
    v.wage_per_week_k,
    v.transfer_value_m,

    -- Attacking & Chance Creation: Core Outputs
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

    -- Attacking & Chance Creation: Shrunk & Advanced Ratios
    CAST((s.goals_per90 - s.xGoals_per90) * (CAST(s.minutes_played AS DECIMAL(10,2)) / (s.minutes_played + 900.0)) AS DECIMAL(7,4)) AS finishing_overperf_shrunk,
    CAST((s.goals_per90 - s.xGoals_per90) * (CAST(s.minutes_played AS DECIMAL(10,2)) / (s.minutes_played + 900.0)) AS DECIMAL(7,4)) AS goals_minus_xg_per90,
    CAST((s.assists_per90 - s.xAssits_per90) * (CAST(s.minutes_played AS DECIMAL(10,2)) / (s.minutes_played + 900.0)) AS DECIMAL(7,4)) AS assist_luck,
    CAST((s.assists_per90 - s.xAssits_per90) * (CAST(s.minutes_played AS DECIMAL(10,2)) / (s.minutes_played + 900.0)) AS DECIMAL(7,4)) AS assist_luck_shrunk,
    CAST(s.non_penalty_xGoals_per90 / NULLIF(s.non_penalty_xGoals_per90 + s.xAssits_per90, 0) AS DECIMAL(7,4)) AS goal_threat_share,
    CAST(s.shots_outside_the_box_per90 / NULLIF(s.shots_per90, 0) AS DECIMAL(7,4)) AS shots_outside_box_ratio,
    CAST(CAST(s.goals_outside_the_box AS DECIMAL(7,4)) / NULLIF(s.goals, 0) AS DECIMAL(7,4)) AS long_range_goal_share,
    CAST(s.chances_created_per90 / NULLIF(s.passes_attempted_per90, 0) * 100.0 AS DECIMAL(7,4)) AS chance_creation_per_pass_pct,
    CAST(s.xAssits_per90 / NULLIF(s.open_play_key_passes_per90, 0) AS DECIMAL(7,4)) AS xa_per_op_kp,
    CAST(s.assists_per90 / NULLIF(s.open_play_key_passes_per90, 0) AS DECIMAL(7,4)) AS assists_per_op_kp,
    CAST(s.non_penalty_xGoals_per90 / NULLIF(s.shots_per90, 0) AS DECIMAL(7,4)) AS npxg_per_shot,
    CAST(s.shots_per90 * s.xGoals_per_shot AS DECIMAL(7,4)) AS box_presence_proxy,

    -- Crossing Dynamics
    s.crosses_attempted_per90,
    s.cross_accuracy,
    CAST(s.crosses_attempted_per90 * s.cross_accuracy AS DECIMAL(7,4)) AS completed_crosses_per90,
    s.open_play_crosses_attempted_per90,
    s.open_play_cross_accuracy,
    CAST(s.open_play_crosses_attempted_per90 * s.open_play_cross_accuracy AS DECIMAL(7,4)) AS open_play_completed_crosses_per90,
    CAST((s.crosses_attempted_per90 * s.cross_accuracy) / NULLIF(s.chances_created_per90, 0) AS DECIMAL(7,4)) AS crosses_share_of_creation,

    -- Passing, Progression & Ball Security
    s.passes_attempted_per90,
    s.pass_accuracy,
    CAST(s.passes_attempted_per90 * s.pass_accuracy AS DECIMAL(7,4)) AS completed_passes_per90,
    s.progressive_passes_per90,
    CAST(s.progressive_passes_per90 + s.dribbles_made_per90 AS DECIMAL(7,4)) AS progressive_actions_per90,
    CAST(s.progressive_passes_per90 / NULLIF(s.passes_attempted_per90, 0) * 100.0 AS DECIMAL(7,4)) AS prog_rate_per_pass,
    CAST(s.possession_lost_per90 / NULLIF(s.passes_attempted_per90 + s.dribbles_made_per90, 0) AS DECIMAL(7,4)) AS turnover_rate,
    CAST(s.progressive_passes_per90 / NULLIF(s.passes_attempted_per90 + s.dribbles_made_per90, 0) AS DECIMAL(7,4)) AS progressive_share_of_actions,
    CAST((s.passes_attempted_per90 * s.pass_accuracy) - s.possession_lost_per90 AS DECIMAL(7,4)) AS net_ball_security,
    s.possession_won_per90,
    s.possession_lost_per90,
    (s.possession_won_per90 - s.possession_lost_per90) AS possession_differential,

    -- Defending & Interventions
    s.tackles_attempted_per90,
    s.tackle_accuracy,
    s.key_tackles_per90,
    s.interceptions_per90,
    s.blocks_per90,
    s.shots_blocked_per90,
    s.clearances_per90,
    CAST(((s.tackles_attempted_per90 * s.tackle_accuracy) + s.interceptions_per90 + s.blocks_per90 + s.shots_blocked_per90 + s.clearances_per90) AS DECIMAL(7,4)) AS defensive_actions_per90,
    CAST(((s.tackles_attempted_per90 * s.tackle_accuracy) + s.interceptions_per90 + s.blocks_per90 + s.shots_blocked_per90) AS DECIMAL(7,4)) AS non_clearance_def_actions_per90,
    CAST(s.interceptions_per90 / NULLIF(s.interceptions_per90 + (s.tackles_attempted_per90 * s.tackle_accuracy), 0) AS DECIMAL(7,4)) AS reactive_vs_positional,

    -- Aerial & Duels
    s.headers_attempted_per90,
    s.heading_accuracy,
    s.key_headers_per90,
    CAST(s.headers_attempted_per90 * s.heading_accuracy AS DECIMAL(7,4)) AS aerial_duels_won_per90,
    CAST((s.tackles_attempted_per90 * s.tackle_accuracy) + (s.headers_attempted_per90 * s.heading_accuracy) AS DECIMAL(7,4)) AS duels_won_per90,
    CAST(s.fouls_made_per90 / NULLIF(s.tackles_attempted_per90 + s.headers_attempted_per90, 0) AS DECIMAL(7,4)) AS foul_per_duel,
    CAST(s.fouls_made_per90 / NULLIF(s.tackles_attempted_per90, 0) AS DECIMAL(7,4)) AS foul_per_tackle,
    CAST(s.clearances_per90 / NULLIF(s.headers_attempted_per90, 0) AS DECIMAL(7,4)) AS clearance_per_aerial,
    s.mistakes_leading_to_goals_per90,

    -- Physical, Pressing & Discipline
    s.pressures_attempted_per90,
    s.pressures_completed_per90,
    CAST(s.pressures_completed_per90 / NULLIF(s.pressures_attempted_per90, 0) * 100.0 AS DECIMAL(7,4)) AS pressure_accuracy,
    CAST(s.pressures_completed_per90 / NULLIF(s.pressures_attempted_per90, 0) AS DECIMAL(7,4)) AS pressure_efficiency,
    s.sprints_per90,
    s.distance_covered_km_per90,
    CAST(s.sprints_per90 / NULLIF(s.distance_covered_km_per90, 0) AS DECIMAL(7,4)) AS sprints_per_km,
    CAST(s.pressures_attempted_per90 * (s.sprints_per90 / NULLIF(s.distance_covered_km_per90, 0)) AS DECIMAL(7,4)) AS high_press_load,
    s.fouls_made_per90,
    s.fouls_against_per90,
    CAST(s.fouls_against_per90 - s.fouls_made_per90 AS DECIMAL(7,4)) AS fouls_won_net,
    s.yellow_cards_per90,
    s.red_cards_per90,
    CAST(s.fouls_made_per90 + (3.0 * s.yellow_cards_per90) + (8.0 * s.red_cards_per90) AS DECIMAL(7,4)) AS discipline_index,
    CAST((s.yellow_cards_per90 + s.red_cards_per90) / NULLIF(s.fouls_made_per90, 0) AS DECIMAL(7,4)) AS cards_per_foul,

    -- League Difficulty Normalized Outputs (_adj)
    CAST(s.goals_per90 * COALESCE(l.league_difficulty_coef, 1.00) AS DECIMAL(7,4)) AS goals_per90_adj,
    CAST(s.xGoals_per90 * COALESCE(l.league_difficulty_coef, 1.00) AS DECIMAL(7,4)) AS xGoals_per90_adj,
    CAST(s.non_penalty_xGoals_per90 * COALESCE(l.league_difficulty_coef, 1.00) AS DECIMAL(7,4)) AS non_penalty_xGoals_per90_adj,
    CAST(s.xAssits_per90 * COALESCE(l.league_difficulty_coef, 1.00) AS DECIMAL(7,4)) AS xAssists_per90_adj,
    CAST((s.non_penalty_xGoals_per90 + s.xAssits_per90) * COALESCE(l.league_difficulty_coef, 1.00) AS DECIMAL(7,4)) AS npxg_plus_xa_per90_adj,

    -- Moneyball Efficiency Indices & Valuation
    CAST(((v.wage_per_week_k * 52.0) + (v.transfer_value_m * 1000.0 / 3.0)) / NULLIF((s.non_penalty_xGoals_per90 + s.xAssits_per90) * (s.minutes_played / 90.0), 0) AS DECIMAL(10,2)) AS cost_per_xgi,
    CAST(((s.non_penalty_xGoals_per90 + s.xAssits_per90) * (s.minutes_played / 90.0)) / NULLIF((v.wage_per_week_k * 0.052) + (v.transfer_value_m / 3.0), 0) AS DECIMAL(7,4)) AS xgi_per_cost_m,
    CAST((PERCENT_RANK() OVER (ORDER BY (s.non_penalty_xGoals_per90 + s.xAssits_per90)) * 100.0) - 
         (PERCENT_RANK() OVER (ORDER BY COALESCE(v.transfer_value_m, 0.10)) * 100.0) AS DECIMAL(5,1)) AS value_gap,

    -- On-Pitch Context
    (s.team_goals_scored_per90 - s.team_goals_conceded_per90) AS on_pitch_goal_diff_per90,
    CAST((s.team_goals_scored_per90 - s.team_goals_conceded_per90) * (CAST(s.minutes_played AS DECIMAL(10,2)) / (s.minutes_played + 900.0)) AS DECIMAL(7,4)) AS on_pitch_gd_shrunk
FROM gold.fact_outfield_player_stats s
INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
INNER JOIN gold.dim_team_info t ON s.team_key = t.team_key
LEFT JOIN gold.dim_league l ON t.league = l.league
LEFT JOIN gold.fact_players_value v ON s.player_key = v.player_key AND v.dwh_current_validity = 1
WHERE s.dwh_current_validity = 1 AND p.dwh_current_validity = 1;
GO

-- ======================================================================
-- 2. UNIFIED GOALKEEPER SCOUTING SUMMARY MART
-- ======================================================================
CREATE OR ALTER VIEW gold.mart_gk_scouting_summary AS
SELECT
    -- Identity & Club Information
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

    -- Financials & Valuation
    v.wage_per_week_k,
    v.transfer_value_m,

    -- 1. Workload & Sample Size (Foundation for Workload & Shrinkage)
    CAST(s.saves_made_per90 + s.goals_conceded_per90 AS DECIMAL(7,4)) AS shots_faced_per90,
    CAST((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0) AS DECIMAL(7,2)) AS shots_faced_total,
    CAST(s.xGoals_prevented_per90 + s.goals_conceded_per90 AS DECIMAL(7,4)) AS xgot_faced_per90,
    CAST((s.xGoals_prevented_per90 + s.goals_conceded_per90) / NULLIF(s.saves_made_per90 + s.goals_conceded_per90, 0) AS DECIMAL(7,4)) AS shot_difficulty,

    -- 2. Shot-Stopping Metrics (Workload-Normalized & Shrunk by Shots Faced k=300)
    CAST(s.xGoals_prevented_per90 / NULLIF(s.saves_made_per90 + s.goals_conceded_per90, 0) AS DECIMAL(7,4)) AS xgp_per_shot,
    CAST((((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) * (s.xGoals_prevented_per90 / NULLIF(s.saves_made_per90 + s.goals_conceded_per90, 0))) / (((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) + 300.0) AS DECIMAL(7,4)) AS xgp_per_shot_shrunk,
    CAST(s.goals_conceded_per90 / NULLIF(s.xGoals_prevented_per90 + s.goals_conceded_per90, 0) AS DECIMAL(7,4)) AS goals_vs_xgot_ratio,
    CAST(s.xGoals_prevented_per90 * 38.0 AS DECIMAL(7,4)) AS xgp_per_season,
    CAST((((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) / (((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) + 300.0)) * s.xGoals_prevented_per90 * 38.0 AS DECIMAL(7,4)) AS xgp_per_season_shrunk,
    
    CAST(s.saves_percentage - s.xSave_rate AS DECIMAL(7,4)) AS shot_stopping_delta,
    CAST((((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) * (s.saves_percentage - s.xSave_rate)) / (((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) + 300.0) AS DECIMAL(7,4)) AS shot_stopping_delta_shrunk,
    s.xGoals_prevented_per90,
    s.saves_made_per90,
    s.saves_percentage,
    s.xSave_rate,
    s.goals_conceded_per90,

    -- 3. Save Handling & Risk (Parry Risk vs Hold Over Expected)
    CAST(s.saves_parried_per90 / NULLIF(s.saves_made_per90, 0) AS DECIMAL(7,4)) AS parry_rate,
    CAST(s.saves_tipped_per90 / NULLIF(s.saves_made_per90, 0) AS DECIMAL(7,4)) AS tip_rate,
    CAST(s.saves_held_per90 / NULLIF(s.saves_made_per90, 0) AS DECIMAL(7,4)) AS hold_rate,
    CAST((s.saves_made_per90 * (s.minutes_played / 90.0) * (s.saves_held_per90 / NULLIF(s.saves_made_per90, 0)) + 150.0 * 0.30) / (s.saves_made_per90 * (s.minutes_played / 90.0) + 150.0) AS DECIMAL(7,4)) AS hold_rate_shrunk,
    CAST((s.saves_held_per90 / NULLIF(s.saves_made_per90, 0)) - (0.1143 + 0.5230 * ((s.xGoals_prevented_per90 + s.goals_conceded_per90) / NULLIF(s.saves_made_per90 + s.goals_conceded_per90, 0))) AS DECIMAL(7,4)) AS hold_rate_over_expected,
    CAST(s.saves_held_per90 / NULLIF(s.saves_held_per90 + s.saves_parried_per90 + s.saves_tipped_per90, 0) * 100.0 AS DECIMAL(7,4)) AS hold_rate_pct,
    s.saves_tipped_per90,
    s.saves_parried_per90,
    s.saves_held_per90,

    -- 4. Sweeping Actions (Proactive Interceptions vs Reactive Clearances)
    CAST(s.interceptions_per90 + s.clearances_per90 AS DECIMAL(7,4)) AS sweeper_actions_per90,
    CAST(s.interceptions_per90 / NULLIF(s.interceptions_per90 + s.clearances_per90, 0) AS DECIMAL(7,4)) AS sweeper_share,
    CAST(s.interceptions_per90 + s.clearances_per90 AS DECIMAL(7,4)) AS sweeper_index, -- Backward compatible alias
    s.interceptions_per90,
    s.clearances_per90,

    -- 5. Distribution Efficiency & Style
    CAST(s.passes_attempted_per90 * s.pass_accuracy AS DECIMAL(7,4)) AS completed_passes_per90,
    CAST(s.passes_attempted_per90 * (1.0 - s.pass_accuracy) AS DECIMAL(7,4)) AS giveaways_per90,
    s.passes_attempted_per90 AS passing_involvement,
    CAST(s.passes_attempted_per90 * s.pass_accuracy AS DECIMAL(7,4)) AS distribution_value, -- Backward compatible alias
    s.passes_attempted_per90,
    s.pass_accuracy,

    -- 6. Penalties & Errors (Heavily Shrunk)
    s.penalties_faced_per90,
    s.penalties_save_percentage,
    CAST((s.penalties_faced_per90 * (s.minutes_played / 90.0) * s.penalties_save_percentage + 20.0 * 0.20) / (s.penalties_faced_per90 * (s.minutes_played / 90.0) + 20.0) AS DECIMAL(7,4)) AS pen_save_shrunk,
    CAST(s.mistakes_leading_to_goals_per90 AS DECIMAL(7,4)) AS errors_leading_to_goal_per90,
    CAST((s.minutes_played * s.mistakes_leading_to_goals_per90 + 5000.0 * 0.0220) / (s.minutes_played + 5000.0) AS DECIMAL(7,4)) AS errors_leading_to_goal_shrunk,
    CAST(s.mistakes_leading_to_goals_per90 AS DECIMAL(7,4)) AS error_rate, -- Backward compatible alias

    -- 7. Team Context, Eligibility, Value Gap & Reliability
    CAST(s.possession_won_per90 - s.possession_lost_per90 AS DECIMAL(7,4)) AS possession_differential,
    CASE WHEN ((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) >= 40.0 THEN 1 ELSE 0 END AS is_eligible,
    CAST(((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) / (((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) + 300.0) AS DECIMAL(7,4)) AS sample_reliability,
    CAST((PERCENT_RANK() OVER (ORDER BY ((((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) * (s.xGoals_prevented_per90 / NULLIF(s.saves_made_per90 + s.goals_conceded_per90, 0))) / (((s.saves_made_per90 + s.goals_conceded_per90) * (s.minutes_played / 90.0)) + 300.0))) * 100.0) - 
         (PERCENT_RANK() OVER (ORDER BY COALESCE(v.transfer_value_m, 0.10)) * 100.0) AS DECIMAL(5,1)) AS value_gap
FROM gold.fact_gk_stats s
INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
INNER JOIN gold.dim_team_info t ON s.team_key = t.team_key
LEFT JOIN gold.dim_league l ON t.league = l.league
LEFT JOIN gold.fact_players_value v ON s.player_key = v.player_key AND v.dwh_current_validity = 1
WHERE s.dwh_current_validity = 1 AND p.dwh_current_validity = 1;
GO

-- ======================================================================
-- 3. SCOUTING VIEW: POTENTIAL TRANSFER TARGETS WITH POSITION PERCENTILES
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_transfer_targets_outfield AS
SELECT 
    player_key,
    player_name,
    age,
    position,
    club_name,
    league,
    wage_per_week_k,
    transfer_value_m,
    minutes_played,

    -- Attacking & Chance Creation
    npxg_plus_xa_per90,
    PERCENT_RANK() OVER (PARTITION BY position ORDER BY npxg_plus_xa_per90) AS pct_npxg_xa,
    assist_luck,
    finishing_overperf_shrunk,
    goal_threat_share,
    xa_per_op_kp,
    completed_crosses_per90,

    -- Passing & Progression
    progressive_passes_per90,
    prog_rate_per_pass,
    PERCENT_RANK() OVER (PARTITION BY position ORDER BY prog_rate_per_pass) AS pct_prog_rate,
    turnover_rate,

    -- Defending & Pressing
    defensive_actions_per90,
    PERCENT_RANK() OVER (PARTITION BY position ORDER BY defensive_actions_per90) AS pct_def_actions,
    reactive_vs_positional,
    pressure_efficiency,
    aerial_duels_won_per90,
    heading_accuracy,
    discipline_index,

    -- Moneyball Efficiency
    cost_per_xgi,
    league_difficulty_coef
FROM gold.mart_outfield_scouting_summary
WHERE is_own_squad = 0                  -- Potential Buys Only
  AND minutes_played >= 900;
GO

-- Drop legacy club-specific views if they exist
DROP VIEW IF EXISTS gold.vw_fiorentina_central_defender_reports;
DROP VIEW IF EXISTS gold.vw_fiorentina_flank_defender_reports;
DROP VIEW IF EXISTS gold.vw_fiorentina_midfielder_reports;
DROP VIEW IF EXISTS gold.vw_fiorentina_attacking_midfielder_reports;
DROP VIEW IF EXISTS gold.vw_fiorentina_striker_reports;
GO

-- ======================================================================
-- 4. SQUAD REPORT: CENTRAL DEFENDERS (TDD Validated Detailed Metrics)
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_squad_central_defender_detailed_reports AS
WITH eligible_defenders AS (
    SELECT 
        fact.player_key,
        dim.age,
        fact.minutes_played,
        fact.team_goals_scored_per90,
        fact.team_goals_conceded_per90,
        fact.open_play_key_passes_per90,
        fact.progressive_passes_per90,
        fact.passes_attempted_per90,
        fact.tackles_attempted_per90,
        fact.tackle_accuracy,
        fact.key_tackles_per90,
        fact.pressures_attempted_per90,
        fact.pressures_completed_per90,
        fact.possession_won_per90,
        fact.possession_lost_per90,
        fact.interceptions_per90,
        fact.clearances_per90,
        fact.blocks_per90,
        fact.shots_blocked_per90,
        fact.headers_attempted_per90,
        fact.heading_accuracy
    FROM gold.fact_outfield_player_stats AS fact
    INNER JOIN gold.dim_player_info AS dim
        ON fact.player_key = dim.player_key
    WHERE dim.contracted = 1 
      AND dim.position = 'Central Defender'
      AND fact.minutes_played >= 1000
      AND fact.dwh_current_validity = 1 
      AND dim.dwh_current_validity = 1
),
calculated_metrics AS (
    SELECT 
        player_key,
        age,
        minutes_played,
        (team_goals_scored_per90 - team_goals_conceded_per90) AS tG_differential_per90,
        CAST(
            (open_play_key_passes_per90 / NULLIF(progressive_passes_per90, 0)) 
            AS DECIMAL(5,2)
        ) AS kP_per_ProgP,
        CAST(
            (progressive_passes_per90 / NULLIF(passes_attempted_per90, 0) * 100.0) 
            AS DECIMAL(5,2)
        ) AS ProgRate_per_Pass,
        tackles_attempted_per90,
        tackle_accuracy,
        key_tackles_per90,
        pressures_attempted_per90,
        CAST(
            (pressures_completed_per90 / NULLIF(pressures_attempted_per90, 0) * 100.0) 
            AS DECIMAL(5,2)
        ) AS pressure_accuracy,
        (possession_won_per90 - possession_lost_per90) AS possession_differential,
        CAST(
            ((interceptions_per90 * 0.50) + (clearances_per90 * 0.25) + ((blocks_per90 + shots_blocked_per90) * 0.25)) 
            AS DECIMAL(5,2)
        ) AS Wdefensive_actions,
        headers_attempted_per90,
        heading_accuracy
    FROM eligible_defenders
)
SELECT 
    player_key,
    age,
    minutes_played,
    tG_differential_per90,
    kP_per_ProgP,
    ProgRate_per_Pass,
    tackles_attempted_per90,
    tackle_accuracy,
    key_tackles_per90,
    pressures_attempted_per90,
    pressure_accuracy,
    possession_differential,
    Wdefensive_actions,
    headers_attempted_per90,
    heading_accuracy
FROM calculated_metrics;
GO

-- ======================================================================
-- 5. SQUAD REPORTS: CENTRAL DEFENDERS
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_squad_central_defender_reports AS
SELECT 
    player_key,
    player_name,
    age,
    minutes_played,
    on_pitch_goal_diff_per90 AS tG_differential_per90,
    prog_rate_per_pass,
    tackles_attempted_per90,
    tackle_accuracy,
    key_tackles_per90,
    defensive_actions_per90,
    reactive_vs_positional,
    pressure_efficiency,
    pressure_accuracy,
    possession_differential,
    aerial_duels_won_per90,
    heading_accuracy,
    clearance_per_aerial,
    foul_per_tackle,
    discipline_index
FROM gold.mart_outfield_scouting_summary
WHERE (is_own_squad = 1 OR contracted = 1)
  AND position = 'Central Defender'
  AND minutes_played >= 1000;
GO

-- ======================================================================
-- 6. SQUAD REPORTS: FLANK DEFENDERS
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_squad_flank_defender_reports AS
SELECT 
    player_key,
    player_name,
    age,
    position,
    minutes_played,
    on_pitch_goal_diff_per90 AS tG_differential_per90,
    (goals_per90 + assists_per90) AS GA_per90,
    (xGoals_per90 + xAssits_per90) AS xGA_per90,
    npxg_plus_xa_per90,
    open_play_key_passes_per90,
    completed_crosses_per90,
    open_play_cross_accuracy,
    prog_rate_per_pass,
    turnover_rate,
    defensive_actions_per90,
    reactive_vs_positional,
    pressure_efficiency,
    pressure_accuracy,
    discipline_index,
    possession_differential,
    sprints_per90
FROM gold.mart_outfield_scouting_summary
WHERE (is_own_squad = 1 OR contracted = 1)
  AND position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender')
  AND minutes_played >= 1000;
GO



-- ======================================================================
-- 7. SQUAD REPORTS: CENTRAL MIDFIELDERS
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_squad_midfielder_reports AS 
SELECT 
    player_key,
    player_name,
    age,
    position,
    minutes_played,
    on_pitch_goal_diff_per90 AS tG_differential_per90,
    npxg_plus_xa_per90,
    assist_luck,
    passes_attempted_per90,
    pass_accuracy,
    progressive_passes_per90,
    prog_rate_per_pass,
    turnover_rate,
    chance_creation_per_pass_pct,
    xa_per_op_kp,
    defensive_actions_per90,
    reactive_vs_positional,
    pressure_efficiency,
    pressure_accuracy,
    discipline_index,
    possession_differential,
    sprints_per90
FROM gold.mart_outfield_scouting_summary
WHERE (is_own_squad = 1 OR contracted = 1)
  AND position = 'Central Midfielder'
  AND minutes_played >= 1000;
GO



-- ======================================================================
-- 8. SQUAD REPORTS: ATTACKING MIDFIELDERS
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_squad_attacking_midfielder_reports AS
SELECT 
    player_key,
    player_name,
    age,
    position,
    minutes_played,
    on_pitch_goal_diff_per90 AS tG_differential_per90,
    (goals_per90 + assists_per90) AS GA_per90,
    (xGoals_per90 + xAssits_per90) AS xGA_per90,
    npxg_plus_xa_per90,
    assist_luck,
    goal_threat_share,
    open_play_key_passes_per90,
    xa_per_op_kp,
    prog_rate_per_pass,
    dribbles_made_per90,
    completed_crosses_per90,
    open_play_cross_accuracy,
    pressure_efficiency,
    pressure_accuracy,
    possession_differential,
    sprints_per90
FROM gold.mart_outfield_scouting_summary
WHERE (is_own_squad = 1 OR contracted = 1)
  AND position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Central Attacking Midfielder')
  AND minutes_played >= 1000;
GO



-- ======================================================================
-- 9. SQUAD REPORTS: STRIKERS
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_squad_striker_reports AS
SELECT 
    player_key,
    player_name,
    age,
    position,
    minutes_played,
    on_pitch_goal_diff_per90 AS team_goal_diff_per90,
    goals_per90,
    assists_per90,
    (goals_per90 + assists_per90) AS GA_per90,
    xGoals_per90,
    non_penalty_xGoals_per90,
    xAssits_per90,
    npxg_plus_xa_per90,
    goal_threat_share,
    finishing_overperf_shrunk,
    (goals_per90 - xGoals_per90) AS finishing_delta,
    conversion_rate,
    shot_on_target_rate,
    xGoals_per_shot,
    shots_outside_box_ratio,
    aerial_duels_won_per90,
    heading_accuracy,
    pressure_efficiency,
    pressure_accuracy,
    possession_differential,
    sprints_per90
FROM gold.mart_outfield_scouting_summary
WHERE (is_own_squad = 1 OR contracted = 1)
  AND position = 'Striker'
  AND minutes_played >= 1000;
GO



-- ======================================================================
-- 10. SCOUTING SIMILARITY ENGINES (BY POSITION)
-- ======================================================================
-- Each engine computes a multi-attribute Euclidean distance between
-- external transfer targets (is_own_squad = 0) and the user club's
-- primary starter (the top-minute player in that position).
-- Metrics are weighted to reflect positional tactical archetypes,
-- and all terms are protected with COALESCE against NULL values.
-- ======================================================================

-- A. STRIKER REPLACEMENT SIMILARITY ENGINE
CREATE OR ALTER VIEW gold.vw_similar_strikers AS
WITH benchmark_striker AS (
    SELECT TOP 1
        non_penalty_xGoals_per90,
        shot_on_target_rate,
        aerial_duels_won_per90,
        heading_accuracy,
        pressure_efficiency,
        xGoals_per_shot,
        goal_threat_share
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 1
      AND position = 'Striker'
    ORDER BY minutes_played DESC
)
SELECT 
    DENSE_RANK() OVER (ORDER BY 
        SQRT(
            SQUARE((COALESCE(tgt.non_penalty_xGoals_per90, 0.0) - COALESCE(bmk.non_penalty_xGoals_per90, 0.0)) * 3.0) +
            SQUARE((COALESCE(tgt.shot_on_target_rate, 0.40) - COALESCE(bmk.shot_on_target_rate, 0.40)) * 5.0) +
            SQUARE((COALESCE(tgt.aerial_duels_won_per90, 0.0) - COALESCE(bmk.aerial_duels_won_per90, 0.0)) * 1.5) +
            SQUARE((COALESCE(tgt.heading_accuracy, 0.50) - COALESCE(bmk.heading_accuracy, 0.50)) * 4.0) +
            SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0) +
            SQUARE((COALESCE(tgt.xGoals_per_shot, 0.12) - COALESCE(bmk.xGoals_per_shot, 0.12)) * 10.0) +
            SQUARE((COALESCE(tgt.goal_threat_share, 0.65) - COALESCE(bmk.goal_threat_share, 0.65)) * 3.0)
        ) ASC
    ) AS similarity_rank,
    CAST(SQRT(
        SQUARE((COALESCE(tgt.non_penalty_xGoals_per90, 0.0) - COALESCE(bmk.non_penalty_xGoals_per90, 0.0)) * 3.0) +
        SQUARE((COALESCE(tgt.shot_on_target_rate, 0.40) - COALESCE(bmk.shot_on_target_rate, 0.40)) * 5.0) +
        SQUARE((COALESCE(tgt.aerial_duels_won_per90, 0.0) - COALESCE(bmk.aerial_duels_won_per90, 0.0)) * 1.5) +
        SQUARE((COALESCE(tgt.heading_accuracy, 0.50) - COALESCE(bmk.heading_accuracy, 0.50)) * 4.0) +
        SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0) +
        SQUARE((COALESCE(tgt.xGoals_per_shot, 0.12) - COALESCE(bmk.xGoals_per_shot, 0.12)) * 10.0) +
        SQUARE((COALESCE(tgt.goal_threat_share, 0.65) - COALESCE(bmk.goal_threat_share, 0.65)) * 3.0)
    ) AS DECIMAL(5,2)) AS similarity_distance,
    tgt.player_key,
    tgt.player_name,
    tgt.age,
    tgt.club_name,
    tgt.league,
    tgt.wage_per_week_k,
    tgt.transfer_value_m,
    tgt.minutes_played,
    tgt.goals_per90,
    tgt.non_penalty_xGoals_per90,
    tgt.finishing_overperf_shrunk,
    tgt.goal_threat_share,
    tgt.shot_on_target_rate,
    tgt.xGoals_per_shot,
    tgt.aerial_duels_won_per90,
    tgt.heading_accuracy,
    tgt.pressure_efficiency,
    tgt.cost_per_xgi
FROM gold.mart_outfield_scouting_summary AS tgt
CROSS JOIN benchmark_striker AS bmk
WHERE tgt.is_own_squad = 0                  -- External transfer targets only
  AND tgt.position = 'Striker'
  AND tgt.minutes_played >= 900;
GO

-- B. CENTRAL DEFENDER REPLACEMENT SIMILARITY ENGINE
CREATE OR ALTER VIEW gold.vw_similar_central_defenders AS
WITH benchmark_cd AS (
    SELECT TOP 1
        aerial_duels_won_per90,
        heading_accuracy,
        prog_rate_per_pass,
        progressive_passes_per90,
        defensive_actions_per90,
        tackle_accuracy,
        reactive_vs_positional,
        discipline_index
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 1
      AND position = 'Central Defender'
    ORDER BY minutes_played DESC
)
SELECT 
    DENSE_RANK() OVER (ORDER BY 
        SQRT(
            SQUARE((COALESCE(tgt.aerial_duels_won_per90, 0.0) - COALESCE(bmk.aerial_duels_won_per90, 0.0)) * 1.5) +
            SQUARE((COALESCE(tgt.heading_accuracy, 0.65) - COALESCE(bmk.heading_accuracy, 0.65)) * 5.0) +
            SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
            SQUARE(COALESCE(tgt.progressive_passes_per90, 0.0) - COALESCE(bmk.progressive_passes_per90, 0.0)) +
            SQUARE(((COALESCE(tgt.defensive_actions_per90, 0.0) - COALESCE(bmk.defensive_actions_per90, 0.0)) / 2.0)) +
            SQUARE((COALESCE(tgt.tackle_accuracy, 0.75) - COALESCE(bmk.tackle_accuracy, 0.75)) * 5.0) +
            SQUARE((COALESCE(tgt.reactive_vs_positional, 0.50) - COALESCE(bmk.reactive_vs_positional, 0.50)) * 5.0) +
            SQUARE(((COALESCE(tgt.discipline_index, 0.0) - COALESCE(bmk.discipline_index, 0.0)) / 3.0))
        ) ASC
    ) AS similarity_rank,
    CAST(SQRT(
        SQUARE((COALESCE(tgt.aerial_duels_won_per90, 0.0) - COALESCE(bmk.aerial_duels_won_per90, 0.0)) * 1.5) +
        SQUARE((COALESCE(tgt.heading_accuracy, 0.65) - COALESCE(bmk.heading_accuracy, 0.65)) * 5.0) +
        SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
        SQUARE(COALESCE(tgt.progressive_passes_per90, 0.0) - COALESCE(bmk.progressive_passes_per90, 0.0)) +
        SQUARE(((COALESCE(tgt.defensive_actions_per90, 0.0) - COALESCE(bmk.defensive_actions_per90, 0.0)) / 2.0)) +
        SQUARE((COALESCE(tgt.tackle_accuracy, 0.75) - COALESCE(bmk.tackle_accuracy, 0.75)) * 5.0) +
        SQUARE((COALESCE(tgt.reactive_vs_positional, 0.50) - COALESCE(bmk.reactive_vs_positional, 0.50)) * 5.0) +
        SQUARE(((COALESCE(tgt.discipline_index, 0.0) - COALESCE(bmk.discipline_index, 0.0)) / 3.0))
    ) AS DECIMAL(5,2)) AS similarity_distance,
    tgt.player_key,
    tgt.player_name,
    tgt.age,
    tgt.club_name,
    tgt.league,
    tgt.wage_per_week_k,
    tgt.transfer_value_m,
    tgt.minutes_played,
    tgt.aerial_duels_won_per90,
    tgt.heading_accuracy,
    tgt.prog_rate_per_pass,
    tgt.progressive_passes_per90,
    tgt.defensive_actions_per90,
    tgt.tackle_accuracy,
    tgt.reactive_vs_positional,
    tgt.clearance_per_aerial,
    tgt.foul_per_tackle,
    tgt.discipline_index,
    tgt.cost_per_xgi
FROM gold.mart_outfield_scouting_summary AS tgt
CROSS JOIN benchmark_cd AS bmk
WHERE tgt.is_own_squad = 0
  AND tgt.position = 'Central Defender'
  AND tgt.minutes_played >= 900;
GO

-- C. FLANK DEFENDER (FULLBACK/WINGBACK) REPLACEMENT SIMILARITY ENGINE
CREATE OR ALTER VIEW gold.vw_similar_flank_defenders AS
WITH benchmark_flank AS (
    SELECT TOP 1
        completed_crosses_per90,
        open_play_cross_accuracy,
        prog_rate_per_pass,
        turnover_rate,
        defensive_actions_per90,
        tackle_accuracy,
        sprints_per90
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 1
      AND position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender')
    ORDER BY minutes_played DESC
)
SELECT 
    DENSE_RANK() OVER (ORDER BY 
        SQRT(
            SQUARE((COALESCE(tgt.completed_crosses_per90, 0.0) - COALESCE(bmk.completed_crosses_per90, 0.0)) * 2.0) +
            SQUARE((COALESCE(tgt.open_play_cross_accuracy, 0.15) - COALESCE(bmk.open_play_cross_accuracy, 0.15)) * 5.0) +
            SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
            SQUARE((COALESCE(tgt.turnover_rate, 0.15) - COALESCE(bmk.turnover_rate, 0.15)) * 5.0) +
            SQUARE(((COALESCE(tgt.defensive_actions_per90, 0.0) - COALESCE(bmk.defensive_actions_per90, 0.0)) / 2.0)) +
            SQUARE((COALESCE(tgt.tackle_accuracy, 0.75) - COALESCE(bmk.tackle_accuracy, 0.75)) * 5.0) +
            SQUARE(((COALESCE(tgt.sprints_per90, 0.0) - COALESCE(bmk.sprints_per90, 0.0)) / 5.0))
        ) ASC
    ) AS similarity_rank,
    CAST(SQRT(
        SQUARE((COALESCE(tgt.completed_crosses_per90, 0.0) - COALESCE(bmk.completed_crosses_per90, 0.0)) * 2.0) +
        SQUARE((COALESCE(tgt.open_play_cross_accuracy, 0.15) - COALESCE(bmk.open_play_cross_accuracy, 0.15)) * 5.0) +
        SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
        SQUARE((COALESCE(tgt.turnover_rate, 0.15) - COALESCE(bmk.turnover_rate, 0.15)) * 5.0) +
        SQUARE(((COALESCE(tgt.defensive_actions_per90, 0.0) - COALESCE(bmk.defensive_actions_per90, 0.0)) / 2.0)) +
        SQUARE((COALESCE(tgt.tackle_accuracy, 0.75) - COALESCE(bmk.tackle_accuracy, 0.75)) * 5.0) +
        SQUARE(((COALESCE(tgt.sprints_per90, 0.0) - COALESCE(bmk.sprints_per90, 0.0)) / 5.0))
    ) AS DECIMAL(5,2)) AS similarity_distance,
    tgt.player_key,
    tgt.player_name,
    tgt.age,
    tgt.position,
    tgt.club_name,
    tgt.league,
    tgt.wage_per_week_k,
    tgt.transfer_value_m,
    tgt.minutes_played,
    tgt.completed_crosses_per90,
    tgt.open_play_cross_accuracy,
    tgt.prog_rate_per_pass,
    tgt.turnover_rate,
    tgt.defensive_actions_per90,
    tgt.tackle_accuracy,
    tgt.sprints_per90,
    tgt.cost_per_xgi
FROM gold.mart_outfield_scouting_summary AS tgt
CROSS JOIN benchmark_flank AS bmk
WHERE tgt.is_own_squad = 0
  AND tgt.position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender')
  AND tgt.minutes_played >= 900;
GO

-- D. DEFENSIVE MIDFIELDER (DM / ANCHOR) REPLACEMENT SIMILARITY ENGINE
CREATE OR ALTER VIEW gold.vw_similar_defensive_midfielders AS
WITH benchmark_dm AS (
    SELECT TOP 1
        defensive_actions_per90,
        tackle_accuracy,
        reactive_vs_positional,
        aerial_duels_won_per90,
        progressive_passes_per90,
        prog_rate_per_pass,
        passes_attempted_per90,
        pass_accuracy,
        turnover_rate,
        pressure_efficiency,
        discipline_index
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 1
      AND position IN ('Defensive Midfielder', 'Central Midfielder')
    ORDER BY CASE WHEN position = 'Defensive Midfielder' THEN 0 ELSE 1 END, minutes_played DESC
)
SELECT 
    DENSE_RANK() OVER (ORDER BY 
        SQRT(
            SQUARE(((COALESCE(tgt.defensive_actions_per90, 0.0) - COALESCE(bmk.defensive_actions_per90, 0.0)) / 2.0)) +
            SQUARE((COALESCE(tgt.tackle_accuracy, 0.75) - COALESCE(bmk.tackle_accuracy, 0.75)) * 5.0) +
            SQUARE((COALESCE(tgt.reactive_vs_positional, 0.50) - COALESCE(bmk.reactive_vs_positional, 0.50)) * 5.0) +
            SQUARE((COALESCE(tgt.aerial_duels_won_per90, 0.0) - COALESCE(bmk.aerial_duels_won_per90, 0.0)) * 1.5) +
            SQUARE(COALESCE(tgt.progressive_passes_per90, 0.0) - COALESCE(bmk.progressive_passes_per90, 0.0)) +
            SQUARE(((COALESCE(tgt.passes_attempted_per90, 0.0) - COALESCE(bmk.passes_attempted_per90, 0.0)) / 10.0)) +
            SQUARE((COALESCE(tgt.pass_accuracy, 0.85) - COALESCE(bmk.pass_accuracy, 0.85)) * 5.0) +
            SQUARE((COALESCE(tgt.turnover_rate, 0.15) - COALESCE(bmk.turnover_rate, 0.15)) * 5.0) +
            SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0) +
            SQUARE(((COALESCE(tgt.discipline_index, 0.0) - COALESCE(bmk.discipline_index, 0.0)) / 3.0))
        ) ASC
    ) AS similarity_rank,
    CAST(SQRT(
        SQUARE(((COALESCE(tgt.defensive_actions_per90, 0.0) - COALESCE(bmk.defensive_actions_per90, 0.0)) / 2.0)) +
        SQUARE((COALESCE(tgt.tackle_accuracy, 0.75) - COALESCE(bmk.tackle_accuracy, 0.75)) * 5.0) +
        SQUARE((COALESCE(tgt.reactive_vs_positional, 0.50) - COALESCE(bmk.reactive_vs_positional, 0.50)) * 5.0) +
        SQUARE((COALESCE(tgt.aerial_duels_won_per90, 0.0) - COALESCE(bmk.aerial_duels_won_per90, 0.0)) * 1.5) +
        SQUARE(COALESCE(tgt.progressive_passes_per90, 0.0) - COALESCE(bmk.progressive_passes_per90, 0.0)) +
        SQUARE(((COALESCE(tgt.passes_attempted_per90, 0.0) - COALESCE(bmk.passes_attempted_per90, 0.0)) / 10.0)) +
        SQUARE((COALESCE(tgt.pass_accuracy, 0.85) - COALESCE(bmk.pass_accuracy, 0.85)) * 5.0) +
        SQUARE((COALESCE(tgt.turnover_rate, 0.15) - COALESCE(bmk.turnover_rate, 0.15)) * 5.0) +
        SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0) +
        SQUARE(((COALESCE(tgt.discipline_index, 0.0) - COALESCE(bmk.discipline_index, 0.0)) / 3.0))
    ) AS DECIMAL(5,2)) AS similarity_distance,
    tgt.player_key,
    tgt.player_name,
    tgt.age,
    tgt.position,
    tgt.club_name,
    tgt.league,
    tgt.wage_per_week_k,
    tgt.transfer_value_m,
    tgt.minutes_played,
    tgt.defensive_actions_per90,
    tgt.tackle_accuracy,
    tgt.reactive_vs_positional,
    tgt.aerial_duels_won_per90,
    tgt.progressive_passes_per90,
    tgt.prog_rate_per_pass,
    tgt.passes_attempted_per90,
    tgt.pass_accuracy,
    tgt.turnover_rate,
    tgt.pressure_efficiency,
    tgt.discipline_index,
    tgt.cost_per_xgi
FROM gold.mart_outfield_scouting_summary AS tgt
CROSS JOIN benchmark_dm AS bmk
WHERE tgt.is_own_squad = 0
  AND tgt.position IN ('Defensive Midfielder', 'Central Midfielder')
  AND tgt.minutes_played >= 900;
GO

-- E. CENTRAL MIDFIELDER (BOX-TO-BOX / PLAYMAKER) REPLACEMENT SIMILARITY ENGINE
CREATE OR ALTER VIEW gold.vw_similar_central_midfielders AS
WITH benchmark_cm AS (
    SELECT TOP 1
        progressive_passes_per90,
        prog_rate_per_pass,
        passes_attempted_per90,
        pass_accuracy,
        turnover_rate,
        npxg_plus_xa_per90,
        defensive_actions_per90,
        pressure_efficiency
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 1
      AND position IN ('Central Midfielder', 'Defensive Midfielder')
    ORDER BY CASE WHEN position = 'Central Midfielder' THEN 0 ELSE 1 END, minutes_played DESC
)
SELECT 
    DENSE_RANK() OVER (ORDER BY 
        SQRT(
            SQUARE(COALESCE(tgt.progressive_passes_per90, 0.0) - COALESCE(bmk.progressive_passes_per90, 0.0)) +
            SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
            SQUARE(((COALESCE(tgt.passes_attempted_per90, 0.0) - COALESCE(bmk.passes_attempted_per90, 0.0)) / 10.0)) +
            SQUARE((COALESCE(tgt.pass_accuracy, 0.85) - COALESCE(bmk.pass_accuracy, 0.85)) * 5.0) +
            SQUARE((COALESCE(tgt.turnover_rate, 0.15) - COALESCE(bmk.turnover_rate, 0.15)) * 5.0) +
            SQUARE((COALESCE(tgt.npxg_plus_xa_per90, 0.0) - COALESCE(bmk.npxg_plus_xa_per90, 0.0)) * 3.0) +
            SQUARE(((COALESCE(tgt.defensive_actions_per90, 0.0) - COALESCE(bmk.defensive_actions_per90, 0.0)) / 2.0)) +
            SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0)
        ) ASC
    ) AS similarity_rank,
    CAST(SQRT(
        SQUARE(COALESCE(tgt.progressive_passes_per90, 0.0) - COALESCE(bmk.progressive_passes_per90, 0.0)) +
        SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
        SQUARE(((COALESCE(tgt.passes_attempted_per90, 0.0) - COALESCE(bmk.passes_attempted_per90, 0.0)) / 10.0)) +
        SQUARE((COALESCE(tgt.pass_accuracy, 0.85) - COALESCE(bmk.pass_accuracy, 0.85)) * 5.0) +
        SQUARE((COALESCE(tgt.turnover_rate, 0.15) - COALESCE(bmk.turnover_rate, 0.15)) * 5.0) +
        SQUARE((COALESCE(tgt.npxg_plus_xa_per90, 0.0) - COALESCE(bmk.npxg_plus_xa_per90, 0.0)) * 3.0) +
        SQUARE(((COALESCE(tgt.defensive_actions_per90, 0.0) - COALESCE(bmk.defensive_actions_per90, 0.0)) / 2.0)) +
        SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0)
    ) AS DECIMAL(5,2)) AS similarity_distance,
    tgt.player_key,
    tgt.player_name,
    tgt.age,
    tgt.position,
    tgt.club_name,
    tgt.league,
    tgt.wage_per_week_k,
    tgt.transfer_value_m,
    tgt.minutes_played,
    tgt.passes_attempted_per90,
    tgt.pass_accuracy,
    tgt.progressive_passes_per90,
    tgt.prog_rate_per_pass,
    tgt.turnover_rate,
    tgt.npxg_plus_xa_per90,
    tgt.defensive_actions_per90,
    tgt.pressure_efficiency,
    tgt.cost_per_xgi
FROM gold.mart_outfield_scouting_summary AS tgt
CROSS JOIN benchmark_cm AS bmk
WHERE tgt.is_own_squad = 0
  AND tgt.position IN ('Central Midfielder', 'Defensive Midfielder')
  AND tgt.minutes_played >= 900;
GO

-- Alias for backwards compatibility
CREATE OR ALTER VIEW gold.vw_similar_midfielders AS
SELECT * FROM gold.vw_similar_central_midfielders;
GO

-- F. WINGER & WIDE ATTACKER REPLACEMENT SIMILARITY ENGINE
CREATE OR ALTER VIEW gold.vw_similar_wingers AS
WITH benchmark_winger AS (
    SELECT TOP 1
        npxg_plus_xa_per90,
        dribbles_made_per90,
        completed_crosses_per90,
        open_play_cross_accuracy,
        xa_per_op_kp,
        sprints_per90,
        prog_rate_per_pass,
        goal_threat_share,
        pressure_efficiency
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 1
      AND position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder')
    ORDER BY minutes_played DESC
)
SELECT 
    DENSE_RANK() OVER (ORDER BY 
        SQRT(
            SQUARE((COALESCE(tgt.npxg_plus_xa_per90, 0.0) - COALESCE(bmk.npxg_plus_xa_per90, 0.0)) * 3.0) +
            SQUARE(COALESCE(tgt.dribbles_made_per90, 0.0) - COALESCE(bmk.dribbles_made_per90, 0.0)) +
            SQUARE((COALESCE(tgt.completed_crosses_per90, 0.0) - COALESCE(bmk.completed_crosses_per90, 0.0)) * 2.0) +
            SQUARE((COALESCE(tgt.open_play_cross_accuracy, 0.15) - COALESCE(bmk.open_play_cross_accuracy, 0.15)) * 5.0) +
            SQUARE((COALESCE(tgt.xa_per_op_kp, 0.20) - COALESCE(bmk.xa_per_op_kp, 0.20)) * 5.0) +
            SQUARE(((COALESCE(tgt.sprints_per90, 0.0) - COALESCE(bmk.sprints_per90, 0.0)) / 5.0)) +
            SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
            SQUARE((COALESCE(tgt.goal_threat_share, 0.50) - COALESCE(bmk.goal_threat_share, 0.50)) * 3.0) +
            SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0)
        ) ASC
    ) AS similarity_rank,
    CAST(SQRT(
        SQUARE((COALESCE(tgt.npxg_plus_xa_per90, 0.0) - COALESCE(bmk.npxg_plus_xa_per90, 0.0)) * 3.0) +
        SQUARE(COALESCE(tgt.dribbles_made_per90, 0.0) - COALESCE(bmk.dribbles_made_per90, 0.0)) +
        SQUARE((COALESCE(tgt.completed_crosses_per90, 0.0) - COALESCE(bmk.completed_crosses_per90, 0.0)) * 2.0) +
        SQUARE((COALESCE(tgt.open_play_cross_accuracy, 0.15) - COALESCE(bmk.open_play_cross_accuracy, 0.15)) * 5.0) +
        SQUARE((COALESCE(tgt.xa_per_op_kp, 0.20) - COALESCE(bmk.xa_per_op_kp, 0.20)) * 5.0) +
        SQUARE(((COALESCE(tgt.sprints_per90, 0.0) - COALESCE(bmk.sprints_per90, 0.0)) / 5.0)) +
        SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
        SQUARE((COALESCE(tgt.goal_threat_share, 0.50) - COALESCE(bmk.goal_threat_share, 0.50)) * 3.0) +
        SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0)
    ) AS DECIMAL(5,2)) AS similarity_distance,
    tgt.player_key,
    tgt.player_name,
    tgt.age,
    tgt.position,
    tgt.club_name,
    tgt.league,
    tgt.wage_per_week_k,
    tgt.transfer_value_m,
    tgt.minutes_played,
    tgt.npxg_plus_xa_per90,
    tgt.assist_luck,
    tgt.xa_per_op_kp,
    tgt.dribbles_made_per90,
    tgt.completed_crosses_per90,
    tgt.open_play_cross_accuracy,
    tgt.prog_rate_per_pass,
    tgt.sprints_per90,
    tgt.goal_threat_share,
    tgt.pressure_efficiency,
    tgt.cost_per_xgi
FROM gold.mart_outfield_scouting_summary AS tgt
CROSS JOIN benchmark_winger AS bmk
WHERE tgt.is_own_squad = 0
  AND tgt.position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder')
  AND tgt.minutes_played >= 900;
GO

-- G. CENTRAL ATTACKING MIDFIELDER (CAM / #10) REPLACEMENT SIMILARITY ENGINE
CREATE OR ALTER VIEW gold.vw_similar_attacking_midfielders AS
WITH benchmark_am AS (
    SELECT TOP 1
        npxg_plus_xa_per90,
        xa_per_op_kp,
        open_play_key_passes_per90,
        prog_rate_per_pass,
        dribbles_made_per90,
        turnover_rate,
        goal_threat_share,
        pressure_efficiency
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 1
      AND position IN ('Central Attacking Midfielder', 'Right Attacking Midfielder', 'Left Attacking Midfielder')
    ORDER BY CASE WHEN position = 'Central Attacking Midfielder' THEN 0 ELSE 1 END, minutes_played DESC
)
SELECT 
    DENSE_RANK() OVER (ORDER BY 
        SQRT(
            SQUARE((COALESCE(tgt.npxg_plus_xa_per90, 0.0) - COALESCE(bmk.npxg_plus_xa_per90, 0.0)) * 3.0) +
            SQUARE((COALESCE(tgt.xa_per_op_kp, 0.20) - COALESCE(bmk.xa_per_op_kp, 0.20)) * 5.0) +
            SQUARE((COALESCE(tgt.open_play_key_passes_per90, 0.0) - COALESCE(bmk.open_play_key_passes_per90, 0.0)) * 2.0) +
            SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
            SQUARE(COALESCE(tgt.dribbles_made_per90, 0.0) - COALESCE(bmk.dribbles_made_per90, 0.0)) +
            SQUARE((COALESCE(tgt.turnover_rate, 0.15) - COALESCE(bmk.turnover_rate, 0.15)) * 5.0) +
            SQUARE((COALESCE(tgt.goal_threat_share, 0.50) - COALESCE(bmk.goal_threat_share, 0.50)) * 3.0) +
            SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0)
        ) ASC
    ) AS similarity_rank,
    CAST(SQRT(
        SQUARE((COALESCE(tgt.npxg_plus_xa_per90, 0.0) - COALESCE(bmk.npxg_plus_xa_per90, 0.0)) * 3.0) +
        SQUARE((COALESCE(tgt.xa_per_op_kp, 0.20) - COALESCE(bmk.xa_per_op_kp, 0.20)) * 5.0) +
        SQUARE((COALESCE(tgt.open_play_key_passes_per90, 0.0) - COALESCE(bmk.open_play_key_passes_per90, 0.0)) * 2.0) +
        SQUARE(((COALESCE(tgt.prog_rate_per_pass, 0.0) - COALESCE(bmk.prog_rate_per_pass, 0.0)) / 5.0)) +
        SQUARE(COALESCE(tgt.dribbles_made_per90, 0.0) - COALESCE(bmk.dribbles_made_per90, 0.0)) +
        SQUARE((COALESCE(tgt.turnover_rate, 0.15) - COALESCE(bmk.turnover_rate, 0.15)) * 5.0) +
        SQUARE((COALESCE(tgt.goal_threat_share, 0.50) - COALESCE(bmk.goal_threat_share, 0.50)) * 3.0) +
        SQUARE((COALESCE(tgt.pressure_efficiency, 0.25) - COALESCE(bmk.pressure_efficiency, 0.25)) * 3.0)
    ) AS DECIMAL(5,2)) AS similarity_distance,
    tgt.player_key,
    tgt.player_name,
    tgt.age,
    tgt.position,
    tgt.club_name,
    tgt.league,
    tgt.wage_per_week_k,
    tgt.transfer_value_m,
    tgt.minutes_played,
    tgt.npxg_plus_xa_per90,
    tgt.assist_luck,
    tgt.xa_per_op_kp,
    tgt.open_play_key_passes_per90,
    tgt.dribbles_made_per90,
    tgt.prog_rate_per_pass,
    tgt.turnover_rate,
    tgt.goal_threat_share,
    tgt.pressure_efficiency,
    tgt.cost_per_xgi
FROM gold.mart_outfield_scouting_summary AS tgt
CROSS JOIN benchmark_am AS bmk
WHERE tgt.is_own_squad = 0
  AND tgt.position IN ('Central Attacking Midfielder', 'Right Attacking Midfielder', 'Left Attacking Midfielder')
  AND tgt.minutes_played >= 900;
GO

-- H. GOALKEEPER REPLACEMENT SIMILARITY ENGINE
CREATE OR ALTER VIEW gold.vw_similar_goalkeepers AS
WITH benchmark_gk AS (
    SELECT TOP 1
        shot_stopping_delta,
        xGoals_prevented_per90,
        hold_rate_pct,
        sweeper_index,
        distribution_value,
        pass_accuracy
    FROM gold.mart_gk_scouting_summary
    WHERE is_own_squad = 1
    ORDER BY minutes_played DESC
)
SELECT 
    DENSE_RANK() OVER (ORDER BY 
        SQRT(
            SQUARE((COALESCE(tgt.shot_stopping_delta, 0.0) - COALESCE(bmk.shot_stopping_delta, 0.0)) * 5.0) +
            SQUARE((COALESCE(tgt.xGoals_prevented_per90, 0.0) - COALESCE(bmk.xGoals_prevented_per90, 0.0)) * 3.0) +
            SQUARE(((COALESCE(tgt.hold_rate_pct, 40.0) - COALESCE(bmk.hold_rate_pct, 40.0)) / 10.0)) +
            SQUARE(((COALESCE(tgt.sweeper_index, 0.0) - COALESCE(bmk.sweeper_index, 0.0)) / 3.0)) +
            SQUARE(((COALESCE(tgt.distribution_value, 0.0) - COALESCE(bmk.distribution_value, 0.0)) / 10.0)) +
            SQUARE((COALESCE(tgt.pass_accuracy, 0.70) - COALESCE(bmk.pass_accuracy, 0.70)) * 5.0)
        ) ASC
    ) AS similarity_rank,
    CAST(SQRT(
        SQUARE((COALESCE(tgt.shot_stopping_delta, 0.0) - COALESCE(bmk.shot_stopping_delta, 0.0)) * 5.0) +
        SQUARE((COALESCE(tgt.xGoals_prevented_per90, 0.0) - COALESCE(bmk.xGoals_prevented_per90, 0.0)) * 3.0) +
        SQUARE(((COALESCE(tgt.hold_rate_pct, 40.0) - COALESCE(bmk.hold_rate_pct, 40.0)) / 10.0)) +
        SQUARE(((COALESCE(tgt.sweeper_index, 0.0) - COALESCE(bmk.sweeper_index, 0.0)) / 3.0)) +
        SQUARE(((COALESCE(tgt.distribution_value, 0.0) - COALESCE(bmk.distribution_value, 0.0)) / 10.0)) +
        SQUARE((COALESCE(tgt.pass_accuracy, 0.70) - COALESCE(bmk.pass_accuracy, 0.70)) * 5.0)
    ) AS DECIMAL(5,2)) AS similarity_distance,
    tgt.player_key,
    tgt.player_name,
    tgt.age,
    tgt.club_name,
    tgt.league,
    tgt.wage_per_week_k,
    tgt.transfer_value_m,
    tgt.minutes_played,
    tgt.shot_stopping_delta,
    tgt.xGoals_prevented_per90,
    tgt.saves_percentage,
    tgt.xSave_rate,
    tgt.hold_rate_pct,
    tgt.sweeper_index,
    tgt.distribution_value,
    tgt.pass_accuracy,
    tgt.error_rate
FROM gold.mart_gk_scouting_summary AS tgt
CROSS JOIN benchmark_gk AS bmk
WHERE tgt.is_own_squad = 0
  AND tgt.minutes_played >= 900;
GO

-- ======================================================================
-- 11. MULTI-SEASON HISTORICAL GROWTH & DEVELOPMENT TRACKING
-- ======================================================================

-- A. WIDE HISTORICAL MART (ALL SNAPSHOTS)
CREATE OR ALTER VIEW gold.mart_outfield_historical_snapshots AS
SELECT 
    p.player_id,
    p.player_name,
    p.position,
    t.club_name,
    t.league,
    p.is_own_squad,
    p.age,
    s.season,
    s.minutes_played,
    v.wage_per_week_k,
    v.transfer_value_m,
    s.goals_per90,
    s.assists_per90,
    s.non_penalty_xGoals_per90,
    s.xAssits_per90,
    (s.non_penalty_xGoals_per90 + s.xAssits_per90) AS npxg_plus_xa_per90,
    s.open_play_key_passes_per90,
    s.dribbles_made_per90,
    s.passes_attempted_per90,
    s.pass_accuracy,
    s.progressive_passes_per90,
    CAST(s.progressive_passes_per90 / NULLIF(s.passes_attempted_per90, 0) * 100.0 AS DECIMAL(5,2)) AS prog_rate_per_pass,
    CAST(((s.tackles_attempted_per90 * s.tackle_accuracy) + s.interceptions_per90 + s.blocks_per90 + s.clearances_per90) AS DECIMAL(5,2)) AS defensive_actions_per90,
    s.tackle_accuracy,
    CAST(s.headers_attempted_per90 * s.heading_accuracy AS DECIMAL(5,2)) AS aerial_duels_won_per90,
    s.heading_accuracy,
    CAST(s.possession_won_per90 / NULLIF(s.pressures_attempted_per90, 0) AS DECIMAL(5,2)) AS pressure_efficiency,
    s.dwh_create_date AS snapshot_date,
    s.dwh_cd_valid_till AS snapshot_valid_till,
    s.dwh_current_validity,
    CASE WHEN s.dwh_current_validity = 1 THEN 'Current' ELSE 'Historical' END AS snapshot_status,
    DENSE_RANK() OVER (PARTITION BY p.player_id ORDER BY s.dwh_create_date ASC) AS snapshot_version
FROM gold.fact_outfield_player_stats s
INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
INNER JOIN gold.dim_team_info t ON s.team_key = t.team_key
LEFT JOIN gold.fact_players_value v ON s.player_key = v.player_key;
GO

-- B. OUTFIELD PLAYER SEASON-OVER-SEASON DEVELOPMENT COMPARISON ENGINE
CREATE OR ALTER VIEW gold.vw_player_growth_tracking AS
WITH season_snapshots AS (
    SELECT 
        player_id,
        player_name,
        position,
        club_name,
        league,
        is_own_squad,
        age,
        season,
        minutes_played,
        wage_per_week_k,
        transfer_value_m,
        npxg_plus_xa_per90,
        open_play_key_passes_per90,
        dribbles_made_per90,
        progressive_passes_per90,
        prog_rate_per_pass,
        defensive_actions_per90,
        tackle_accuracy,
        aerial_duels_won_per90,
        pressure_efficiency,
        snapshot_date,
        snapshot_version,
        DENSE_RANK() OVER (PARTITION BY player_id ORDER BY season ASC) AS season_num,
        ROW_NUMBER() OVER (PARTITION BY player_id, season ORDER BY snapshot_date DESC) AS rn_season_latest
    FROM gold.mart_outfield_historical_snapshots
)
SELECT 
    curr.player_id,
    curr.player_name,
    curr.position,
    curr.club_name AS current_club,
    prev.club_name AS previous_club,
    curr.is_own_squad,
    curr.season AS current_season,
    prev.season AS previous_season,
    curr.season_num AS current_season_num,
    prev.season_num AS previous_season_num,
    curr.age AS current_age,
    prev.age AS previous_age,
    curr.minutes_played AS current_minutes,
    prev.minutes_played AS previous_minutes,

    -- Financial & Market Growth
    curr.transfer_value_m AS current_value_m,
    prev.transfer_value_m AS previous_value_m,
    CAST((curr.transfer_value_m - prev.transfer_value_m) AS DECIMAL(6,2)) AS delta_transfer_value_m,
    CAST((curr.wage_per_week_k - prev.wage_per_week_k) AS DECIMAL(6,2)) AS delta_wage_per_week_k,

    -- Attacking & Creation Growth
    curr.npxg_plus_xa_per90 AS current_npxg_plus_xa,
    prev.npxg_plus_xa_per90 AS previous_npxg_plus_xa,
    CAST((curr.npxg_plus_xa_per90 - prev.npxg_plus_xa_per90) AS DECIMAL(5,2)) AS delta_npxg_plus_xa_per90,
    CAST((curr.open_play_key_passes_per90 - prev.open_play_key_passes_per90) AS DECIMAL(5,2)) AS delta_key_passes_per90,
    CAST((curr.dribbles_made_per90 - prev.dribbles_made_per90) AS DECIMAL(5,2)) AS delta_dribbles_per90,

    -- Passing & Progression Growth
    curr.progressive_passes_per90 AS current_prog_passes,
    prev.progressive_passes_per90 AS previous_prog_passes,
    CAST((curr.progressive_passes_per90 - prev.progressive_passes_per90) AS DECIMAL(5,2)) AS delta_prog_passes_per90,
    CAST((curr.prog_rate_per_pass - prev.prog_rate_per_pass) AS DECIMAL(5,2)) AS delta_prog_rate_pct,

    -- Defending & Physical Growth
    curr.defensive_actions_per90 AS current_def_actions,
    prev.defensive_actions_per90 AS previous_def_actions,
    CAST((curr.defensive_actions_per90 - prev.defensive_actions_per90) AS DECIMAL(5,2)) AS delta_def_actions_per90,
    CAST((curr.aerial_duels_won_per90 - prev.aerial_duels_won_per90) AS DECIMAL(5,2)) AS delta_aerial_duels_won_per90,
    CAST((curr.pressure_efficiency - prev.pressure_efficiency) AS DECIMAL(5,2)) AS delta_pressure_efficiency,

    curr.snapshot_date AS current_snapshot_date,
    prev.snapshot_date AS previous_snapshot_date
FROM season_snapshots curr
INNER JOIN season_snapshots prev 
    ON curr.player_id = prev.player_id 
   AND curr.rn_season_latest = 1
   AND prev.rn_season_latest = 1
   AND curr.season_num = prev.season_num + 1;
GO

-- C. GOALKEEPER SEASON-OVER-SEASON DEVELOPMENT COMPARISON ENGINE
CREATE OR ALTER VIEW gold.vw_gk_growth_tracking AS
WITH gk_snapshots AS (
    SELECT 
        p.player_id,
        p.player_name,
        t.club_name,
        p.is_own_squad,
        p.age,
        s.season,
        s.minutes_played,
        v.transfer_value_m,
        v.wage_per_week_k,
        s.saves_percentage,
        s.xSave_rate,
        (s.saves_percentage - s.xSave_rate) AS shot_stopping_delta,
        s.xGoals_prevented_per90,
        CAST(s.saves_held_per90 / NULLIF(s.saves_made_per90, 0) * 100.0 AS DECIMAL(5,2)) AS hold_rate_pct,
        (s.clearances_per90 + s.interceptions_per90) AS sweeper_index,
        (s.passes_attempted_per90 * s.pass_accuracy) AS distribution_value,
        s.pass_accuracy,
        s.dwh_create_date AS snapshot_date,
        s.dwh_current_validity,
        DENSE_RANK() OVER (PARTITION BY p.player_id ORDER BY s.dwh_create_date ASC) AS snapshot_version,
        DENSE_RANK() OVER (PARTITION BY p.player_id ORDER BY s.season ASC) AS season_num,
        ROW_NUMBER() OVER (PARTITION BY p.player_id, s.season ORDER BY s.dwh_create_date DESC) AS rn_season_latest
    FROM gold.fact_gk_stats s
    INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
    INNER JOIN gold.dim_team_info t ON s.team_key = t.team_key
    LEFT JOIN gold.fact_players_value v ON s.player_key = v.player_key
)
SELECT 
    curr.player_id,
    curr.player_name,
    curr.club_name AS current_club,
    prev.club_name AS previous_club,
    curr.is_own_squad,
    curr.season AS current_season,
    prev.season AS previous_season,
    curr.season_num AS current_season_num,
    prev.season_num AS previous_season_num,
    curr.age AS current_age,
    prev.age AS previous_age,
    curr.minutes_played AS current_minutes,
    prev.minutes_played AS previous_minutes,

    -- Valuation Growth
    CAST((curr.transfer_value_m - prev.transfer_value_m) AS DECIMAL(6,2)) AS delta_transfer_value_m,

    -- Goalkeeping Metric Growth
    curr.shot_stopping_delta AS current_shot_stopping_delta,
    prev.shot_stopping_delta AS previous_shot_stopping_delta,
    CAST((curr.shot_stopping_delta - prev.shot_stopping_delta) AS DECIMAL(5,2)) AS delta_shot_stopping,
    CAST((curr.xGoals_prevented_per90 - prev.xGoals_prevented_per90) AS DECIMAL(5,2)) AS delta_xg_prevented_per90,
    CAST((curr.hold_rate_pct - prev.hold_rate_pct) AS DECIMAL(5,2)) AS delta_hold_rate_pct,
    CAST((curr.sweeper_index - prev.sweeper_index) AS DECIMAL(5,2)) AS delta_sweeper_index,
    CAST((curr.distribution_value - prev.distribution_value) AS DECIMAL(5,2)) AS delta_distribution_value,

    curr.snapshot_date AS current_snapshot_date,
    prev.snapshot_date AS previous_snapshot_date
FROM gk_snapshots curr
INNER JOIN gk_snapshots prev 
    ON curr.player_id = prev.player_id 
   AND curr.rn_season_latest = 1
   AND prev.rn_season_latest = 1
   AND curr.season_num = prev.season_num + 1;
GO

-- ======================================================================
-- 12. ADVANCED TACTICAL & RECRUITMENT INTELLIGENCE
-- ======================================================================

-- A. SQUAD AGE CURVE & SUCCESSION PLANNING VIEW
CREATE OR ALTER VIEW gold.vw_squad_succession_plan AS
WITH squad_aging_starters AS (
    SELECT 
        player_key,
        player_name,
        position,
        age,
        minutes_played,
        transfer_value_m,
        wage_per_week_k,
        CASE 
            WHEN age >= 30 THEN 'Urgent (30+)'
            WHEN age >= 28 THEN 'Approaching (28-29)'
            ELSE 'Stable (U28)'
        END AS replacement_urgency,
        CASE 
            WHEN position IN ('Central Defender') THEN 'CD'
            WHEN position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender') THEN 'Flank'
            WHEN position IN ('Defensive Midfielder') THEN 'DM'
            WHEN position IN ('Central Midfielder') THEN 'CM'
            WHEN position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder') THEN 'Winger'
            WHEN position IN ('Central Attacking Midfielder') THEN 'CAM'
            WHEN position IN ('Striker') THEN 'ST'
            ELSE 'Other'
        END AS pos_group
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 1
      AND minutes_played >= 450
      AND age >= 28
),
u23_candidates AS (
    SELECT 
        player_key AS u23_player_key,
        player_name AS u23_player_name,
        age AS u23_age,
        club_name AS u23_club,
        league AS u23_league,
        position AS u23_position,
        transfer_value_m AS u23_value_m,
        wage_per_week_k AS u23_wage_k,
        minutes_played AS u23_minutes,
        npxg_plus_xa_per90 AS u23_npxg_plus_xa,
        progressive_passes_per90 AS u23_prog_passes,
        defensive_actions_per90 AS u23_def_actions,
        cost_per_xgi AS u23_cost_per_xgi,
        CASE 
            WHEN position IN ('Central Defender') THEN 'CD'
            WHEN position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender') THEN 'Flank'
            WHEN position IN ('Defensive Midfielder') THEN 'DM'
            WHEN position IN ('Central Midfielder') THEN 'CM'
            WHEN position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder') THEN 'Winger'
            WHEN position IN ('Central Attacking Midfielder') THEN 'CAM'
            WHEN position IN ('Striker') THEN 'ST'
            ELSE 'Other'
        END AS pos_group
    FROM gold.mart_outfield_scouting_summary
    WHERE is_own_squad = 0
      AND age <= 23
      AND minutes_played >= 900
),
ranked_successors AS (
    SELECT 
        s.player_name AS veteran_player,
        s.position AS veteran_position,
        s.age AS veteran_age,
        s.minutes_played AS veteran_minutes,
        s.transfer_value_m AS veteran_value_m,
        s.replacement_urgency,
        u.u23_player_name,
        u.u23_position,
        u.u23_age,
        u.u23_club,
        u.u23_league,
        u.u23_value_m,
        u.u23_wage_k,
        u.u23_minutes,
        u.u23_npxg_plus_xa,
        u.u23_prog_passes,
        u.u23_def_actions,
        u.u23_cost_per_xgi,
        ROW_NUMBER() OVER (PARTITION BY s.player_key ORDER BY u.u23_cost_per_xgi ASC, u.u23_minutes DESC) AS candidate_rank
    FROM squad_aging_starters s
    INNER JOIN u23_candidates u ON s.pos_group = u.pos_group
)
SELECT 
    veteran_player,
    veteran_position,
    veteran_age,
    veteran_minutes,
    veteran_value_m,
    replacement_urgency,
    candidate_rank,
    u23_player_name AS recommended_successor,
    u23_position,
    u23_age,
    u23_club,
    u23_league,
    u23_value_m,
    u23_wage_k,
    u23_minutes,
    u23_npxg_plus_xa,
    u23_prog_passes,
    u23_def_actions,
    u23_cost_per_xgi
FROM ranked_successors
WHERE candidate_rank <= 3;
GO

-- B. CONTRACT EXPIRY & VALUE EFFICIENCY BARGAIN WATCH ENGINE
CREATE OR ALTER VIEW gold.vw_bargain_watch AS
SELECT 
    player_key,
    player_name,
    age,
    position,
    club_name,
    league,
    wage_per_week_k,
    transfer_value_m,
    minutes_played,
    npxg_plus_xa_per90,
    goals_per90,
    assists_per90,
    progressive_passes_per90,
    defensive_actions_per90,
    cost_per_xgi,
    CAST((npxg_plus_xa_per90 * 100.0) / NULLIF(transfer_value_m + (wage_per_week_k * 0.052), 0) AS DECIMAL(10,2)) AS attacking_bargain_index,
    CAST((defensive_actions_per90 * 10.0) / NULLIF(transfer_value_m + (wage_per_week_k * 0.052), 0) AS DECIMAL(10,2)) AS defensive_bargain_index,
    DENSE_RANK() OVER (ORDER BY cost_per_xgi ASC) AS cost_efficiency_rank
FROM gold.mart_outfield_scouting_summary
WHERE is_own_squad = 0
  AND minutes_played >= 900
  AND (transfer_value_m <= 15.0 OR wage_per_week_k <= 30.0)
  AND (npxg_plus_xa_per90 >= 0.30 OR defensive_actions_per90 >= 5.00);
GO

-- C. TACTICAL ARCHETYPE PROFILING VIEW
CREATE OR ALTER VIEW gold.vw_tactical_archetypes AS
SELECT 
    player_key,
    player_name,
    age,
    position,
    club_name,
    league,
    is_own_squad,
    minutes_played,
    transfer_value_m,
    wage_per_week_k,
    
    -- Archetype Classification
    CASE 
        -- Strikers
        WHEN position = 'Striker' AND aerial_duels_won_per90 >= 4.0 AND heading_accuracy >= 0.55 THEN 'Target Forward'
        WHEN position = 'Striker' AND non_penalty_xGoals_per90 >= 0.35 AND conversion_rate >= 0.15 THEN 'Poacher / Finisher'
        WHEN position = 'Striker' AND xAssits_per90 >= 0.12 AND open_play_key_passes_per90 >= 0.70 THEN 'Complete / Deep-Lying Forward'
        WHEN position = 'Striker' THEN 'Pressing / Advanced Forward'
        
        -- Central Defenders
        WHEN position = 'Central Defender' AND prog_rate_per_pass >= 5.0 AND progressive_passes_per90 >= 4.0 THEN 'Ball-Playing Defender'
        WHEN position = 'Central Defender' AND aerial_duels_won_per90 >= 3.5 AND heading_accuracy >= 0.65 THEN 'Stopper / No-Nonsense Defender'
        WHEN position = 'Central Defender' AND reactive_vs_positional >= 0.55 THEN 'Cover / Interceptor Defender'
        WHEN position = 'Central Defender' THEN 'Traditional Central Defender'
        
        -- Flank Defenders
        WHEN position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender') AND completed_crosses_per90 >= 1.0 AND sprints_per90 >= 12.0 THEN 'Attacking Wingback'
        WHEN position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender') AND prog_rate_per_pass >= 6.0 AND turnover_rate <= 0.18 THEN 'Inverted Fullback'
        WHEN position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender') AND defensive_actions_per90 >= 5.5 THEN 'Defensive Fullback'
        WHEN position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender') THEN 'Balanced Fullback'
        
        -- Midfielders
        WHEN position IN ('Central Midfielder', 'Defensive Midfielder') AND passes_attempted_per90 >= 55.0 AND pass_accuracy >= 0.88 AND progressive_passes_per90 >= 4.5 THEN 'Deep-Lying Playmaker'
        WHEN position IN ('Central Midfielder', 'Defensive Midfielder') AND defensive_actions_per90 >= 5.5 AND pressure_efficiency >= 0.25 THEN 'Ball-Winning Midfielder'
        WHEN position IN ('Central Midfielder') AND distance_covered_km_per90 >= 13.0 AND sprints_per90 >= 10.0 AND npxg_plus_xa_per90 >= 0.15 THEN 'Box-to-Box Midfielder'
        WHEN position IN ('Central Midfielder', 'Defensive Midfielder') AND prog_rate_per_pass >= 7.0 THEN 'Roaming Playmaker / Mezzala'
        WHEN position IN ('Central Midfielder', 'Defensive Midfielder') THEN 'Central Midfielder'
        
        -- Attacking Midfielders & Wingers
        WHEN position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder') AND goal_threat_share >= 0.55 AND shots_per90 >= 1.8 THEN 'Inside Forward'
        WHEN position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder') AND completed_crosses_per90 >= 1.0 THEN 'Traditional Winger'
        WHEN position IN ('Central Attacking Midfielder') AND goal_threat_share >= 0.50 THEN 'Shadow Striker'
        WHEN position IN ('Central Attacking Midfielder', 'Right Attacking Midfielder', 'Left Attacking Midfielder') AND xAssits_per90 >= 0.20 THEN 'Advanced Playmaker'
        ELSE 'Utility Attacker'
    END AS tactical_archetype,
    
    npxg_plus_xa_per90,
    progressive_passes_per90,
    prog_rate_per_pass,
    defensive_actions_per90,
    aerial_duels_won_per90,
    pressure_efficiency,
    cost_per_xgi
FROM gold.mart_outfield_scouting_summary
WHERE minutes_played >= 450;
GO

-- ======================================================================
-- D. TEAM TACTICAL ARCHETYPES VIEW
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_team_tactical_archetypes AS
SELECT 
    team_key,
    club_name,
    league,
    active_players,
    total_minutes,
    tactical_archetype,
    tactical_framework,
    tactical_description,
    
    -- Minutes-weighted team metrics per 90
    pressures_attempted_per90,
    pressures_completed_per90,
    possession_won_per90,
    possession_lost_per90,
    passes_attempted_per90,
    pass_accuracy,
    progressive_passes_per90,
    open_play_key_passes_per90,
    chances_created_per90,
    xGoals_per_shot,
    crosses_attempted_per90,
    open_play_crosses_attempted_per90,
    headers_attempted_per90,
    heading_accuracy,
    clearances_per90,
    blocks_per90,
    shots_blocked_per90,
    tackles_attempted_per90,
    dribbles_made_per90,
    sprints_per90,
    distance_covered_km_per90,
    fouls_made_per90,
    shots_per90,
    xGoals_per90,
    
    -- Tactical Framework Affinity Scores
    gegenpressing_score,
    tiki_taka_score,
    vertical_tiki_taka_score,
    fluid_counter_score,
    direct_counter_score,
    wing_play_score,
    route_one_score,
    catenaccio_score,
    season
FROM gold.dim_team_tactical_archetypes
WHERE dwh_current_validity = 1;
GO

-- ======================================================================
-- E. PLAYER TACTICAL SYSTEM OUTLIERS VIEW
-- ======================================================================
CREATE OR ALTER VIEW gold.vw_player_system_outliers AS
SELECT 
    p.player_key,
    p.player_name,
    p.age,
    p.position,
    p.club_name,
    p.league,
    p.is_own_squad,
    p.minutes_played,
    p.transfer_value_m,
    p.wage_per_week_k,
    
    -- Team Tactical System
    ta.tactical_archetype AS team_tactical_archetype,
    ta.tactical_framework AS team_tactical_framework,
    
    -- Outlier Flag & Categorization
    CASE 
        -- 1. Striker maintaining high pass accuracy in Route One / Direct Counter
        WHEN p.position = 'Striker' AND ta.tactical_archetype IN ('Route One', 'Direct Counter-Attack') AND p.pass_accuracy >= 0.84 AND p.passes_attempted_per90 >= 12.0
            THEN 'High-Precision Striker in Direct System'
            
        -- 2. Defender logging massive clearances in Tiki-Taka / Possession
        WHEN p.position LIKE '%Defender%' AND ta.tactical_archetype IN ('Tiki-Taka', 'Vertical Tiki-Taka', 'Control Possession') AND p.clearances_per90 >= 1.45
            THEN 'High-Clearance Defender in Tiki-Taka'
            
        -- 3. Attacking Talisman in Deep Block / Catenaccio
        WHEN ta.tactical_archetype = 'Catenaccio' AND p.npxg_plus_xa_per90 >= 0.35
            THEN 'Attacking Talisman in Deep Block'
            
        -- 4. Lone Pressing Engine in passive system
        WHEN p.position IN ('Striker', 'Central Attacking Midfielder') AND ta.pressures_attempted_per90 <= 7.2 AND p.pressures_attempted_per90 >= 13.0
            THEN 'Lone Pressing Engine'
            
        -- 5. Dynamic Ball Carrier in Direct / Route One system
        WHEN ta.tactical_archetype IN ('Route One', 'Direct Counter-Attack') AND p.dribbles_made_per90 >= 1.80
            THEN 'Dynamic Ball Carrier in Direct System'
            
        -- 6. Central Playmaker in Flank System
        WHEN ta.tactical_archetype = 'Wing Play' AND p.position IN ('Central Midfielder', 'Central Attacking Midfielder') AND p.open_play_key_passes_per90 >= 1.30 AND p.crosses_share_of_creation <= 0.25
            THEN 'Central Incision Playmaker in Flank System'
            
        -- 7. Ball-Playing Quarterback in Direct System
        WHEN p.position = 'Central Defender' AND ta.tactical_archetype IN ('Route One', 'Direct Counter-Attack') AND p.progressive_passes_per90 >= 4.50 AND p.pass_accuracy >= 0.86
            THEN 'Ball-Playing Quarterback in Direct System'
            
        -- 8. Ball Retention Hub in High-Intensity Pressing
        WHEN ta.tactical_archetype = 'Gegenpressing' AND p.position IN ('Central Midfielder', 'Defensive Midfielder') AND p.pass_accuracy >= 0.89 AND p.turnover_rate <= 0.14
            THEN 'Ball Retention Hub in Gegenpress'
            
        ELSE NULL
    END AS outlier_tag,
    
    CASE 
        WHEN p.position = 'Striker' AND ta.tactical_archetype IN ('Route One', 'Direct Counter-Attack') AND p.pass_accuracy >= 0.84 AND p.passes_attempted_per90 >= 12.0
            THEN 'Technical Striker defying direct aerial system: achieves high completion rate despite low team passing volume.'
        WHEN p.position LIKE '%Defender%' AND ta.tactical_archetype IN ('Tiki-Taka', 'Vertical Tiki-Taka', 'Control Possession') AND p.clearances_per90 >= 1.45
            THEN 'Resilient stopper executing emergency box clearances despite team philosophy prioritizing possession recycling.'
        WHEN ta.tactical_archetype = 'Catenaccio' AND p.npxg_plus_xa_per90 >= 0.35
            THEN 'High-efficiency attacking talisman creating elite expected goal involvement despite defensive suppression.'
        WHEN p.position IN ('Striker', 'Central Attacking Midfielder') AND ta.pressures_attempted_per90 <= 7.2 AND p.pressures_attempted_per90 >= 13.0
            THEN 'Elite individual pressing work rate operating in a passive low-intensity defensive structure.'
        WHEN ta.tactical_archetype IN ('Route One', 'Direct Counter-Attack') AND p.dribbles_made_per90 >= 1.80
            THEN 'Dynamic individual ball carrier breaking lines via ground take-ons in an aerial long-ball system.'
        WHEN ta.tactical_archetype = 'Wing Play' AND p.position IN ('Central Midfielder', 'Central Attacking Midfielder') AND p.open_play_key_passes_per90 >= 1.30 AND p.crosses_share_of_creation <= 0.25
            THEN 'Central playmaker providing line-breaking incision within an aggressive flank-crossing game model.'
        WHEN p.position = 'Central Defender' AND ta.tactical_archetype IN ('Route One', 'Direct Counter-Attack') AND p.progressive_passes_per90 >= 4.50 AND p.pass_accuracy >= 0.86
            THEN 'Elite progressive ball-playing distributor initiating transitions in a direct attrition setup.'
        WHEN ta.tactical_archetype = 'Gegenpressing' AND p.position IN ('Central Midfielder', 'Defensive Midfielder') AND p.pass_accuracy >= 0.89 AND p.turnover_rate <= 0.14
            THEN 'High-security retention anchor providing calm circulation in a high-turnover pressing system.'
        ELSE NULL
    END AS outlier_rationale,
    
    -- Player output metrics
    p.pass_accuracy AS player_pass_accuracy,
    p.passes_attempted_per90 AS player_passes_attempted_per90,
    p.clearances_per90 AS player_clearances_per90,
    p.npxg_plus_xa_per90 AS player_npxg_plus_xa_per90,
    p.chances_created_per90 AS player_chances_created_per90,
    p.open_play_key_passes_per90 AS player_open_play_key_passes_per90,
    p.progressive_passes_per90 AS player_progressive_passes_per90,
    p.dribbles_made_per90 AS player_dribbles_made_per90,
    p.pressures_attempted_per90 AS player_pressures_attempted_per90,
    p.turnover_rate AS player_turnover_rate,
    
    -- Team benchmarks
    ta.passes_attempted_per90 AS team_passes_attempted_per90,
    ta.pass_accuracy AS team_pass_accuracy,
    ta.clearances_per90 AS team_clearances_per90,
    ta.pressures_attempted_per90 AS team_pressures_attempted_per90,
    ta.headers_attempted_per90 AS team_headers_attempted_per90,
    ta.crosses_attempted_per90 AS team_crosses_attempted_per90,
    ta.blocks_per90 AS team_blocks_per90
FROM gold.mart_outfield_scouting_summary p
JOIN gold.dim_team_tactical_archetypes ta ON p.club_name = ta.club_name AND ta.dwh_current_validity = 1
WHERE p.minutes_played >= 450;
GO