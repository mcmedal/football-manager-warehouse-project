/*
======================================================================
Creates a View: Load Gold View (Silver Layer -> Gold Layer)
======================================================================
   This script is designed to create views from the silver schema that 
   loads tables from the silver layer  into the gold schema. These views
   are structured to facilitate easier access and analysis of the data
   for end-users.
 
======================================================================
*/

--Business Question: "Fiorentina Central Defender Player Reports"
CREATE VIEW gold.vw_fiorentina_central_defender_reports AS
    SELECT 
        A.player_key,
        B.age,
        A.minutes_played,
        (A.team_goals_scored_per90 - A.team_goals_conceded_per90) AS tG_differential_per90,
        CAST((A.open_play_key_passes_per90/A.progressive_passes_per90) AS DECIMAL(5,2)) AS kP_per_ProgP,
        CAST((A.progressive_passes_per90/A.passes_attempted_per90 * 100) AS DECIMAL(5,2)) AS ProgRate_per_Pass,
        A.tackles_attempted_per90,
        A.tackle_accuracy,
        A.key_tackles_per90,
        A.pressures_attempted_per90,
        CAST(((A.pressures_attempted_per90 - A.pressures_completed_per90)/A.pressures_attempted_per90 * 100) AS DECIMAL(5,2)) AS pressure_accuracy,
        (A.possession_won_per90 - A.possession_lost_per90) AS possession_differential,
        CAST(((A.interceptions_per90 * 0.5) + (A.clearances_per90 * 0.25) + (AVG(A.blocks_per90 + A.shots_blocked_per90) * 0.25)) AS DECIMAL(5,2)) AS Wdefensive_actions,
        A.headers_attempted_per90,
        A.heading_accuracy
    FROM silver.fact_outfield_player_stats AS A
    INNER JOIN silver.dim_player_info AS B
        ON A.player_key = B.player_key
    WHERE B.contracted = 1 AND B.position = 'Central Defender'
        AND A.minutes_played >= 1000
        AND A.dwh_current_validity = 1 AND B.dwh_current_validity = 1
    GROUP BY A.player_key, B.age, A.minutes_played, A.team_goals_scored_per90, A.team_goals_conceded_per90,
        A.open_play_key_passes_per90, A.progressive_passes_per90, A.progressive_passes_per90, A.passes_attempted_per90,
        A.tackles_attempted_per90, A.tackle_accuracy, A.key_tackles_per90, A.pressures_attempted_per90,
        A.pressures_attempted_per90, A.pressures_completed_per90, A.possession_won_per90, A.possession_lost_per90, A.interceptions_per90, A.clearances_per90, 
        A.blocks_per90, A.shots_blocked_per90, A.headers_attempted_per90, A.heading_accuracy;
GO

--Business Question: "Fiorentina Flank Defender Reports"
CREATE VIEW gold.vw_fiorentina_flank_defender_reports AS
    SELECT 
        A.player_key,
        B.age,
        B.position,
        A.minutes_played,
        (A.team_goals_scored_per90 - A.team_goals_conceded_per90) AS tG_differential_per90,
        (A.goals_per90 + A.assists_per90) AS GA_per90,
        (A.xGoals_per90 + A.xAssits_per90) AS xGA_per90,
        CAST((A.xAssits_per90 * A.chances_created_per90) AS DECIMAL(5,2)) AS creative_index,
        A.open_play_key_passes_per90,
        CAST(((A.dribbles_made_per90 * 0.65) + (A.passes_attempted_per90 * 0.35)) AS DECIMAL(5,2)) AS progA_score,
        A.open_play_crosses_attempted_per90,
        A.open_play_cross_accuracy,
        A.tackles_attempted_per90,
        A.tackle_accuracy,
        A.key_tackles_per90,
        A.pressures_attempted_per90,
        CAST(((A.pressures_attempted_per90 - A.pressures_completed_per90)/A.pressures_attempted_per90 * 100) AS DECIMAL(5,2)) AS pressure_accuracy,
        A.possession_won_per90,
        (A.possession_won_per90 - A.possession_lost_per90) AS possession_differential,
        CAST(((A.interceptions_per90 * 0.5) + (A.clearances_per90 * 0.25) + (AVG(A.blocks_per90 + A.shots_blocked_per90) * 0.25)) AS DECIMAL(5,2)) AS Wdefensive_actions,
        A.heading_accuracy,
        A.sprints_per90
    FROM silver.fact_outfield_player_stats AS A
    INNER JOIN silver.dim_player_info AS B
        ON A.player_key = B.player_key
    WHERE B.contracted = 1 AND (
        B.position = 'Right Wingback' OR B.position = 'Left Wingback'
        OR B.position = 'Right Defender' OR B.position = 'Left Defender'
        OR B.position = 'Right Attacking Midfielder' OR B.position = 'Left Attacking Midfielder'
        )
        AND A.minutes_played >= 1000
        AND A.dwh_current_validity = 1 AND B.dwh_current_validity = 1
    GROUP BY A.player_key, B.age, B.position, A.minutes_played, A.goals_per90, A.assists_per90, A.xGoals_per90, A.xAssits_per90,
        A.team_goals_scored_per90, A.team_goals_conceded_per90, A.dribbles_made_per90, A.chances_created_per90,
        A.open_play_key_passes_per90, A.progressive_passes_per90, A.progressive_passes_per90, A.passes_attempted_per90,
        A.tackles_attempted_per90, A.tackle_accuracy, A.key_tackles_per90, A.pressures_attempted_per90, A.sprints_per90,
        A.pressures_attempted_per90, A.pressures_completed_per90, A.possession_won_per90, A.possession_lost_per90, A.interceptions_per90, A.clearances_per90, 
        A.blocks_per90, A.shots_blocked_per90, A.heading_accuracy, A.open_play_cross_accuracy, A.open_play_crosses_attempted_per90;
GO

--Business Question: "Fiorentina Attacking Midfielder Player Reports"
CREATE VIEW gold.vw_fiorentina_attacking_midfielder_reports AS
    SELECT 
        A.player_key,
        B.age,
        B.position,
        A.minutes_played,
        (A.team_goals_scored_per90 - A.team_goals_conceded_per90) AS tG_differential_per90,
        (A.goals_per90 + A.assists_per90) AS GA_per90,
        (A.xGoals_per90 + A.xAssits_per90) AS xGA_per90,
        CAST((A.xAssits_per90 * A.chances_created_per90) AS DECIMAL(5,2)) AS creative_index,
        A.open_play_key_passes_per90,
        CAST(((A.dribbles_made_per90 * 0.65) + (A.passes_attempted_per90 * 0.35)) AS DECIMAL(5,2)) AS progA_score,
        A.open_play_crosses_attempted_per90,
        A.open_play_cross_accuracy,
        A.pressures_attempted_per90,
        CAST(((A.pressures_attempted_per90 - A.pressures_completed_per90)/A.pressures_attempted_per90 * 100) AS DECIMAL(5,2)) AS pressure_accuracy,
        A.possession_won_per90,
        (A.possession_won_per90 - A.possession_lost_per90) AS possession_differential,
        CAST(((A.interceptions_per90 * 0.5) + (A.clearances_per90 * 0.25) + (AVG(A.blocks_per90 + A.shots_blocked_per90) * 0.25)) AS DECIMAL(5,2)) AS Wdefensive_actions,
        A.heading_accuracy,
        A.sprints_per90
    FROM silver.fact_outfield_player_stats AS A
    INNER JOIN silver.dim_player_info AS B
        ON A.player_key = B.player_key
    WHERE B.contracted = 1 AND (
        B.position = 'Right Attacking Midfielder' OR B.position = 'Left Attacking Midfielder'
        OR B.position = 'Central Attacking Midfielder'
        )
        AND A.minutes_played >= 1000
        AND A.dwh_current_validity = 1 AND B.dwh_current_validity = 1
    GROUP BY A.player_key, B.age, B.position, A.minutes_played, A.goals_per90, A.assists_per90, A.xGoals_per90, A.xAssits_per90,
        A.team_goals_scored_per90, A.team_goals_conceded_per90, A.dribbles_made_per90, A.chances_created_per90,
        A.open_play_key_passes_per90, A.progressive_passes_per90, A.progressive_passes_per90, A.passes_attempted_per90,
        A.pressures_attempted_per90, A.sprints_per90, A.pressures_attempted_per90, A.pressures_completed_per90, A.possession_won_per90, 
        A.possession_lost_per90, A.interceptions_per90, A.clearances_per90, 
        A.blocks_per90, A.shots_blocked_per90, A.heading_accuracy, A.open_play_cross_accuracy, A.open_play_crosses_attempted_per90;
GO

--Business Question: "Fiorentina Midfielder Player Reports"
CREATE VIEW gold.vw_fiorentina_midfielder_reports AS 
    SELECT 
        A.player_key,
        B.age,
        A.minutes_played,
        (A.team_goals_scored_per90 - A.team_goals_conceded_per90) AS tG_differential_per90,
        (A.goals_per90 + A.assists_per90) AS GA_per90,
        (A.xGoals_per90 + A.xAssits_per90) AS xGA_per90,
        CAST((A.xAssits_per90 * A.chances_created_per90) AS DECIMAL(5,2)) AS creative_index,
        A.open_play_key_passes_per90,
        CAST(((A.dribbles_made_per90 * 0.65) + (A.passes_attempted_per90 * 0.35)) AS DECIMAL(5,2)) AS progA_score,
        A.tackles_attempted_per90,
        A.tackle_accuracy,
        A.key_tackles_per90,
        A.pressures_attempted_per90,
        CAST(((A.pressures_attempted_per90 - A.pressures_completed_per90)/A.pressures_attempted_per90 * 100) AS DECIMAL(5,2)) AS pressure_accuracy,
        A.possession_won_per90,
        (A.possession_won_per90 - A.possession_lost_per90) AS possession_differential,
        CAST(((A.interceptions_per90 * 0.5) + (A.clearances_per90 * 0.25) + (AVG(A.blocks_per90 + A.shots_blocked_per90) * 0.25)) AS DECIMAL(5,2)) AS Wdefensive_actions,
        A.heading_accuracy,
        A.sprints_per90
    FROM silver.fact_outfield_player_stats AS A
    INNER JOIN silver.dim_player_info AS B
        ON A.player_key = B.player_key
    WHERE B.contracted = 1 AND B.position = 'Central Midfielder'
        AND A.minutes_played >= 1000
        AND A.dwh_current_validity = 1 AND B.dwh_current_validity = 1
    GROUP BY A.player_key, B.age, A.minutes_played, A.goals_per90, A.assists_per90, A.xGoals_per90, A.xAssits_per90,
        A.team_goals_scored_per90, A.team_goals_conceded_per90, A.dribbles_made_per90, A.chances_created_per90,
        A.open_play_key_passes_per90, A.progressive_passes_per90, A.progressive_passes_per90, A.passes_attempted_per90,
        A.pressures_attempted_per90, A.sprints_per90, A.pressures_attempted_per90, A.pressures_completed_per90, A.possession_won_per90, 
        A.possession_lost_per90, A.interceptions_per90, A.clearances_per90, A.tackles_attempted_per90, A.tackle_accuracy, A.key_tackles_per90,
        A.blocks_per90, A.shots_blocked_per90, A.heading_accuracy, A.open_play_cross_accuracy, A.open_play_crosses_attempted_per90;
GO

--Business Question: "Fiorentina Striker Player Reports"
CREATE VIEW gold.vw_fiorentina_striker_reports AS
    SELECT 
        A.player_key,
        B.age,
        A.minutes_played,
        (A.team_goals_scored_per90 - A.team_goals_conceded_per90) AS team_goal_diff_per90,
        A.goals_per90,
        A.assists_per90,
        (A.goals_per90 + A.assists_per90) AS GA_per90,
        A.xGoals_per90,
        A.non_penalty_xGoals_per90,
        A.xAssits_per90,
        (A.xGoals_per90 + A.xAssits_per90) AS xGA_per90,
        (A.goals_per90 - A.xGoals_per90) AS finishing_delta,
        CAST(((A.goals_per90 / A.shots_per90)) AS DECIMAL(5,2)) AS shot_conversion_pct,
        CAST((A.xGoals_per90 / A.shots_per90) AS DECIMAL(5,2)) AS shot_quality_index,
        A.shots_on_target_per90,
        A.shot_accuracy,
        A.headers_attempted_per90,
        A.heading_accuracy,
        A.pressures_attempted_per90,
        CAST(((A.pressures_completed_per90 / A.pressures_attempted_per90)) AS DECIMAL(5,2)) AS pressure_success_pct,
        A.sprints_per90,
        A.possession_won_per90,
        CAST(((A.possession_won_per90 - A.possession_lost_per90)) AS DECIMAL(5,2)) AS possession_differential
    FROM silver.fact_outfield_player_stats AS A
    INNER JOIN silver.dim_player_info AS B
        ON A.player_key = B.player_key
    WHERE 
        B.contracted = 1 AND B.position = 'Striker'
        AND A.minutes_played >= 1000 AND A.dwh_current_validity = 1 
        AND B.dwh_current_validity = 1
    GROUP BY
        A.player_key, B.age, B.position, A.minutes_played, A.team_goals_scored_per90, A.team_goals_conceded_per90,
        A.goals_per90, A.assists_per90, A.xGoals_per90, A.xAssits_per90, A.shots_per90,
        A.shots_on_target_per90, A.shot_accuracy, A.non_penalty_xGoals_per90, A.headers_attempted_per90,
        A.heading_accuracy, A.open_play_key_passes_per90, A.dribbles_made_per90, A.pressures_attempted_per90,
        A.pressures_completed_per90, A.sprints_per90, A.possession_won_per90, A.possession_lost_per90;
GO

-- Business Question: "Strikers with 75% similarity to our best performing Striker"
CREATE VIEW gold.vw_similar_strikers AS
    WITH 
        main_st AS (
            SELECT
            TOP 1
                *
            FROM gold.vw_fiorentina_striker_reports
            WHERE minutes_played > 1500
            ORDER BY goals_per90 DESC
    )
    SELECT DISTINCT
        DENSE_RANK() OVER (ORDER BY 
            A.non_penalty_xGoals_per90 DESC,
            A.goals_per90 DESC,
            A.conversion_rate DESC,
            A.heading_accuracy DESC,
            A.headers_attempted_per90 DESC
        ) AS rank,
        A.player_key,
        C.player_name,
        C.age,
        D.club_name,
        A.minutes_played,
        (A.team_goals_scored_per90 - A.team_goals_conceded_per90) AS team_goal_diff_per90,
        A.goals_per90,
        A.assists_per90,
        (A.goals_per90 + A.assists_per90) AS GA_per90,
        A.xGoals_per90,
        A.non_penalty_xGoals_per90,
        A.xAssits_per90,
        (A.xGoals_per90 + A.xAssits_per90) AS xGA_per90,
        (A.goals_per90 - A.xGoals_per90) AS finishing_delta,
        CAST(((A.goals_per90 / A.shots_per90)) AS DECIMAL(5,2)) AS shot_conversion_pct,
        CAST((A.xGoals_per90 / A.shots_per90) AS DECIMAL(5,2)) AS shot_quality_index,
        A.shots_on_target_per90,
        A.shot_accuracy,
        A.headers_attempted_per90,
        A.heading_accuracy,
        A.pressures_attempted_per90,
        CAST(((A.pressures_completed_per90 / A.pressures_attempted_per90)) AS DECIMAL(5,2)) AS pressure_success_pct,
        A.sprints_per90,
        A.possession_won_per90,
        CAST(((A.possession_won_per90 - A.possession_lost_per90)) AS DECIMAL(5,2)) AS possession_differential,
        E.transfer_value_m
    FROM silver.fact_outfield_player_stats AS A
    INNER JOIN silver.dim_player_info AS C
        ON A.player_key = C.player_key
    INNER JOIN silver.dim_team_info AS D
        ON A.team_key = D.team_key
    INNER JOIN silver.fact_players_value AS E
        ON A.player_key = E.player_key
    CROSS JOIN main_st AS B
    WHERE
        C.contracted = 0
        AND A.minutes_played >= 2000
        AND C.position = 'Striker'
        AND A.non_penalty_xGoals_per90 >= 0.75 * B.non_penalty_xGoals_per90
        AND A.shot_accuracy >= 0.50 * B.shot_accuracy
        AND A.non_penalty_xGoals_per90 >= 0.75 * B.non_penalty_xGoals_per90
        AND A.headers_attempted_per90 >= 0.75 * B.headers_attempted_per90
        AND A.heading_accuracy >= 0.75 * B.heading_accuracy
        AND C.age <= 28
        AND E.transfer_value_m <= 10;
GO