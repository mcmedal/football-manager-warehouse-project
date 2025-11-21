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


-- View: gold.dim_team_info
CREATE VIEW gold.dim_team_info AS
    WITH league_ids AS (
        SELECT
            DISTINCT(league) AS league,
            DENSE_RANK () OVER (ORDER BY league) AS league_id,
            club_name
        FROM silver.fmdata_interested_out_players
        ),
        club_ids AS (
        SELECT
            DISTINCT(club_name) AS club_name,
            DENSE_RANK () OVER (ORDER BY club_name) AS club_id,
            league
        FROM silver.fmdata_interested_out_players
        )
    SELECT
        CONCAT(A.league_id, '.', C.club_id) AS team_key,
        A.league,
        B.club_name
    FROM silver.fmdata_possession_data AS B
    LEFT JOIN league_ids AS A
        ON B.club_name = A.club_name
    INNER JOIN club_ids AS C
        ON B.club_name = C.club_name;
GO

-- View: gold.fact_teams_possession
CREATE VIEW gold.fact_teams_possession AS
    SELECT
        B.team_key,
        CAST(A.average_possession AS INT) AS average_possession
    FROM silver.fmdata_possession_data AS A
    INNER JOIN gold.dim_team_info AS B
        ON A.club_name = B.club_name;
GO

-- View: gold.dim_playstyle
CREATE VIEW gold.dim_playstyle AS
    WITH playstyle AS (
        SELECT
            *,
            ROW_NUMBER() OVER(ORDER BY tactical_style, playing_mentality, preferred_formation, pressing_style, marking_style) AS playstyle_key
        FROM(
            SELECT
                DISTINCT tactical_style, playing_mentality, preferred_formation, pressing_style, marking_style
            FROM silver.fmdata_manager_data) AS styles_of_play
        )
    SELECT
        playstyle_key,
        tactical_style,
        playing_mentality,
        preferred_formation,
        pressing_style,
        marking_style
    FROM playstyle;
GO

-- View: gold.dim_manager_info
CREATE VIEW gold.dim_manager_info AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY A.staff_name) AS manager_key,
        A.staff_id,
        A.staff_name,
        C.team_key AS current_club_key,
        D.team_key AS previous_club_key,
        B.playstyle_key,
        CAST(A.contract_begins AS DATE) AS contract_begins,
        CAST(A.contract_expires AS DATE) AS contract_expires
    FROM silver.fmdata_manager_data AS A
    INNER JOIN gold.dim_playstyle AS B
        ON A.tactical_style = B.tactical_style
    INNER JOIN gold.dim_team_info AS C
        ON A.club_name = C.club_name
    LEFT JOIN gold.dim_team_info AS D
        ON A.previous_club_name = D.club_name
    WHERE A.tactical_style = B.tactical_style
        AND A.playing_mentality = B.playing_mentality
        AND A.preferred_formation = B.preferred_formation
        AND A.pressing_style = B.pressing_style
        AND A.marking_style = B.marking_style;
GO

-- View: gold.dim_player_info
CREATE VIEW gold.dim_player_info AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY player_name) AS player_key,
        *
    FROM (
        SELECT
            0 AS contracted,
            A.player_id,
            A.player_name,
            CAST(A.age AS INT) AS age,
            A.position,
            B.team_key AS team_key
        FROM silver.fmdata_interested_out_players AS A
        INNER JOIN gold.dim_team_info AS B
            ON A.club_name = B.club_name
        WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players) -- If present, ignore our players
    UNION
        SELECT
            1 AS contracted,
            A.player_id,
            A.player_name,
            CAST(A.age AS INT) AS age,
            A.position,
            B.team_key AS team_key
        FROM silver.fmdata_team_players AS A
        INNER JOIN gold.dim_team_info AS B
            ON A.club_name = B.club_name
    UNION
        SELECT
            0 AS contracted,
            A.player_id,
            A.player_name,
            CAST(A.age AS INT) AS age,
            A.position,
            B.team_key AS team_key
        FROM silver.fmdata_interested_gks AS A
        INNER JOIN gold.dim_team_info AS B
            ON A.club_name = B.club_name
        WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks) -- If present, ignore our players
    UNION
        SELECT
            1 AS contracted,
            A.player_id,
            A.player_name,
            CAST(A.age AS INT) AS age,
            A.position,
            B.team_key AS team_key
        FROM silver.fmdata_team_gks AS A
        INNER JOIN gold.dim_team_info AS B
            ON A.club_name = B.club_name
            ) AS player_info;
GO

-- View: gold.fact_outfield_player_stats
CREATE VIEW gold.fact_outfield_player_stats AS
        SELECT
            B.player_key,
            C.team_key,
            B.contracted,
            A.position,
            CAST(A.minutes_played AS INT) AS minutes_played,
            CAST(A.team_goals_scored_per90 AS DECIMAL( 5, 2)) AS team_goals_scored_per90,
            CAST(A.team_goals_conceded_per90 AS DECIMAL( 5, 2)) AS team_goals_conceded_per90,	
            CAST(A.goals AS INT) AS goals,    
            CAST(A.goals_outside_the_box AS INT) AS goals_outside_the_box,
            CAST(A.shots_per90 AS DECIMAL( 5, 2)) AS shots_per90,	
            CAST(A.xGoals_per_shot AS DECIMAL( 5, 2)) AS xGoals_per_shot,
            CAST(A.shot_accuracy AS DECIMAL( 5, 2)) AS shot_accuracy,
            CAST(A.shots_on_target_per90 AS DECIMAL( 5, 2)) AS shots_on_target_per90,
            CAST(A.shots_outside_the_box_per90 AS DECIMAL( 5, 2)) AS shots_outside_the_box_per90,
            CAST(A.goals_per90 AS DECIMAL( 5, 2)) AS goals_per90,
            CAST(A.xGoals_per90 AS DECIMAL( 5, 2)) AS xGoals_per90,
            CAST(A.non_penalty_xGoals_per90 AS DECIMAL( 5, 2)) AS non_penalty_xGoals_per90,
            CAST(A.xGoals_overperformance AS DECIMAL( 5, 2)) AS xGoals_overperformance,
            CAST(A.conversion_rate AS DECIMAL( 5, 2)) AS conversion_rate,
            CAST(A.assists AS INT) AS assists,
            CAST(A.assists_per90 AS DECIMAL( 5, 2)) AS assists_per90,
            CAST(A.passes_attempted_per90 AS DECIMAL( 5, 2)) AS passes_attempted_per90,
            CAST(A.pass_accuracy AS DECIMAL( 5, 2)) AS pass_accuracy,
            CAST(A.xAssits_per90 AS DECIMAL( 5, 2)) AS xAssits_per90,
            CAST(A.open_play_key_passes_per90 AS DECIMAL( 5, 2)) AS open_play_key_passes_per90,
            CAST(A.chances_created_per90 AS DECIMAL( 5, 2)) AS chances_created_per90,
            CAST(A.dribbles_made_per90 AS DECIMAL( 5, 2)) AS dribbles_made_per90,
            CAST(A.progressive_passes_per90 AS DECIMAL( 5, 2)) AS progressive_passes_per90,
            CAST(A.open_play_crosses_attempted_per90 AS DECIMAL( 5, 2)) AS open_play_crosses_attempted_per90,
            CAST(A.open_play_cross_accuracy AS DECIMAL( 5, 2)) AS open_play_cross_accuracy,
            CAST(A.crosses_attempted_per90 AS DECIMAL( 5, 2)) AS crosses_attempted_per90,
            CAST(A.cross_accuracy AS DECIMAL( 5, 2)) AS cross_accuracy,
            CAST(A.tackles_attempted_per90 AS DECIMAL( 5, 2)) AS tackles_attempted_per90,
            CAST(A.tackle_accuracy AS DECIMAL( 5, 2)) AS tackle_accuracy,
            CAST(A.pressures_attempted_per90 AS DECIMAL( 5, 2)) AS pressures_attempted_per90,
            CAST(A.pressures_completed_per90 AS DECIMAL( 5, 2)) AS pressures_completed_per90,
            CAST(A.possession_won_per90 AS DECIMAL( 5, 2)) AS possession_won_per90,
            CAST(A.possession_lost_per90 AS DECIMAL( 5, 2)) AS possession_lost_per90,
            CAST(A.key_tackles_per90 AS DECIMAL( 5, 2)) AS key_tackles_per90,
            CAST(A.interceptions_per90 AS DECIMAL( 5, 2)) AS interceptions_per90,
            CAST(A.clearances_per90 AS DECIMAL( 5, 2)) AS clearances_per90,
            CAST(A.blocks_per90 AS DECIMAL( 5, 2)) AS blocks_per90,
            CAST(A.shots_blocked_per90 AS DECIMAL( 5, 2)) AS shots_blocked_per90,
            CAST(A.headers_attempted_per90 AS DECIMAL( 5, 2)) AS headers_attempted_per90,
            CAST(A.heading_accuracy AS DECIMAL( 5, 2)) AS heading_accuracy,
            CAST(A.key_headers_per90 AS DECIMAL( 5, 2)) AS key_headers_per90,
            CAST(A.sprints_per90 AS DECIMAL( 5, 2)) AS sprints_per90,
            CAST(A.distance_covered_km_per90 AS DECIMAL( 5, 2)) AS distance_covered_km_per90,
            CAST(A.mistakes_leading_to_goals_per90 AS DECIMAL( 5, 2)) AS mistakes_leading_to_goals_per90,
            CAST(A.fouls_made_per90 AS DECIMAL( 5, 2)) AS fouls_made_per90,
            CAST(A.fouls_against_per90 AS DECIMAL( 5, 2)) AS fouls_against_per90,
            CAST(A.yellow_cards_per90 AS DECIMAL( 5, 2)) AS yellow_cards_per90,
            CAST(A.red_cards_per90 AS DECIMAL( 5, 2)) AS red_cards_per90
        FROM silver.fmdata_interested_out_players AS A
        INNER JOIN gold.dim_player_info AS B
            ON A.player_id = B.player_id
        INNER JOIN gold.dim_team_info AS C
            ON A.club_name = C.club_name
        WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players) -- If present, ignore our players
    UNION
        SELECT
            B.player_key,
            C.team_key,
            B.contracted,
            A.position,
            CAST(A.minutes_played AS INT) AS minutes_played,
            CAST(A.team_goals_scored_per90 AS DECIMAL( 5, 2)) AS team_goals_scored_per90,
            CAST(A.team_goals_conceded_per90 AS DECIMAL( 5, 2)) AS team_goals_conceded_per90,	
            CAST(A.goals AS INT) AS goals,    
            CAST(A.goals_outside_the_box AS INT) AS goals_outside_the_box,
            CAST(A.shots_per90 AS DECIMAL( 5, 2)) AS shots_per90,	
            CAST(A.xGoals_per_shot AS DECIMAL( 5, 2)) AS xGoals_per_shot,
            CAST(A.shot_accuracy AS DECIMAL( 5, 2)) AS shot_accuracy,
            CAST(A.shots_on_target_per90 AS DECIMAL( 5, 2)) AS shots_on_target_per90,
            CAST(A.shots_outside_the_box_per90 AS DECIMAL( 5, 2)) AS shots_outside_the_box_per90,
            CAST(A.goals_per90 AS DECIMAL( 5, 2)) AS goals_per90,
            CAST(A.xGoals_per90 AS DECIMAL( 5, 2)) AS xGoals_per90,
            CAST(A.non_penalty_xGoals_per90 AS DECIMAL( 5, 2)) AS non_penalty_xGoals_per90,
            CAST(A.xGoals_overperformance AS DECIMAL( 5, 2)) AS xGoals_overperformance,
            CAST(A.conversion_rate AS DECIMAL( 5, 2)) AS conversion_rate,
            CAST(A.assists AS INT) AS assists,
            CAST(A.assists_per90 AS DECIMAL( 5, 2)) AS assists_per90,
            CAST(A.passes_attempted_per90 AS DECIMAL( 5, 2)) AS passes_attempted_per90,
            CAST(A.pass_accuracy AS DECIMAL( 5, 2)) AS pass_accuracy,
            CAST(A.xAssits_per90 AS DECIMAL( 5, 2)) AS xAssits_per90,
            CAST(A.open_play_key_passes_per90 AS DECIMAL( 5, 2)) AS open_play_key_passes_per90,
            CAST(A.chances_created_per90 AS DECIMAL( 5, 2)) AS chances_created_per90,
            CAST(A.dribbles_made_per90 AS DECIMAL( 5, 2)) AS dribbles_made_per90,
            CAST(A.progressive_passes_per90 AS DECIMAL( 5, 2)) AS progressive_passes_per90,
            CAST(A.open_play_crosses_attempted_per90 AS DECIMAL( 5, 2)) AS open_play_crosses_attempted_per90,
            CAST(A.open_play_cross_accuracy AS DECIMAL( 5, 2)) AS open_play_cross_accuracy,
            CAST(A.crosses_attempted_per90 AS DECIMAL( 5, 2)) AS crosses_attempted_per90,
            CAST(A.cross_accuracy AS DECIMAL( 5, 2)) AS cross_accuracy,
            CAST(A.tackles_attempted_per90 AS DECIMAL( 5, 2)) AS tackles_attempted_per90,
            CAST(A.tackle_accuracy AS DECIMAL( 5, 2)) AS tackle_accuracy,
            CAST(A.pressures_attempted_per90 AS DECIMAL( 5, 2)) AS pressures_attempted_per90,
            CAST(A.pressures_completed_per90 AS DECIMAL( 5, 2)) AS pressures_completed_per90,
            CAST(A.possession_won_per90 AS DECIMAL( 5, 2)) AS possession_won_per90,
            CAST(A.possession_lost_per90 AS DECIMAL( 5, 2)) AS possession_lost_per90,
            CAST(A.key_tackles_per90 AS DECIMAL( 5, 2)) AS key_tackles_per90,
            CAST(A.interceptions_per90 AS DECIMAL( 5, 2)) AS interceptions_per90,
            CAST(A.clearances_per90 AS DECIMAL( 5, 2)) AS clearances_per90,
            CAST(A.blocks_per90 AS DECIMAL( 5, 2)) AS blocks_per90,
            CAST(A.shots_blocked_per90 AS DECIMAL( 5, 2)) AS shots_blocked_per90,
            CAST(A.headers_attempted_per90 AS DECIMAL( 5, 2)) AS headers_attempted_per90,
            CAST(A.heading_accuracy AS DECIMAL( 5, 2)) AS heading_accuracy,
            CAST(A.key_headers_per90 AS DECIMAL( 5, 2)) AS key_headers_per90,
            CAST(A.sprints_per90 AS DECIMAL( 5, 2)) AS sprints_per90,
            CAST(A.distance_covered_km_per90 AS DECIMAL( 5, 2)) AS distance_covered_km_per90,
            CAST(A.mistakes_leading_to_goals_per90 AS DECIMAL( 5, 2)) AS mistakes_leading_to_goals_per90,
            CAST(A.fouls_made_per90 AS DECIMAL( 5, 2)) AS fouls_made_per90,
            CAST(A.fouls_against_per90 AS DECIMAL( 5, 2)) AS fouls_against_per90,
            CAST(A.yellow_cards_per90 AS DECIMAL( 5, 2)) AS yellow_cards_per90,
            CAST(A.red_cards_per90 AS DECIMAL( 5, 2)) AS red_cards_per90
        FROM silver.fmdata_team_players AS A
        INNER JOIN gold.dim_player_info AS B
            ON A.player_id = B.player_id
        INNER JOIN gold.dim_team_info AS C
            ON A.club_name = C.club_name;
GO

-- View: gold.fact_gk_stats
CREATE VIEW gold.fact_gk_stats AS
        SELECT
            B.player_key,
            C.team_key,
            B.contracted,
            CAST(A.minutes_played AS INT) AS minutes_played,
			CAST(A.team_goals_scored_per90 AS DECIMAL( 5, 2)) AS team_goals_scored_per90,
			CAST(A.team_goals_conceded_per90 AS DECIMAL( 5, 2)) AS team_goals_conceded_per90,	
			CAST(A.goals_conceded_per90 AS DECIMAL( 5, 2)) AS goals_conceded_per90,	
			CAST(A.saves_made_per90 AS DECIMAL( 5, 2)) AS saves_made_per90,
			CAST(A.xGoals_prevented_per90 AS DECIMAL( 5, 2)) AS xGoals_prevented_per90,
			CAST(A.xSave_rate AS DECIMAL( 5, 2)) AS xSave_rate,
			CAST(A.saves_tipped_per90 AS DECIMAL( 5, 2)) AS saves_tipped_per90,
			CAST(A.saves_parried_per90 AS DECIMAL( 5, 2)) AS saves_parried_per90,
			CAST(A.saves_held_per90 AS DECIMAL( 5, 2)) AS saves_held_per90,
			CAST(A.saves_percentage AS DECIMAL( 5, 2)) AS saves_percentage,
			CAST(A.passes_attempted_per90 AS DECIMAL( 5, 2)) AS passes_attempted_per90,
			CAST(A.pass_accuracy AS DECIMAL( 5, 2)) AS pass_accuracy,
			CAST(A.possession_won_per90 AS DECIMAL( 5, 2)) AS possession_won_per90,
			CAST(A.possession_lost_per90 AS DECIMAL( 5, 2)) AS possession_lost_per90,
			CAST(A.interceptions_per90 AS DECIMAL( 5, 2)) AS interceptions_per90,
			CAST(A.clearances_per90 AS DECIMAL( 5, 2)) AS clearances_per90,
			CAST(A.penalties_faced_per90 AS DECIMAL( 5, 2)) AS penalties_faced_per90,
			CAST(A.penalties_save_percentage AS DECIMAL( 5, 2)) AS penalties_save_percentage,
			CAST(A.distance_covered_km_per90 AS DECIMAL( 5, 2)) AS distance_covered_km_per90,
			CAST(A.mistakes_leading_to_goals_per90 AS DECIMAL( 5, 2)) AS mistakes_leading_to_goals_per90,
			CAST(A.fouls_against_per90 AS DECIMAL( 5, 2)) AS fouls_against_per90
        FROM silver.fmdata_interested_gks AS A
        INNER JOIN gold.dim_player_info AS B
            ON A.player_id = B.player_id
        INNER JOIN gold.dim_team_info AS C
            ON A.club_name = C.club_name
        WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks) -- If present, ignore our players
    UNION
        SELECT
            B.player_key,
            C.team_key,
            B.contracted,
            CAST(A.minutes_played AS INT) AS minutes_played,
			CAST(A.team_goals_scored_per90 AS DECIMAL( 5, 2)) AS team_goals_scored_per90,
			CAST(A.team_goals_conceded_per90 AS DECIMAL( 5, 2)) AS team_goals_conceded_per90,	
			CAST(A.goals_conceded_per90 AS DECIMAL( 5, 2)) AS goals_conceded_per90,	
			CAST(A.saves_made_per90 AS DECIMAL( 5, 2)) AS saves_made_per90,
			CAST(A.xGoals_prevented_per90 AS DECIMAL( 5, 2)) AS xGoals_prevented_per90,
			CAST(A.xSave_rate AS DECIMAL( 5, 2)) AS xSave_rate,
			CAST(A.saves_tipped_per90 AS DECIMAL( 5, 2)) AS saves_tipped_per90,
			CAST(A.saves_parried_per90 AS DECIMAL( 5, 2)) AS saves_parried_per90,
			CAST(A.saves_held_per90 AS DECIMAL( 5, 2)) AS saves_held_per90,
			CAST(A.saves_percentage AS DECIMAL( 5, 2)) AS saves_percentage,
			CAST(A.passes_attempted_per90 AS DECIMAL( 5, 2)) AS passes_attempted_per90,
			CAST(A.pass_accuracy AS DECIMAL( 5, 2)) AS pass_accuracy,
			CAST(A.possession_won_per90 AS DECIMAL( 5, 2)) AS possession_won_per90,
			CAST(A.possession_lost_per90 AS DECIMAL( 5, 2)) AS possession_lost_per90,
			CAST(A.interceptions_per90 AS DECIMAL( 5, 2)) AS interceptions_per90,
			CAST(A.clearances_per90 AS DECIMAL( 5, 2)) AS clearances_per90,
			CAST(A.penalties_faced_per90 AS DECIMAL( 5, 2)) AS penalties_faced_per90,
			CAST(A.penalties_save_percentage AS DECIMAL( 5, 2)) AS penalties_save_percentage,
			CAST(A.distance_covered_km_per90 AS DECIMAL( 5, 2)) AS distance_covered_km_per90,
			CAST(A.mistakes_leading_to_goals_per90 AS DECIMAL( 5, 2)) AS mistakes_leading_to_goals_per90,
			CAST(A.fouls_against_per90 AS DECIMAL( 5, 2)) AS fouls_against_per90
        FROM silver.fmdata_team_gks AS A
        INNER JOIN gold.dim_player_info AS B
            ON A.player_id = B.player_id
        INNER JOIN gold.dim_team_info AS C
            ON A.club_name = C.club_name;
GO

-- View: gold.fact_players_value
CREATE VIEW gold.fact_players_value AS
        SELECT
            A.player_key,
            A.team_key,
            CAST(B.wage_per_week_k AS DECIMAL( 10, 2)) AS wage_per_week_k,
            CAST(B.transfer_value_m AS DECIMAL( 10, 2)) AS transfer_value_m
        FROM gold.dim_player_info AS A
        INNER JOIN silver.fmdata_interested_out_players AS B
            ON A.player_id = B.player_id
    UNION
        SELECT
            A.player_key,
            A.team_key,
            CAST(B.wage_per_week_k AS DECIMAL( 10, 2)) AS wage_per_week_k,
            CAST(B.transfer_value_m AS DECIMAL( 10, 2)) AS transfer_value_m
        FROM gold.dim_player_info AS A
        INNER JOIN silver.fmdata_interested_gks AS B
            ON A.player_id = B.player_id
    UNION
        SELECT
            A.player_key,
            A.team_key,
            CAST(B.wage_per_week_k AS DECIMAL( 10, 2)) AS wage_per_week_k,
            CAST(B.transfer_value_m AS DECIMAL( 10, 2)) AS transfer_value_m
        FROM gold.dim_player_info AS A
        INNER JOIN silver.fmdata_team_players AS B
            ON A.player_id = B.player_id
    UNION
        SELECT
            A.player_key,
            A.team_key,
            CAST(B.wage_per_week_k AS DECIMAL( 10, 2)) AS wage_per_week_k,
            CAST(B.transfer_value_m AS DECIMAL( 10, 2)) AS transfer_value_m
        FROM gold.dim_player_info AS A
        INNER JOIN silver.fmdata_team_gks AS B
            ON A.player_id = B.player_id;
GO

-- View: gold.fact_team_statistics
CREATE VIEW gold.fact_team_statistics AS
    SELECT
        A.*,
        B.position,
        SUM(B.minutes_played) AS total_minutes_played,
        SUM(B.team_goals_scored_per90) AS total_team_goals_scored_per90,
        SUM(B.team_goals_conceded_per90) AS total_team_goals_conceded_per90,
        SUM(B.goals) AS total_goals,
        SUM(B.goals_outside_the_box) AS total_goals_outside_the_box,
        SUM(B.shots_per90) AS total_shots_per90,
        SUM(B.xGoals_per_shot) AS total_xGoals_per_shot,
        AVG(B.shot_accuracy) AS average_shot_accuracy,
        SUM(B.shots_on_target_per90) AS total_shots_on_target_per90,
        SUM(B.shots_outside_the_box_per90) AS total_shots_outside_the_box_per90,
        SUM(B.goals_per90) AS total_goals_per90,
        SUM(B.xGoals_per90) AS total_xGoals_per90,
        SUM(B.non_penalty_xGoals_per90) AS total_non_penalty_xGoals_per90,
        SUM(B.xGoals_overperformance) AS total_xGoals_overperformance,
        AVG(B.conversion_rate) AS average_conversion_rate,
        SUM(B.assists) AS total_assists,
        SUM(B.assists_per90) AS total_assists_per90,
        SUM(B.passes_attempted_per90) AS total_passes_attempted_per90,
        AVG(B.pass_accuracy) AS average_pass_accuracy,
        SUM(B.xAssits_per90) AS total_xAssits_per90,
        SUM(B.open_play_key_passes_per90) AS total_open_play_key_passes_per90,
        SUM(B.chances_created_per90) AS total_chances_created_per90,
        SUM(B.dribbles_made_per90) AS total_dribbles_made_per90,
        SUM(B.progressive_passes_per90) AS total_progressive_passes_per90,
        SUM(B.open_play_crosses_attempted_per90) AS total_open_play_crosses_attempted_per90,
        AVG(B.open_play_cross_accuracy) AS average_open_play_cross_accuracy,
        SUM(B.crosses_attempted_per90) AS total_crosses_attempted_per90,
        AVG(B.cross_accuracy) AS average_cross_accuracy,
        SUM(B.tackles_attempted_per90) AS total_tackles_attempted_per90,
        AVG(B.tackle_accuracy) AS average_tackle_accuracy,
        SUM(B.pressures_attempted_per90) AS total_pressures_attempted_per90,
        SUM(B.pressures_completed_per90) AS total_pressures_completed_per90,
        SUM(B.possession_won_per90) AS total_possession_won_per90,
        SUM(B.possession_lost_per90) AS total_possession_lost_per90,
        SUM(B.key_tackles_per90) AS total_key_tackles_per90,
        SUM(B.interceptions_per90) AS total_interceptions_per90,
        SUM(B.clearances_per90) AS total_clearances_per90,
        SUM(B.blocks_per90) AS total_blocks_per90,
        SUM(B.shots_blocked_per90) AS total_shots_blocked_per90,
        SUM(B.headers_attempted_per90) AS total_headers_attempted_per90,
        AVG(B.heading_accuracy) AS average_heading_accuracy,
        SUM(B.key_headers_per90) AS total_key_headers_per90,
        SUM(B.sprints_per90) AS total_sprints_per90,
        SUM(B.distance_covered_km_per90) AS total_distance_covered_km_per90,
        SUM(B.mistakes_leading_to_goals_per90) AS total_mistakes_leading_to_goals_per90,
        SUM(B.fouls_made_per90) AS total_fouls_made_per90,
        SUM(B.fouls_against_per90) AS total_fouls_against_per90,
        SUM(B.yellow_cards_per90) AS total_yellow_cards_per90,
        SUM(B.red_cards_per90) AS total_red_cards_per90
    FROM gold.dim_team_info AS A
    INNER JOIN gold.fact_outfield_player_stats AS B
        ON A.team_key = B.team_key
    GROUP BY A.team_key, A.league, A.club_name, B.position;
GO

-- View: gold.fact_team_gks_statistics
CREATE VIEW gold.fact_team_gks_statistics AS
    SELECT
        A.*,
        SUM(B.minutes_played) AS total_minutes_played,
        SUM(B.team_goals_scored_per90) AS total_team_goals_scored_per90,
        SUM(B.team_goals_conceded_per90) AS total_team_goals_conceded_per90,
        SUM(B.goals_conceded_per90) AS total_goals_conceded_per90,
        SUM(B.saves_made_per90) AS total_saves_made_per90,
        SUM(B.xGoals_prevented_per90) AS total_xGoals_prevented_per90,
        AVG(B.xSave_rate) AS average_xSave_rate,
        SUM(B.saves_tipped_per90) AS total_saves_tipped_per90,
        SUM(B.saves_parried_per90) AS total_saves_parried_per90,
        SUM(B.saves_held_per90) AS total_saves_held_per90,
        AVG(B.saves_percentage) AS average_saves_percentage,
        SUM(B.passes_attempted_per90) AS total_passes_attempted_per90,
        AVG(B.pass_accuracy) AS average_pass_accuracy,
        SUM(B.possession_won_per90) AS total_possession_won_per90,
        SUM(B.possession_lost_per90) AS total_possession_lost_per90,
        SUM(B.interceptions_per90) AS total_interceptions_per90,
        SUM(B.clearances_per90) AS total_clearances_per90,
        SUM(B.penalties_faced_per90) AS total_penalties_faced_per90,
        AVG(B.penalties_save_percentage) AS average_penalties_save_percentage,
        SUM(B.distance_covered_km_per90) AS total_distance_covered_km_per90,
        SUM(B.mistakes_leading_to_goals_per90) AS total_mistakes_leading_to_goals_per90,
        SUM(B.fouls_against_per90) AS total_fouls_against_per90
    FROM gold.dim_team_info AS A
    INNER JOIN gold.fact_gk_stats AS B
        ON A.team_key = B.team_key
    GROUP BY A.team_key, A.league, A.club_name;
GO