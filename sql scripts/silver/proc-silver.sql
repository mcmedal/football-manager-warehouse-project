/*
======================================================================
Creates a Stored Procedure: Load Silver Layer (Bronze Layer -> Silver Layer)
======================================================================
   This script is designed to create a stored procedure that loads tables
  from the bronze layer  into the silver schema. This procedure:
    - truncates the bronze tables before loading the data.
    - uses the 'INSERT INTO' to load data into the tables.

  This Stored Procedure does not require any parameters.
  Example: EXECUTE silver.load_silver;
======================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '====================';
	PRINT 'LOADING SILVER LAYER';
	PRINT '====================';

	--- FMDATA_TEAM_PLAYERS
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_team_players';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>IDENTIFYING OLD RECORDS: silver.fmdata_team_players';
        WITH cleaned_bronze AS (
            SELECT 
                CAST(player_id AS VARCHAR) AS player_id,
                REPLACE(player_name, ' - Pick Player', '') AS player_name,
                TRIM(club_name) AS club_name,
                UPPER(league) AS league,
                CASE 
                    WHEN position  = 'AM (C)' THEN 'Central Attacking Midfielder'
                    WHEN position  = 'AM (L)' THEN 'Left Attacking Midfielder'
                    WHEN position  = 'AM (R)' THEN 'Right Attacking Midfielder'
                    WHEN position  = 'D (C)' THEN 'Central Defender'
                    WHEN position  = 'D (L)' THEN 'Left Defender'
                    WHEN position  = 'D (R)' THEN 'Right Defender'
                    WHEN position  = 'DM' THEN 'Defensive Midfielder'
                    WHEN position  = 'M (C)' THEN 'Central Midfielder'
                    WHEN position  = 'M (L)' THEN 'Left Midfielder'
                    WHEN position  = 'M (R)' THEN 'Right Midfielder'
                    WHEN position  = 'ST (C)' THEN 'Striker'
                    WHEN position  = 'WB (L)' THEN 'Left Wingback'
                    WHEN position  = 'WB (R)' THEN 'Right Wingback'
                END AS position,
                CAST(age AS INT) AS age,
                CASE 
                    WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
                    WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
                END AS minutes_played,
                CASE 
                    WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
                    WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
                END AS team_goals_scored_per90,
                CASE 
                    WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
                    WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
                END AS team_goals_conceded_per90,
                CASE 
                    WHEN goals  = '-' THEN CAST(REPLACE(goals, '-', '0') AS INT)
                    ELSE CAST(goals AS INT)
                END AS goals,
                CASE 
                    WHEN goals_outside_the_box  = '-' THEN CAST(REPLACE(goals_outside_the_box, '-', '0') AS INT)
                    ELSE CAST(goals_outside_the_box AS INT)
                END AS goals_outside_the_box,
                CASE 
                    WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS FLOAT)
                    ELSE CAST(shots_per90 AS FLOAT)
                END AS shots_per90,
                CASE 
                    WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS FLOAT)
                    ELSE CAST(xGoals_per_shot AS FLOAT)
                END AS xGoals_per_shot,
                CASE 
                    WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(shot_accuracy, 1)  = '%' THEN (CAST(REPLACE(shot_accuracy, '%', '') AS FLOAT)/100)
                END AS shot_accuracy,
                CASE
                    WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS FLOAT)
                    ELSE CAST(shots_on_target_per90 AS FLOAT)
                END AS shots_on_target_per90,
                CASE
                    WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS FLOAT)
                    ELSE CAST(shots_outside_the_box_per90 AS FLOAT)
                END AS shots_outside_the_box_per90,
                CASE
                    WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS FLOAT)
                    ELSE CAST(goals_per90 AS FLOAT)
                END AS goals_per90,
                CASE
                    WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS FLOAT)
                    ELSE CAST(xGoals_per90 AS FLOAT)
                END AS xGoals_per90,
                CASE
                    WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS FLOAT)
                    ELSE CAST(non_penalty_xGoals_per90 AS FLOAT)
                END AS non_penalty_xGoals_per90,
                CASE
                    WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS FLOAT)
                    ELSE CAST(xGoals_overperformance AS FLOAT)
                END AS xGoals_overperformance,
                CASE 
                    WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
                    WHEN RIGHT(conversion_rate, 1)  = '%' THEN (CAST(REPLACE(conversion_rate, '%', '') AS FLOAT)/100)
                END AS conversion_rate,
                CASE 
                    WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
                    ELSE CAST(assists AS INT)
                END AS assists,
                CASE
                    WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS FLOAT)
                    ELSE CAST(assists_per90 AS FLOAT)
                END AS assists_per90,
                CASE
                    WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(passes_attempted_per90 AS FLOAT)
                END AS passes_attempted_per90,
                CASE 
                    WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
                END AS pass_accuracy,
                CASE
                    WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS FLOAT)
                    ELSE CAST(xAssits_per90 AS FLOAT)
                END AS xAssits_per90,
                CASE
                    WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS FLOAT)
                    ELSE CAST(open_play_key_passes_per90 AS FLOAT)
                END AS open_play_key_passes_per90,
                CASE
                    WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS FLOAT)
                    ELSE CAST(chances_created_per90 AS FLOAT)
                END AS chances_created_per90,
                CASE
                    WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS FLOAT)
                    ELSE CAST(dribbles_made_per90 AS FLOAT)
                END AS dribbles_made_per90,
                CASE
                    WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS FLOAT)
                    ELSE CAST(progressive_passes_per90 AS FLOAT)
                END AS progressive_passes_per90,
                CASE
                    WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(open_play_crosses_attempted_per90 AS FLOAT)
                END AS open_play_crosses_attempted_per90,
                CASE 
                    WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(open_play_cross_accuracy, '%', '') AS FLOAT)/100)
                END AS open_play_cross_accuracy,
                CASE
                    WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(crosses_attempted_per90 AS FLOAT)
                END AS crosses_attempted_per90,
                CASE 
                    WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(cross_accuracy, '%', '') AS FLOAT)/100)
                END AS cross_accuracy,
                CASE
                    WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
                    ELSE CAST(tackles_attempted AS FLOAT)
                END AS tackles_attempted,
                CASE 
                    WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN (CAST(REPLACE(tackle_accuracy, '%', '') AS FLOAT)/100)
                END AS tackle_accuracy,
                CASE
                    WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(pressures_attempted_per90 AS FLOAT)
                END AS pressures_attempted_per90,
                CASE
                    WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS FLOAT)
                    ELSE CAST(pressures_completed_per90 AS FLOAT)
                END AS pressures_completed_per90,
                CASE
                    WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
                    ELSE CAST(possession_won_per90 AS FLOAT)
                END AS possession_won_per90,
                CASE
                    WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
                    ELSE CAST(possession_lost_per90 AS FLOAT)
                END AS possession_lost_per90,
                CASE
                    WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS FLOAT)
                    ELSE CAST(key_tackles_per90 AS FLOAT)
                END AS key_tackles_per90,
                CASE
                    WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
                    ELSE CAST(interceptions_per90 AS FLOAT)
                END AS interceptions_per90,
                CASE
                    WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
                    ELSE CAST(clearances_per90 AS FLOAT)
                END AS clearances_per90,
                CASE
                    WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS FLOAT)
                    ELSE CAST(blocks_per90 AS FLOAT)
                END AS blocks_per90,
                CASE
                    WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS FLOAT)
                    ELSE CAST(shots_blocked_per90 AS FLOAT)
                END AS shots_blocked_per90,
                CASE
                    WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(headers_attempted_per90 AS FLOAT)
                END AS headers_attempted_per90,
                CASE 
                    WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(heading_accuracy, 1)  = '%' THEN (CAST(REPLACE(heading_accuracy, '%', '') AS FLOAT)/100)
                END AS heading_accuracy,
                CASE
                    WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS FLOAT)
                    ELSE CAST(key_headers_per90 AS FLOAT)
                END AS key_headers_per90,
                CASE
                    WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS FLOAT)
                    ELSE CAST(sprints_per90 AS FLOAT)
                END AS sprints_per90,
                CASE
                    WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
                    ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
                END AS distance_covered_km_per90,
                CASE
                    WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
                    ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
                END AS mistakes_leading_to_goals,
                CASE
                    WHEN fouls_made = '-' THEN CAST(REPLACE(fouls_made, '-', '0') AS INT)
                    ELSE CAST(fouls_made AS DECIMAL(4,2))
                END AS fouls_made,
                CASE
                    WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
                    ELSE CAST(fouls_against AS DECIMAL(4,2))
                END AS fouls_against,
                CASE
                    WHEN yellow_cards = '-' THEN CAST(REPLACE(yellow_cards, '-', '0') AS INT)
                    ELSE CAST(yellow_cards AS DECIMAL(4,2))
                END AS yellow_cards,
                CASE
                    WHEN red_cards = '-' THEN CAST(REPLACE(red_cards, '-', '0') AS INT)
                    ELSE CAST(red_cards AS DECIMAL(4,2))
                END AS red_cards,
                CASE
                    WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
                    ELSE CAST(
                        (CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
                        AS DECIMAL(10, 1)
                    )
                END AS wage_per_week_k,
                CASE
                    WHEN transfer_value = 'Not for Sale' THEN NULL
                    WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
                        REPLACE(
                            CAST(
                                REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
                                (CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
                                LEN(transfer_value)), '£', '') 
                            AS FLOAT) * 0.001,
                        ' ', '')
                    WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
                        REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
                    WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
                        REPLACE(
                            CAST(
                                REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
                                (CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
                                LEN(transfer_value)), '£', '') 
                            AS FLOAT), 
                        ' ', '')
                    WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
                        REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
                    WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
                END AS transfer_value_m,
                GETDATE() AS dwh_create_date,
                '9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
                1 AS dwh_current_validity
            FROM bronze.fmdata_team_players
            WHERE player_name LIKE '% - Pick Player'
        )

        UPDATE S
        SET 
            dwh_cd_valid_till = GETDATE(),
            dwh_current_validity = 0
        FROM silver.fmdata_team_players AS S
        JOIN cleaned_bronze AS B
            ON S.player_id = B.player_id
        WHERE S.player_id = B.player_id
            AND S.age != B.age
            AND S.club_name != B.club_name
            AND S.position != B.position
            AND S.dwh_current_validity = 1

		PRINT '>>> INSERTING NEW DATA INTO: silver.fmdata_team_players';
        WITH cleaned_bronze AS (
            SELECT 
                CAST(player_id AS VARCHAR) AS player_id,
                REPLACE(player_name, ' - Pick Player', '') AS player_name,
                TRIM(club_name) AS club_name,
                UPPER(league) AS league,
                CASE 
                    WHEN position  = 'AM (C)' THEN 'Central Attacking Midfielder'
                    WHEN position  = 'AM (L)' THEN 'Left Attacking Midfielder'
                    WHEN position  = 'AM (R)' THEN 'Right Attacking Midfielder'
                    WHEN position  = 'D (C)' THEN 'Central Defender'
                    WHEN position  = 'D (L)' THEN 'Left Defender'
                    WHEN position  = 'D (R)' THEN 'Right Defender'
                    WHEN position  = 'DM' THEN 'Defensive Midfielder'
                    WHEN position  = 'M (C)' THEN 'Central Midfielder'
                    WHEN position  = 'M (L)' THEN 'Left Midfielder'
                    WHEN position  = 'M (R)' THEN 'Right Midfielder'
                    WHEN position  = 'ST (C)' THEN 'Striker'
                    WHEN position  = 'WB (L)' THEN 'Left Wingback'
                    WHEN position  = 'WB (R)' THEN 'Right Wingback'
                END AS position,
                CAST(age AS INT) AS age,
                CASE 
                    WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
                    WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
                END AS minutes_played,
                CASE 
                    WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
                    WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
                END AS team_goals_scored_per90,
                CASE 
                    WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
                    WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
                END AS team_goals_conceded_per90,
                CASE 
                    WHEN goals  = '-' THEN CAST(REPLACE(goals, '-', '0') AS INT)
                    ELSE CAST(goals AS INT)
                END AS goals,
                CASE 
                    WHEN goals_outside_the_box  = '-' THEN CAST(REPLACE(goals_outside_the_box, '-', '0') AS INT)
                    ELSE CAST(goals_outside_the_box AS INT)
                END AS goals_outside_the_box,
                CASE 
                    WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS FLOAT)
                    ELSE CAST(shots_per90 AS FLOAT)
                END AS shots_per90,
                CASE 
                    WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS FLOAT)
                    ELSE CAST(xGoals_per_shot AS FLOAT)
                END AS xGoals_per_shot,
                CASE 
                    WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(shot_accuracy, 1)  = '%' THEN (CAST(REPLACE(shot_accuracy, '%', '') AS FLOAT)/100)
                END AS shot_accuracy,
                CASE
                    WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS FLOAT)
                    ELSE CAST(shots_on_target_per90 AS FLOAT)
                END AS shots_on_target_per90,
                CASE
                    WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS FLOAT)
                    ELSE CAST(shots_outside_the_box_per90 AS FLOAT)
                END AS shots_outside_the_box_per90,
                CASE
                    WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS FLOAT)
                    ELSE CAST(goals_per90 AS FLOAT)
                END AS goals_per90,
                CASE
                    WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS FLOAT)
                    ELSE CAST(xGoals_per90 AS FLOAT)
                END AS xGoals_per90,
                CASE
                    WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS FLOAT)
                    ELSE CAST(non_penalty_xGoals_per90 AS FLOAT)
                END AS non_penalty_xGoals_per90,
                CASE
                    WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS FLOAT)
                    ELSE CAST(xGoals_overperformance AS FLOAT)
                END AS xGoals_overperformance,
                CASE 
                    WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
                    WHEN RIGHT(conversion_rate, 1)  = '%' THEN (CAST(REPLACE(conversion_rate, '%', '') AS FLOAT)/100)
                END AS conversion_rate,
                CASE 
                    WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
                    ELSE CAST(assists AS INT)
                END AS assists,
                CASE
                    WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS FLOAT)
                    ELSE CAST(assists_per90 AS FLOAT)
                END AS assists_per90,
                CASE
                    WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(passes_attempted_per90 AS FLOAT)
                END AS passes_attempted_per90,
                CASE 
                    WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
                END AS pass_accuracy,
                CASE
                    WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS FLOAT)
                    ELSE CAST(xAssits_per90 AS FLOAT)
                END AS xAssits_per90,
                CASE
                    WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS FLOAT)
                    ELSE CAST(open_play_key_passes_per90 AS FLOAT)
                END AS open_play_key_passes_per90,
                CASE
                    WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS FLOAT)
                    ELSE CAST(chances_created_per90 AS FLOAT)
                END AS chances_created_per90,
                CASE
                    WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS FLOAT)
                    ELSE CAST(dribbles_made_per90 AS FLOAT)
                END AS dribbles_made_per90,
                CASE
                    WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS FLOAT)
                    ELSE CAST(progressive_passes_per90 AS FLOAT)
                END AS progressive_passes_per90,
                CASE
                    WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(open_play_crosses_attempted_per90 AS FLOAT)
                END AS open_play_crosses_attempted_per90,
                CASE 
                    WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(open_play_cross_accuracy, '%', '') AS FLOAT)/100)
                END AS open_play_cross_accuracy,
                CASE
                    WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(crosses_attempted_per90 AS FLOAT)
                END AS crosses_attempted_per90,
                CASE 
                    WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(cross_accuracy, '%', '') AS FLOAT)/100)
                END AS cross_accuracy,
                CASE
                    WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
                    ELSE CAST(tackles_attempted AS FLOAT)
                END AS tackles_attempted,
                CASE 
                    WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN (CAST(REPLACE(tackle_accuracy, '%', '') AS FLOAT)/100)
                END AS tackle_accuracy,
                CASE
                    WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(pressures_attempted_per90 AS FLOAT)
                END AS pressures_attempted_per90,
                CASE
                    WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS FLOAT)
                    ELSE CAST(pressures_completed_per90 AS FLOAT)
                END AS pressures_completed_per90,
                CASE
                    WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
                    ELSE CAST(possession_won_per90 AS FLOAT)
                END AS possession_won_per90,
                CASE
                    WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
                    ELSE CAST(possession_lost_per90 AS FLOAT)
                END AS possession_lost_per90,
                CASE
                    WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS FLOAT)
                    ELSE CAST(key_tackles_per90 AS FLOAT)
                END AS key_tackles_per90,
                CASE
                    WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
                    ELSE CAST(interceptions_per90 AS FLOAT)
                END AS interceptions_per90,
                CASE
                    WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
                    ELSE CAST(clearances_per90 AS FLOAT)
                END AS clearances_per90,
                CASE
                    WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS FLOAT)
                    ELSE CAST(blocks_per90 AS FLOAT)
                END AS blocks_per90,
                CASE
                    WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS FLOAT)
                    ELSE CAST(shots_blocked_per90 AS FLOAT)
                END AS shots_blocked_per90,
                CASE
                    WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS FLOAT)
                    ELSE CAST(headers_attempted_per90 AS FLOAT)
                END AS headers_attempted_per90,
                CASE 
                    WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
                    WHEN RIGHT(heading_accuracy, 1)  = '%' THEN (CAST(REPLACE(heading_accuracy, '%', '') AS FLOAT)/100)
                END AS heading_accuracy,
                CASE
                    WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS FLOAT)
                    ELSE CAST(key_headers_per90 AS FLOAT)
                END AS key_headers_per90,
                CASE
                    WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS FLOAT)
                    ELSE CAST(sprints_per90 AS FLOAT)
                END AS sprints_per90,
                CASE
                    WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
                    ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
                END AS distance_covered_km_per90,
                CASE
                    WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
                    ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
                END AS mistakes_leading_to_goals,
                CASE
                    WHEN fouls_made = '-' THEN CAST(REPLACE(fouls_made, '-', '0') AS INT)
                    ELSE CAST(fouls_made AS DECIMAL(4,2))
                END AS fouls_made,
                CASE
                    WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
                    ELSE CAST(fouls_against AS DECIMAL(4,2))
                END AS fouls_against,
                CASE
                    WHEN yellow_cards = '-' THEN CAST(REPLACE(yellow_cards, '-', '0') AS INT)
                    ELSE CAST(yellow_cards AS DECIMAL(4,2))
                END AS yellow_cards,
                CASE
                    WHEN red_cards = '-' THEN CAST(REPLACE(red_cards, '-', '0') AS INT)
                    ELSE CAST(red_cards AS DECIMAL(4,2))
                END AS red_cards,
                CASE
                    WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
                    ELSE CAST(
                        (CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
                        AS DECIMAL(10, 1)
                    )
                END AS wage_per_week_k,
                CASE
                    WHEN transfer_value = 'Not for Sale' THEN NULL
                    WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
                        REPLACE(
                            CAST(
                                REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
                                (CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
                                LEN(transfer_value)), '£', '') 
                            AS FLOAT) * 0.001,
                        ' ', '')
                    WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
                        REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
                    WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
                        REPLACE(
                            CAST(
                                REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
                                (CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
                                LEN(transfer_value)), '£', '') 
                            AS FLOAT), 
                        ' ', '')
                    WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
                        REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
                    WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
                END AS transfer_value_m,
                GETDATE() AS dwh_create_date,
                '9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
                1 AS dwh_current_validity
            FROM bronze.fmdata_team_players
            WHERE player_name LIKE '% - Pick Player'
        )
		INSERT INTO silver.fmdata_team_players (
            player_id, player_name, club_name, league,	position, age, minutes_played, team_goals_scored_per90, team_goals_conceded_per90,	
			goals,	goals_outside_the_box, shots_per90, xGoals_per_shot, shot_accuracy, shots_on_target_per90, shots_outside_the_box_per90,
			goals_per90, xGoals_per90, non_penalty_xGoals_per90, xGoals_overperformance, conversion_rate, assists, assists_per90, passes_attempted_per90,
			pass_accuracy, xAssits_per90, open_play_key_passes_per90, chances_created_per90, dribbles_made_per90, progressive_passes_per90, 
			open_play_crosses_attempted_per90, open_play_cross_accuracy, crosses_attempted_per90, cross_accuracy, tackles_attempted_per90,
			tackle_accuracy, pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,	key_tackles_per90,
			interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90, headers_attempted_per90, heading_accuracy, key_headers_per90,
			sprints_per90, distance_covered_km_per90, mistakes_leading_to_goals_per90, fouls_made_per90, fouls_against_per90, yellow_cards_per90,
			red_cards_per90, wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
        )
		SELECT
			player_id,
			player_name,
			club_name,
			league,
			position,
			age,
			minutes_played,
			team_goals_scored_per90,
			team_goals_conceded_per90,	
			goals,	
			goals_outside_the_box,
			shots_per90,	
			xGoals_per_shot,
			shot_accuracy,
			shots_on_target_per90,
			shots_outside_the_box_per90,
			goals_per90,
			xGoals_per90,
			non_penalty_xGoals_per90,
			xGoals_overperformance,
			conversion_rate,
			assists,
			assists_per90,
			passes_attempted_per90,
			pass_accuracy,
			xAssits_per90,
			open_play_key_passes_per90,
			chances_created_per90,
			dribbles_made_per90,
			progressive_passes_per90,
			open_play_crosses_attempted_per90,
			open_play_cross_accuracy,
			crosses_attempted_per90,
			cross_accuracy,
			CAST(CASE WHEN tackles_attempted = 0 THEN 0
				ELSE (tackles_attempted/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS tackles_attempted_per90,
			tackle_accuracy,
			pressures_attempted_per90,
			pressures_completed_per90,
			possession_won_per90,
			possession_lost_per90,
			key_tackles_per90,
			interceptions_per90,
			clearances_per90,
			blocks_per90,
			shots_blocked_per90,
			headers_attempted_per90,
			heading_accuracy,
			key_headers_per90,
			sprints_per90,
			distance_covered_km_per90,
			CAST(CASE WHEN mistakes_leading_to_goals = 0 THEN 0
				ELSE (mistakes_leading_to_goals/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS mistakes_leading_to_goals_per90,
			CAST(CASE WHEN fouls_made = 0 THEN 0
				ELSE (fouls_made/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_made_per90,
			CAST(CASE WHEN fouls_against = 0 THEN 0
				ELSE (fouls_against/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_against_per90,
			CAST(CASE WHEN yellow_cards = 0 THEN 0
				ELSE (yellow_cards/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS yellow_cards_per90,
			CAST(CASE WHEN red_cards = 0 THEN 0
				ELSE (red_cards/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS red_cards_per90,
			wage_per_week_k,
			transfer_value_m,
			dwh_create_date,
			dwh_cd_valid_till,
			dwh_current_validity
		FROM cleaned_bronze AS B
		WHERE NOT EXISTS (
			SELECT 1
			FROM silver.fmdata_team_players AS S
			WHERE S.player_id = B.player_id
			AND S.age = B.age
			AND S.position = B.position
			AND S.dwh_current_validity = 1
		);

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		--- FMDATA_TEAM_GKS
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_team_gks';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>IDENTIFYING OLD RECORDS: silver.fmdata_team_gks';
		WITH cleaned_bronze AS (
            SELECT
				CAST(player_id AS VARCHAR) AS player_id,
				REPLACE(player_name, ' - Pick Player', '') AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				REPLACE(position, 'GK', 'Goalkeeper') AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals_conceded_per90  = '-' THEN CAST(REPLACE(goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN goals_conceded_per90  != '-' THEN CAST(REPLACE(goals_conceded_per90, ',', '') AS FLOAT)
				END AS goals_conceded_per90,
				CASE 
					WHEN saves_made_per90  = '-' THEN CAST(REPLACE(saves_made_per90, '-', '0') AS FLOAT)
					WHEN saves_made_per90  != '-' THEN CAST(REPLACE(saves_made_per90, ',', '') AS FLOAT)
				END AS saves_made_per90,
				CASE 
					WHEN xGoals_prevented_per90  = '-' THEN CAST(REPLACE(xGoals_prevented_per90, '-', '0') AS FLOAT)
					WHEN xGoals_prevented_per90  != '-' THEN CAST(REPLACE(xGoals_prevented_per90, ',', '') AS FLOAT)
				END AS xGoals_prevented_per90,
				CASE 
					WHEN xSave_rate  = '-' THEN CAST(REPLACE(xSave_rate, '-', '0') AS INT)
					WHEN RIGHT(xSave_rate, 1)  = '%' THEN (CAST(REPLACE(xSave_rate, '%', '') AS FLOAT)/100)
				END AS xSave_rate,
				CASE
					WHEN saves_tipped = '-' THEN CAST(REPLACE(saves_tipped, '-', '0') AS INT)
					ELSE CAST(saves_tipped AS FLOAT)
				END AS saves_tipped,
				CASE
					WHEN saves_parried = '-' THEN CAST(REPLACE(saves_parried, '-', '0') AS INT)
					ELSE CAST(saves_parried AS FLOAT)
				END AS saves_parried,
				CASE
					WHEN saves_held = '-' THEN CAST(REPLACE(saves_held, '-', '0') AS INT)
					ELSE CAST(saves_held AS FLOAT)
				END AS saves_held,
				CASE 
					WHEN saves_percentage  = '-' THEN CAST(REPLACE(saves_percentage, '-', '0') AS INT)
					WHEN RIGHT(saves_percentage, 1)  = '%' THEN (CAST(REPLACE(saves_percentage, '%', '') AS FLOAT)/100)
				END AS saves_percentage,
				CASE 
					WHEN passes_attempted_per90  = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					WHEN passes_attempted_per90  != '-' THEN CAST(REPLACE(passes_attempted_per90, ',', '') AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE 
					WHEN possession_won_per90  = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					WHEN possession_won_per90  != '-' THEN CAST(REPLACE(possession_won_per90, ',', '') AS FLOAT)
				END AS possession_won_per90,
				CASE 
					WHEN possession_lost_per90  = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					WHEN possession_lost_per90  != '-' THEN CAST(REPLACE(possession_lost_per90, ',', '') AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
						WHEN penalties_faced = '-' THEN CAST(REPLACE(penalties_faced, '-', '0') AS INT)
						ELSE CAST(penalties_faced AS FLOAT)
					END AS penalties_faced,
				CASE 
					WHEN penalties_save_percentage  = '-' THEN CAST(REPLACE(penalties_save_percentage, '-', '0') AS INT)
					WHEN RIGHT(penalties_save_percentage, 1)  = '%' THEN (CAST(REPLACE(penalties_save_percentage, '%', '') AS FLOAT)/100)
				END AS penalties_save_percentage,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL(4,2))
				END AS fouls_against,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1) -- Adjust precision (10) and scale (1) as needed
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_team_gks
        )

        UPDATE S
		SET 
			dwh_cd_valid_till = GETDATE(),
			dwh_current_validity = 0
		FROM silver.fmdata_team_gks AS S
		JOIN cleaned_bronze AS B
			ON S.player_id = B.player_id
		WHERE S.player_id = B.player_id
			AND S.age != B.age
			AND S.club_name != B.club_name
			AND S.dwh_current_validity = 1;

		PRINT '>>> INSERTING NEW DATA INTO: silver.fmdata_team_gks';
        WITH cleaned_bronze AS (
            SELECT
				CAST(player_id AS VARCHAR) AS player_id,
				REPLACE(player_name, ' - Pick Player', '') AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				REPLACE(position, 'GK', 'Goalkeeper') AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals_conceded_per90  = '-' THEN CAST(REPLACE(goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN goals_conceded_per90  != '-' THEN CAST(REPLACE(goals_conceded_per90, ',', '') AS FLOAT)
				END AS goals_conceded_per90,
				CASE 
					WHEN saves_made_per90  = '-' THEN CAST(REPLACE(saves_made_per90, '-', '0') AS FLOAT)
					WHEN saves_made_per90  != '-' THEN CAST(REPLACE(saves_made_per90, ',', '') AS FLOAT)
				END AS saves_made_per90,
				CASE 
					WHEN xGoals_prevented_per90  = '-' THEN CAST(REPLACE(xGoals_prevented_per90, '-', '0') AS FLOAT)
					WHEN xGoals_prevented_per90  != '-' THEN CAST(REPLACE(xGoals_prevented_per90, ',', '') AS FLOAT)
				END AS xGoals_prevented_per90,
				CASE 
					WHEN xSave_rate  = '-' THEN CAST(REPLACE(xSave_rate, '-', '0') AS INT)
					WHEN RIGHT(xSave_rate, 1)  = '%' THEN (CAST(REPLACE(xSave_rate, '%', '') AS FLOAT)/100)
				END AS xSave_rate,
				CASE
					WHEN saves_tipped = '-' THEN CAST(REPLACE(saves_tipped, '-', '0') AS INT)
					ELSE CAST(saves_tipped AS FLOAT)
				END AS saves_tipped,
				CASE
					WHEN saves_parried = '-' THEN CAST(REPLACE(saves_parried, '-', '0') AS INT)
					ELSE CAST(saves_parried AS FLOAT)
				END AS saves_parried,
				CASE
					WHEN saves_held = '-' THEN CAST(REPLACE(saves_held, '-', '0') AS INT)
					ELSE CAST(saves_held AS FLOAT)
				END AS saves_held,
				CASE 
					WHEN saves_percentage  = '-' THEN CAST(REPLACE(saves_percentage, '-', '0') AS INT)
					WHEN RIGHT(saves_percentage, 1)  = '%' THEN (CAST(REPLACE(saves_percentage, '%', '') AS FLOAT)/100)
				END AS saves_percentage,
				CASE 
					WHEN passes_attempted_per90  = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					WHEN passes_attempted_per90  != '-' THEN CAST(REPLACE(passes_attempted_per90, ',', '') AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE 
					WHEN possession_won_per90  = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					WHEN possession_won_per90  != '-' THEN CAST(REPLACE(possession_won_per90, ',', '') AS FLOAT)
				END AS possession_won_per90,
				CASE 
					WHEN possession_lost_per90  = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					WHEN possession_lost_per90  != '-' THEN CAST(REPLACE(possession_lost_per90, ',', '') AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
						WHEN penalties_faced = '-' THEN CAST(REPLACE(penalties_faced, '-', '0') AS INT)
						ELSE CAST(penalties_faced AS FLOAT)
					END AS penalties_faced,
				CASE 
					WHEN penalties_save_percentage  = '-' THEN CAST(REPLACE(penalties_save_percentage, '-', '0') AS INT)
					WHEN RIGHT(penalties_save_percentage, 1)  = '%' THEN (CAST(REPLACE(penalties_save_percentage, '%', '') AS FLOAT)/100)
				END AS penalties_save_percentage,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL(4,2))
				END AS fouls_against,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1) -- Adjust precision (10) and scale (1) as needed
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_team_gks
        )
		INSERT INTO silver.fmdata_team_gks (
            player_id, player_name, club_name, league,	position, age, minutes_played, team_goals_scored_per90,	team_goals_conceded_per90,	goals_conceded_per90,	
			saves_made_per90, xGoals_prevented_per90, xSave_rate, saves_tipped_per90, saves_parried_per90, saves_held_per90, saves_percentage, passes_attempted_per90,
			pass_accuracy, possession_won_per90, possession_lost_per90,	interceptions_per90, clearances_per90, penalties_faced_per90, penalties_save_percentage,
			distance_covered_km_per90, mistakes_leading_to_goals_per90, fouls_against_per90, wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
        )
		SELECT
			player_id,
			player_name,
			club_name,
			league,
			position,
			age,
			minutes_played,
			team_goals_scored_per90,
			team_goals_conceded_per90,	
			goals_conceded_per90,	
			saves_made_per90,
			xGoals_prevented_per90,
			xSave_rate,
			CAST(CASE WHEN saves_tipped = 0 THEN 0
				ELSE (saves_tipped/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS saves_tipped_per90,
			CAST(CASE WHEN saves_parried = 0 THEN 0
				ELSE (saves_parried/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS saves_parried_per90,
			CAST(CASE WHEN saves_held = 0 THEN 0
				ELSE (saves_held/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS saves_held_per90,
			saves_percentage,
			passes_attempted_per90,
			pass_accuracy,
			possession_won_per90,
			possession_lost_per90,
			interceptions_per90,
			clearances_per90,
			CAST(CASE WHEN penalties_faced = 0 THEN 0
				ELSE (penalties_faced/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS penalties_faced_per90,
			penalties_save_percentage,
			distance_covered_km_per90,
			CAST(CASE WHEN mistakes_leading_to_goals = 0 THEN 0
				ELSE (mistakes_leading_to_goals/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS mistakes_leading_to_goals_per90,
			CAST(CASE WHEN fouls_against = 0 THEN 0
				ELSE (fouls_against/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_against_per90,
			wage_per_week_k,
			transfer_value_m,
			dwh_create_date,
			dwh_cd_valid_till,
			dwh_current_validity
		FROM cleaned_bronze AS B
		WHERE NOT EXISTS (
			SELECT 1
			FROM silver.fmdata_team_gks AS S
			WHERE S.player_id = B.player_id
			AND S.age = B.age
			AND S.dwh_current_validity = 1
		);

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		--- FMDATA_MANAGER_DATA
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_manager_data';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>IDENTIFYING OLD RECORDS: silver.fmdata_manager_data';
		        WITH cleaned_bronze AS (
            SELECT
                CAST(staff_id AS VARCHAR) AS staff_id,
                TRIM(staff_name) AS staff_name,
                UPPER(TRIM(club_name)) AS club_name,
                TRIM(job_at_club) AS job_at_club,
                CASE
                    WHEN previous_club_name = '-' THEN 'n/a'
                    WHEN previous_club_name IS NULL THEN 'n/a'
                    ELSE(UPPER(TRIM(previous_club_name)))
                END AS previous_club_name,
                CASE
                    WHEN tactical_style = '-' THEN 'n/a'
                    ELSE tactical_style
                END AS tactical_style,
                TRIM(playing_mentality) AS playing_mentality,
                CASE
                    WHEN preferred_formation = '4/4/2002' THEN REPLACE(SUBSTRING(preferred_formation, 1, 5), '/', '-')
                    ELSE TRIM(CAST(preferred_formation AS NVARCHAR))
                END AS preferred_formation,
                TRIM(pressing_style) AS pressing_style,
                TRIM(marking_style) AS marking_style,
                CONVERT(DATE,contract_begins, 103) AS contract_begins,
                CASE
                    WHEN contract_expires = '-' THEN NULL
                    ELSE(CONVERT(DATE,contract_expires, 103))
                END AS contract_expires,
                GETDATE() AS dwh_create_date,
                '9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
                1 AS dwh_current_validity
            FROM bronze.fmdata_manager_data
            WHERE job_at_club = 'Manager'
        )
        
        UPDATE S
		SET 
			dwh_cd_valid_till = GETDATE(),
			dwh_current_validity = 0
		FROM silver.fmdata_manager_data AS S
		JOIN cleaned_bronze AS B
			ON S.staff_id = B.staff_id
		WHERE S.staff_id = B.staff_id
			AND (
                S.contract_begins != B.contract_begins 
                OR ISNULL(S.contract_expires, '1900-01-01') != ISNULL(B.contract_expires, '1900-01-01')
                )
			AND S.dwh_current_validity = 1;

		PRINT '>>> INSERTING NEW DATA INTO: silver.fmdata_manager_data';
        WITH cleaned_bronze AS (
            SELECT
                CAST(staff_id AS VARCHAR) AS staff_id,
                TRIM(staff_name) AS staff_name,
                UPPER(TRIM(club_name)) AS club_name,
                TRIM(job_at_club) AS job_at_club,
                CASE
                    WHEN previous_club_name = '-' THEN 'n/a'
                    WHEN previous_club_name IS NULL THEN 'n/a'
                    ELSE(UPPER(TRIM(previous_club_name)))
                END AS previous_club_name,
                CASE
                    WHEN tactical_style = '-' THEN 'n/a'
                    ELSE tactical_style
                END AS tactical_style,
                TRIM(playing_mentality) AS playing_mentality,
                CASE
                    WHEN preferred_formation = '4/4/2002' THEN REPLACE(SUBSTRING(preferred_formation, 1, 5), '/', '-')
                    ELSE TRIM(CAST(preferred_formation AS NVARCHAR))
                END AS preferred_formation,
                TRIM(pressing_style) AS pressing_style,
                TRIM(marking_style) AS marking_style,
                CONVERT(DATE,contract_begins, 103) AS contract_begins,
                CASE
                    WHEN contract_expires = '-' THEN NULL
                    ELSE(CONVERT(DATE,contract_expires, 103))
                END AS contract_expires,
                GETDATE() AS dwh_create_date,
                '9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
                1 AS dwh_current_validity
            FROM bronze.fmdata_manager_data
            WHERE job_at_club = 'Manager'
        )
		INSERT INTO silver.fmdata_manager_data(
			staff_id, staff_name, club_name,job_at_club, previous_club_name, tactical_style, playing_mentality, preferred_formation, pressing_style,
			marking_style, contract_begins, contract_expires, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
		SELECT
			staff_id,
			staff_name,
			club_name,
			job_at_club,
			previous_club_name,
            tactical_style,
			playing_mentality,
			preferred_formation,
			pressing_style,
			marking_style,
			contract_begins,
			contract_expires,
			dwh_create_date,
			dwh_cd_valid_till,
			dwh_current_validity
		FROM cleaned_bronze AS B
		WHERE NOT EXISTS (
			SELECT 1
			FROM silver.fmdata_manager_data AS S
			WHERE S.staff_id = B.staff_id
			AND S.dwh_current_validity = 1
            AND S.contract_begins = B.contract_begins
            AND ISNULL(S.contract_expires, '1900-01-01') = ISNULL(B.contract_expires, '1900-01-01')
        );

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		--- FMDATA_INTERESTED_OUT_PLAYERS
		PRINT '---------------------------------------------';
		PRINT 'NUMBERED VARIATIONS OF DATA WILL BE APPENDED INTO: bronze.fmdata_interested_out_players'
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>IDENTIFYING OLD RECORDS: silver.fmdata_interested_out_players';
		        WITH cleaned_bronze1 AS (
            SELECT 
				CAST(player_id AS VARCHAR) AS player_id,
				TRIM(player_name) AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				CASE 
					WHEN position  = 'AM (C)' THEN 'Central Attacking Midfielder'
					WHEN position  = 'AM (L)' THEN 'Left Attacking Midfielder'
					WHEN position  = 'AM (R)' THEN 'Right Attacking Midfielder'
					WHEN position  = 'D (C)' THEN 'Central Defender'
					WHEN position  = 'D (L)' THEN 'Left Defender'
					WHEN position  = 'D (R)' THEN 'Right Defender'
					WHEN position  = 'DM' THEN 'Defensive Midfielder'
					WHEN position  = 'M (C)' THEN 'Central Midfielder'
					WHEN position  = 'M (L)' THEN 'Left Midfielder'
					WHEN position  = 'M (R)' THEN 'Right Midfielder'
					WHEN position  = 'ST (C)' THEN 'Striker'
					WHEN position  = 'WB (L)' THEN 'Left Wingback'
					WHEN position  = 'WB (R)' THEN 'Right Wingback'
				END AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals  = '-' THEN CAST(REPLACE(goals, '-', '0') AS INT)
					ELSE CAST(goals AS INT)
				END AS goals,
				CASE 
					WHEN goals_outside_the_box  = '-' THEN CAST(REPLACE(goals_outside_the_box, '-', '0') AS INT)
					ELSE CAST(goals_outside_the_box AS INT)
				END AS goals_outside_the_box,
				CASE 
					WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_per90 AS FLOAT)
				END AS shots_per90,
				CASE 
					WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per_shot AS FLOAT)
				END AS xGoals_per_shot,
				CASE 
					WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
					WHEN RIGHT(shot_accuracy, 1)  = '%' THEN (CAST(REPLACE(shot_accuracy, '%', '') AS FLOAT)/100)
				END AS shot_accuracy,
				CASE
					WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_on_target_per90 AS FLOAT)
				END AS shots_on_target_per90,
				CASE
					WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_outside_the_box_per90 AS FLOAT)
				END AS shots_outside_the_box_per90,
				CASE
					WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS FLOAT)
					ELSE CAST(goals_per90 AS FLOAT)
				END AS goals_per90,
				CASE
					WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per90 AS FLOAT)
				END AS xGoals_per90,
				CASE
					WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(non_penalty_xGoals_per90 AS FLOAT)
				END AS non_penalty_xGoals_per90,
				CASE
					WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_overperformance AS FLOAT)
				END AS xGoals_overperformance,
				CASE 
					WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
					WHEN RIGHT(conversion_rate, 1)  = '%' THEN (CAST(REPLACE(conversion_rate, '%', '') AS FLOAT)/100)
				END AS conversion_rate,
				CASE 
					WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
					ELSE CAST(assists AS INT)
				END AS assists,
				CASE
					WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS FLOAT)
					ELSE CAST(assists_per90 AS FLOAT)
				END AS assists_per90,
				CASE
					WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(passes_attempted_per90 AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE
					WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS FLOAT)
					ELSE CAST(xAssits_per90 AS FLOAT)
				END AS xAssits_per90,
				CASE
					WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_key_passes_per90 AS FLOAT)
				END AS open_play_key_passes_per90,
				CASE
					WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS FLOAT)
					ELSE CAST(chances_created_per90 AS FLOAT)
				END AS chances_created_per90,
				CASE
					WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS FLOAT)
					ELSE CAST(dribbles_made_per90 AS FLOAT)
				END AS dribbles_made_per90,
				CASE
					WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(progressive_passes_per90 AS FLOAT)
				END AS progressive_passes_per90,
				CASE
					WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_crosses_attempted_per90 AS FLOAT)
				END AS open_play_crosses_attempted_per90,
				CASE 
					WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(open_play_cross_accuracy, '%', '') AS FLOAT)/100)
				END AS open_play_cross_accuracy,
				CASE
					WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(crosses_attempted_per90 AS FLOAT)
				END AS crosses_attempted_per90,
				CASE 
					WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(cross_accuracy, '%', '') AS FLOAT)/100)
				END AS cross_accuracy,
				CASE
					WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
					ELSE CAST(tackles_attempted AS FLOAT)
				END AS tackles_attempted,
				CASE 
					WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
					WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN (CAST(REPLACE(tackle_accuracy, '%', '') AS FLOAT)/100)
				END AS tackle_accuracy,
				CASE
					WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_attempted_per90 AS FLOAT)
				END AS pressures_attempted_per90,
				CASE
					WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_completed_per90 AS FLOAT)
				END AS pressures_completed_per90,
				CASE
					WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_won_per90 AS FLOAT)
				END AS possession_won_per90,
				CASE
					WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_lost_per90 AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_tackles_per90 AS FLOAT)
				END AS key_tackles_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
					WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS FLOAT)
					ELSE CAST(blocks_per90 AS FLOAT)
				END AS blocks_per90,
				CASE
					WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_blocked_per90 AS FLOAT)
				END AS shots_blocked_per90,
				CASE
					WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(headers_attempted_per90 AS FLOAT)
				END AS headers_attempted_per90,
				CASE 
					WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
					WHEN RIGHT(heading_accuracy, 1)  = '%' THEN (CAST(REPLACE(heading_accuracy, '%', '') AS FLOAT)/100)
				END AS heading_accuracy,
				CASE
					WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_headers_per90 AS FLOAT)
				END AS key_headers_per90,
				CASE
					WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS FLOAT)
					ELSE CAST(sprints_per90 AS FLOAT)
				END AS sprints_per90,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL( 5, 2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_made = '-' THEN CAST(REPLACE(fouls_made, '-', '0') AS INT)
					ELSE CAST(fouls_made AS DECIMAL( 5, 2))
				END AS fouls_made,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL( 5, 2))
				END AS fouls_against,
				CASE
					WHEN yellow_cards = '-' THEN CAST(REPLACE(yellow_cards, '-', '0') AS INT)
					ELSE CAST(yellow_cards AS DECIMAL( 5, 2))
				END AS yellow_cards,
				CASE
					WHEN red_cards = '-' THEN CAST(REPLACE(red_cards, '-', '0') AS INT)
					ELSE CAST(red_cards AS DECIMAL( 5, 2))
				END AS red_cards,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					WHEN wage_per_week = 'N/A' THEN NULL
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1)
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_interested_out_players1
        ),
        cleaned_bronze2 AS (
            SELECT 
				CAST(player_id AS VARCHAR) AS player_id,
				TRIM(player_name) AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				CASE 
					WHEN position  = 'AM (C)' THEN 'Central Attacking Midfielder'
					WHEN position  = 'AM (L)' THEN 'Left Attacking Midfielder'
					WHEN position  = 'AM (R)' THEN 'Right Attacking Midfielder'
					WHEN position  = 'D (C)' THEN 'Central Defender'
					WHEN position  = 'D (L)' THEN 'Left Defender'
					WHEN position  = 'D (R)' THEN 'Right Defender'
					WHEN position  = 'DM' THEN 'Defensive Midfielder'
					WHEN position  = 'M (C)' THEN 'Central Midfielder'
					WHEN position  = 'M (L)' THEN 'Left Midfielder'
					WHEN position  = 'M (R)' THEN 'Right Midfielder'
					WHEN position  = 'ST (C)' THEN 'Striker'
					WHEN position  = 'WB (L)' THEN 'Left Wingback'
					WHEN position  = 'WB (R)' THEN 'Right Wingback'
				END AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals  = '-' THEN CAST(REPLACE(goals, '-', '0') AS INT)
					ELSE CAST(goals AS INT)
				END AS goals,
				CASE 
					WHEN goals_outside_the_box  = '-' THEN CAST(REPLACE(goals_outside_the_box, '-', '0') AS INT)
					ELSE CAST(goals_outside_the_box AS INT)
				END AS goals_outside_the_box,
				CASE 
					WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_per90 AS FLOAT)
				END AS shots_per90,
				CASE 
					WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per_shot AS FLOAT)
				END AS xGoals_per_shot,
				CASE 
					WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
					WHEN RIGHT(shot_accuracy, 1)  = '%' THEN (CAST(REPLACE(shot_accuracy, '%', '') AS FLOAT)/100)
				END AS shot_accuracy,
				CASE
					WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_on_target_per90 AS FLOAT)
				END AS shots_on_target_per90,
				CASE
					WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_outside_the_box_per90 AS FLOAT)
				END AS shots_outside_the_box_per90,
				CASE
					WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS FLOAT)
					ELSE CAST(goals_per90 AS FLOAT)
				END AS goals_per90,
				CASE
					WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per90 AS FLOAT)
				END AS xGoals_per90,
				CASE
					WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(non_penalty_xGoals_per90 AS FLOAT)
				END AS non_penalty_xGoals_per90,
				CASE
					WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_overperformance AS FLOAT)
				END AS xGoals_overperformance,
				CASE 
					WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
					WHEN RIGHT(conversion_rate, 1)  = '%' THEN (CAST(REPLACE(conversion_rate, '%', '') AS FLOAT)/100)
				END AS conversion_rate,
				CASE 
					WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
					ELSE CAST(assists AS INT)
				END AS assists,
				CASE
					WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS FLOAT)
					ELSE CAST(assists_per90 AS FLOAT)
				END AS assists_per90,
				CASE
					WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(passes_attempted_per90 AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE
					WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS FLOAT)
					ELSE CAST(xAssits_per90 AS FLOAT)
				END AS xAssits_per90,
				CASE
					WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_key_passes_per90 AS FLOAT)
				END AS open_play_key_passes_per90,
				CASE
					WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS FLOAT)
					ELSE CAST(chances_created_per90 AS FLOAT)
				END AS chances_created_per90,
				CASE
					WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS FLOAT)
					ELSE CAST(dribbles_made_per90 AS FLOAT)
				END AS dribbles_made_per90,
				CASE
					WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(progressive_passes_per90 AS FLOAT)
				END AS progressive_passes_per90,
				CASE
					WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_crosses_attempted_per90 AS FLOAT)
				END AS open_play_crosses_attempted_per90,
				CASE 
					WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(open_play_cross_accuracy, '%', '') AS FLOAT)/100)
				END AS open_play_cross_accuracy,
				CASE
					WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(crosses_attempted_per90 AS FLOAT)
				END AS crosses_attempted_per90,
				CASE 
					WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(cross_accuracy, '%', '') AS FLOAT)/100)
				END AS cross_accuracy,
				CASE
					WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
					ELSE CAST(tackles_attempted AS FLOAT)
				END AS tackles_attempted,
				CASE 
					WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
					WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN (CAST(REPLACE(tackle_accuracy, '%', '') AS FLOAT)/100)
				END AS tackle_accuracy,
				CASE
					WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_attempted_per90 AS FLOAT)
				END AS pressures_attempted_per90,
				CASE
					WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_completed_per90 AS FLOAT)
				END AS pressures_completed_per90,
				CASE
					WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_won_per90 AS FLOAT)
				END AS possession_won_per90,
				CASE
					WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_lost_per90 AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_tackles_per90 AS FLOAT)
				END AS key_tackles_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
					WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS FLOAT)
					ELSE CAST(blocks_per90 AS FLOAT)
				END AS blocks_per90,
				CASE
					WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_blocked_per90 AS FLOAT)
				END AS shots_blocked_per90,
				CASE
					WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(headers_attempted_per90 AS FLOAT)
				END AS headers_attempted_per90,
				CASE 
					WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
					WHEN RIGHT(heading_accuracy, 1)  = '%' THEN (CAST(REPLACE(heading_accuracy, '%', '') AS FLOAT)/100)
				END AS heading_accuracy,
				CASE
					WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_headers_per90 AS FLOAT)
				END AS key_headers_per90,
				CASE
					WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS FLOAT)
					ELSE CAST(sprints_per90 AS FLOAT)
				END AS sprints_per90,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL( 5, 2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_made = '-' THEN CAST(REPLACE(fouls_made, '-', '0') AS INT)
					ELSE CAST(fouls_made AS DECIMAL( 5, 2))
				END AS fouls_made,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL( 5, 2))
				END AS fouls_against,
				CASE
					WHEN yellow_cards = '-' THEN CAST(REPLACE(yellow_cards, '-', '0') AS INT)
					ELSE CAST(yellow_cards AS DECIMAL( 5, 2))
				END AS yellow_cards,
				CASE
					WHEN red_cards = '-' THEN CAST(REPLACE(red_cards, '-', '0') AS INT)
					ELSE CAST(red_cards AS DECIMAL( 5, 2))
				END AS red_cards,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					WHEN wage_per_week = 'N/A' THEN NULL
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1)
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_interested_out_players2
        ),
        cleaned_bronze3 AS (
            SELECT 
				CAST(player_id AS VARCHAR) AS player_id,
				TRIM(player_name) AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				CASE 
					WHEN position  = 'AM (C)' THEN 'Central Attacking Midfielder'
					WHEN position  = 'AM (L)' THEN 'Left Attacking Midfielder'
					WHEN position  = 'AM (R)' THEN 'Right Attacking Midfielder'
					WHEN position  = 'D (C)' THEN 'Central Defender'
					WHEN position  = 'D (L)' THEN 'Left Defender'
					WHEN position  = 'D (R)' THEN 'Right Defender'
					WHEN position  = 'DM' THEN 'Defensive Midfielder'
					WHEN position  = 'M (C)' THEN 'Central Midfielder'
					WHEN position  = 'M (L)' THEN 'Left Midfielder'
					WHEN position  = 'M (R)' THEN 'Right Midfielder'
					WHEN position  = 'ST (C)' THEN 'Striker'
					WHEN position  = 'WB (L)' THEN 'Left Wingback'
					WHEN position  = 'WB (R)' THEN 'Right Wingback'
				END AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals  = '-' THEN CAST(REPLACE(goals, '-', '0') AS INT)
					ELSE CAST(goals AS INT)
				END AS goals,
				CASE 
					WHEN goals_outside_the_box  = '-' THEN CAST(REPLACE(goals_outside_the_box, '-', '0') AS INT)
					ELSE CAST(goals_outside_the_box AS INT)
				END AS goals_outside_the_box,
				CASE 
					WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_per90 AS FLOAT)
				END AS shots_per90,
				CASE 
					WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per_shot AS FLOAT)
				END AS xGoals_per_shot,
				CASE 
					WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
					WHEN RIGHT(shot_accuracy, 1)  = '%' THEN (CAST(REPLACE(shot_accuracy, '%', '') AS FLOAT)/100)
				END AS shot_accuracy,
				CASE
					WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_on_target_per90 AS FLOAT)
				END AS shots_on_target_per90,
				CASE
					WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_outside_the_box_per90 AS FLOAT)
				END AS shots_outside_the_box_per90,
				CASE
					WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS FLOAT)
					ELSE CAST(goals_per90 AS FLOAT)
				END AS goals_per90,
				CASE
					WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per90 AS FLOAT)
				END AS xGoals_per90,
				CASE
					WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(non_penalty_xGoals_per90 AS FLOAT)
				END AS non_penalty_xGoals_per90,
				CASE
					WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_overperformance AS FLOAT)
				END AS xGoals_overperformance,
				CASE 
					WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
					WHEN RIGHT(conversion_rate, 1)  = '%' THEN (CAST(REPLACE(conversion_rate, '%', '') AS FLOAT)/100)
				END AS conversion_rate,
				CASE 
					WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
					ELSE CAST(assists AS INT)
				END AS assists,
				CASE
					WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS FLOAT)
					ELSE CAST(assists_per90 AS FLOAT)
				END AS assists_per90,
				CASE
					WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(passes_attempted_per90 AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE
					WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS FLOAT)
					ELSE CAST(xAssits_per90 AS FLOAT)
				END AS xAssits_per90,
				CASE
					WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_key_passes_per90 AS FLOAT)
				END AS open_play_key_passes_per90,
				CASE
					WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS FLOAT)
					ELSE CAST(chances_created_per90 AS FLOAT)
				END AS chances_created_per90,
				CASE
					WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS FLOAT)
					ELSE CAST(dribbles_made_per90 AS FLOAT)
				END AS dribbles_made_per90,
				CASE
					WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(progressive_passes_per90 AS FLOAT)
				END AS progressive_passes_per90,
				CASE
					WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_crosses_attempted_per90 AS FLOAT)
				END AS open_play_crosses_attempted_per90,
				CASE 
					WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(open_play_cross_accuracy, '%', '') AS FLOAT)/100)
				END AS open_play_cross_accuracy,
				CASE
					WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(crosses_attempted_per90 AS FLOAT)
				END AS crosses_attempted_per90,
				CASE 
					WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(cross_accuracy, '%', '') AS FLOAT)/100)
				END AS cross_accuracy,
				CASE
					WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
					ELSE CAST(tackles_attempted AS FLOAT)
				END AS tackles_attempted,
				CASE 
					WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
					WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN (CAST(REPLACE(tackle_accuracy, '%', '') AS FLOAT)/100)
				END AS tackle_accuracy,
				CASE
					WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_attempted_per90 AS FLOAT)
				END AS pressures_attempted_per90,
				CASE
					WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_completed_per90 AS FLOAT)
				END AS pressures_completed_per90,
				CASE
					WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_won_per90 AS FLOAT)
				END AS possession_won_per90,
				CASE
					WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_lost_per90 AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_tackles_per90 AS FLOAT)
				END AS key_tackles_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
					WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS FLOAT)
					ELSE CAST(blocks_per90 AS FLOAT)
				END AS blocks_per90,
				CASE
					WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_blocked_per90 AS FLOAT)
				END AS shots_blocked_per90,
				CASE
					WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(headers_attempted_per90 AS FLOAT)
				END AS headers_attempted_per90,
				CASE 
					WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
					WHEN RIGHT(heading_accuracy, 1)  = '%' THEN (CAST(REPLACE(heading_accuracy, '%', '') AS FLOAT)/100)
				END AS heading_accuracy,
				CASE
					WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_headers_per90 AS FLOAT)
				END AS key_headers_per90,
				CASE
					WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS FLOAT)
					ELSE CAST(sprints_per90 AS FLOAT)
				END AS sprints_per90,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL( 5, 2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_made = '-' THEN CAST(REPLACE(fouls_made, '-', '0') AS INT)
					ELSE CAST(fouls_made AS DECIMAL( 5, 2))
				END AS fouls_made,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL( 5, 2))
				END AS fouls_against,
				CASE
					WHEN yellow_cards = '-' THEN CAST(REPLACE(yellow_cards, '-', '0') AS INT)
					ELSE CAST(yellow_cards AS DECIMAL( 5, 2))
				END AS yellow_cards,
				CASE
					WHEN red_cards = '-' THEN CAST(REPLACE(red_cards, '-', '0') AS INT)
					ELSE CAST(red_cards AS DECIMAL( 5, 2))
				END AS red_cards,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					WHEN wage_per_week = 'N/A' THEN NULL
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1)
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_interested_out_players3
        )

        UPDATE S
		SET 
			dwh_cd_valid_till = GETDATE(),
			dwh_current_validity = 0
		FROM silver.fmdata_interested_out_players AS S
		JOIN cleaned_bronze1 AS B1
			ON S.player_id = B1.player_id
		JOIN cleaned_bronze2 AS B2
			ON S.player_id = B2.player_id
		JOIN cleaned_bronze3 AS B3
			ON S.player_id = B3.player_id
		WHERE S.player_id = B1.player_id 
			AND S.player_id = B2.player_id 
			AND S.player_id = B3.player_id 
			AND S.age != B1.age
			AND S.age != B2.age
			AND S.age != B3.age
			AND S.club_name != B1.club_name
			AND S.club_name != B2.club_name
			AND S.club_name != B3.club_name
			AND S.position != B1.position
			AND S.position != B2.position
			AND S.position != B3.position
			AND S.dwh_current_validity = 1;

        PRINT '---------------------------------------------';
		PRINT '>>> INSERTING NEW [bronze.fmdata_interested_out_players1] DATA INTO: silver.fmdata_interested_out_players';
		-- FMDATA_INTERESTED_OUT_PLAYERS1
        WITH cleaned_bronze1 AS (
            SELECT 
				CAST(player_id AS VARCHAR) AS player_id,
				TRIM(player_name) AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				CASE 
					WHEN position  = 'AM (C)' THEN 'Central Attacking Midfielder'
					WHEN position  = 'AM (L)' THEN 'Left Attacking Midfielder'
					WHEN position  = 'AM (R)' THEN 'Right Attacking Midfielder'
					WHEN position  = 'D (C)' THEN 'Central Defender'
					WHEN position  = 'D (L)' THEN 'Left Defender'
					WHEN position  = 'D (R)' THEN 'Right Defender'
					WHEN position  = 'DM' THEN 'Defensive Midfielder'
					WHEN position  = 'M (C)' THEN 'Central Midfielder'
					WHEN position  = 'M (L)' THEN 'Left Midfielder'
					WHEN position  = 'M (R)' THEN 'Right Midfielder'
					WHEN position  = 'ST (C)' THEN 'Striker'
					WHEN position  = 'WB (L)' THEN 'Left Wingback'
					WHEN position  = 'WB (R)' THEN 'Right Wingback'
				END AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals  = '-' THEN CAST(REPLACE(goals, '-', '0') AS INT)
					ELSE CAST(goals AS INT)
				END AS goals,
				CASE 
					WHEN goals_outside_the_box  = '-' THEN CAST(REPLACE(goals_outside_the_box, '-', '0') AS INT)
					ELSE CAST(goals_outside_the_box AS INT)
				END AS goals_outside_the_box,
				CASE 
					WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_per90 AS FLOAT)
				END AS shots_per90,
				CASE 
					WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per_shot AS FLOAT)
				END AS xGoals_per_shot,
				CASE 
					WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
					WHEN RIGHT(shot_accuracy, 1)  = '%' THEN (CAST(REPLACE(shot_accuracy, '%', '') AS FLOAT)/100)
				END AS shot_accuracy,
				CASE
					WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_on_target_per90 AS FLOAT)
				END AS shots_on_target_per90,
				CASE
					WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_outside_the_box_per90 AS FLOAT)
				END AS shots_outside_the_box_per90,
				CASE
					WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS FLOAT)
					ELSE CAST(goals_per90 AS FLOAT)
				END AS goals_per90,
				CASE
					WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per90 AS FLOAT)
				END AS xGoals_per90,
				CASE
					WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(non_penalty_xGoals_per90 AS FLOAT)
				END AS non_penalty_xGoals_per90,
				CASE
					WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_overperformance AS FLOAT)
				END AS xGoals_overperformance,
				CASE 
					WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
					WHEN RIGHT(conversion_rate, 1)  = '%' THEN (CAST(REPLACE(conversion_rate, '%', '') AS FLOAT)/100)
				END AS conversion_rate,
				CASE 
					WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
					ELSE CAST(assists AS INT)
				END AS assists,
				CASE
					WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS FLOAT)
					ELSE CAST(assists_per90 AS FLOAT)
				END AS assists_per90,
				CASE
					WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(passes_attempted_per90 AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE
					WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS FLOAT)
					ELSE CAST(xAssits_per90 AS FLOAT)
				END AS xAssits_per90,
				CASE
					WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_key_passes_per90 AS FLOAT)
				END AS open_play_key_passes_per90,
				CASE
					WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS FLOAT)
					ELSE CAST(chances_created_per90 AS FLOAT)
				END AS chances_created_per90,
				CASE
					WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS FLOAT)
					ELSE CAST(dribbles_made_per90 AS FLOAT)
				END AS dribbles_made_per90,
				CASE
					WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(progressive_passes_per90 AS FLOAT)
				END AS progressive_passes_per90,
				CASE
					WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_crosses_attempted_per90 AS FLOAT)
				END AS open_play_crosses_attempted_per90,
				CASE 
					WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(open_play_cross_accuracy, '%', '') AS FLOAT)/100)
				END AS open_play_cross_accuracy,
				CASE
					WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(crosses_attempted_per90 AS FLOAT)
				END AS crosses_attempted_per90,
				CASE 
					WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(cross_accuracy, '%', '') AS FLOAT)/100)
				END AS cross_accuracy,
				CASE
					WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
					ELSE CAST(tackles_attempted AS FLOAT)
				END AS tackles_attempted,
				CASE 
					WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
					WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN (CAST(REPLACE(tackle_accuracy, '%', '') AS FLOAT)/100)
				END AS tackle_accuracy,
				CASE
					WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_attempted_per90 AS FLOAT)
				END AS pressures_attempted_per90,
				CASE
					WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_completed_per90 AS FLOAT)
				END AS pressures_completed_per90,
				CASE
					WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_won_per90 AS FLOAT)
				END AS possession_won_per90,
				CASE
					WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_lost_per90 AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_tackles_per90 AS FLOAT)
				END AS key_tackles_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
					WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS FLOAT)
					ELSE CAST(blocks_per90 AS FLOAT)
				END AS blocks_per90,
				CASE
					WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_blocked_per90 AS FLOAT)
				END AS shots_blocked_per90,
				CASE
					WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(headers_attempted_per90 AS FLOAT)
				END AS headers_attempted_per90,
				CASE 
					WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
					WHEN RIGHT(heading_accuracy, 1)  = '%' THEN (CAST(REPLACE(heading_accuracy, '%', '') AS FLOAT)/100)
				END AS heading_accuracy,
				CASE
					WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_headers_per90 AS FLOAT)
				END AS key_headers_per90,
				CASE
					WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS FLOAT)
					ELSE CAST(sprints_per90 AS FLOAT)
				END AS sprints_per90,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL( 5, 2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_made = '-' THEN CAST(REPLACE(fouls_made, '-', '0') AS INT)
					ELSE CAST(fouls_made AS DECIMAL( 5, 2))
				END AS fouls_made,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL( 5, 2))
				END AS fouls_against,
				CASE
					WHEN yellow_cards = '-' THEN CAST(REPLACE(yellow_cards, '-', '0') AS INT)
					ELSE CAST(yellow_cards AS DECIMAL( 5, 2))
				END AS yellow_cards,
				CASE
					WHEN red_cards = '-' THEN CAST(REPLACE(red_cards, '-', '0') AS INT)
					ELSE CAST(red_cards AS DECIMAL( 5, 2))
				END AS red_cards,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					WHEN wage_per_week = 'N/A' THEN NULL
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1)
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_interested_out_players1
        )
		INSERT INTO silver.fmdata_interested_out_players (
            player_id, player_name, club_name, league,	position, age, minutes_played, team_goals_scored_per90, team_goals_conceded_per90,	
			goals,	goals_outside_the_box, shots_per90, xGoals_per_shot, shot_accuracy, shots_on_target_per90, shots_outside_the_box_per90,
			goals_per90, xGoals_per90, non_penalty_xGoals_per90, xGoals_overperformance, conversion_rate, assists, assists_per90, passes_attempted_per90,
			pass_accuracy, xAssits_per90, open_play_key_passes_per90, chances_created_per90, dribbles_made_per90, progressive_passes_per90, 
			open_play_crosses_attempted_per90, open_play_cross_accuracy, crosses_attempted_per90, cross_accuracy, tackles_attempted_per90,
			tackle_accuracy, pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,	key_tackles_per90,
			interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90, headers_attempted_per90, heading_accuracy, key_headers_per90,
			sprints_per90, distance_covered_km_per90, mistakes_leading_to_goals_per90, fouls_made_per90, fouls_against_per90, yellow_cards_per90,
			red_cards_per90, wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
        )
		SELECT
			player_id,
			player_name,
			club_name,
			league,
			position,
			age,
			minutes_played,
			team_goals_scored_per90,
			team_goals_conceded_per90,	
			goals,	
			goals_outside_the_box,
			shots_per90,	
			xGoals_per_shot,
			shot_accuracy,
			shots_on_target_per90,
			shots_outside_the_box_per90,
			goals_per90,
			xGoals_per90,
			non_penalty_xGoals_per90,
			xGoals_overperformance,
			conversion_rate,
			assists,
			assists_per90,
			passes_attempted_per90,
			pass_accuracy,
			xAssits_per90,
			open_play_key_passes_per90,
			chances_created_per90,
			dribbles_made_per90,
			progressive_passes_per90,
			open_play_crosses_attempted_per90,
			open_play_cross_accuracy,
			crosses_attempted_per90,
			cross_accuracy,
			CAST(CASE WHEN tackles_attempted = 0 THEN 0
				ELSE (tackles_attempted/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS tackles_attempted_per90,
			tackle_accuracy,
			pressures_attempted_per90,
			pressures_completed_per90,
			possession_won_per90,
			possession_lost_per90,
			key_tackles_per90,
			interceptions_per90,
			clearances_per90,
			blocks_per90,
			shots_blocked_per90,
			headers_attempted_per90,
			heading_accuracy,
			key_headers_per90,
			sprints_per90,
			distance_covered_km_per90,
			CAST(CASE WHEN mistakes_leading_to_goals = 0 THEN 0
				ELSE (mistakes_leading_to_goals/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS mistakes_leading_to_goals_per90,
			CAST(CASE WHEN fouls_made = 0 THEN 0
				ELSE (fouls_made/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_made_per90,
			CAST(CASE WHEN fouls_against = 0 THEN 0
				ELSE (fouls_against/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_against_per90,
			CAST(CASE WHEN yellow_cards = 0 THEN 0
				ELSE (yellow_cards/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS yellow_cards_per90,
			CAST(CASE WHEN red_cards = 0 THEN 0
				ELSE (red_cards/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS red_cards_per90,
			wage_per_week_k,
			transfer_value_m,
			dwh_create_date,
			dwh_cd_valid_till,
			dwh_current_validity
		FROM cleaned_bronze1 AS B
		WHERE NOT EXISTS (
			SELECT 1
			FROM silver.fmdata_interested_out_players AS S
			WHERE S.player_id = B.player_id
			AND S.age = B.age
			AND S.position = B.position
			AND S.dwh_current_validity = 1
		);

		-- FMDATA_INTERETSED_OUT_PLAYERS2
		PRINT '---------------------------------------------';
		PRINT '>>> INSERTING NEW [bronze.fmdata_interested_out_players2] DATA INTO: silver.fmdata_interested_out_players';
        WITH cleaned_bronze2 AS (
            SELECT 
				CAST(player_id AS VARCHAR) AS player_id,
				TRIM(player_name) AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				CASE 
					WHEN position  = 'AM (C)' THEN 'Central Attacking Midfielder'
					WHEN position  = 'AM (L)' THEN 'Left Attacking Midfielder'
					WHEN position  = 'AM (R)' THEN 'Right Attacking Midfielder'
					WHEN position  = 'D (C)' THEN 'Central Defender'
					WHEN position  = 'D (L)' THEN 'Left Defender'
					WHEN position  = 'D (R)' THEN 'Right Defender'
					WHEN position  = 'DM' THEN 'Defensive Midfielder'
					WHEN position  = 'M (C)' THEN 'Central Midfielder'
					WHEN position  = 'M (L)' THEN 'Left Midfielder'
					WHEN position  = 'M (R)' THEN 'Right Midfielder'
					WHEN position  = 'ST (C)' THEN 'Striker'
					WHEN position  = 'WB (L)' THEN 'Left Wingback'
					WHEN position  = 'WB (R)' THEN 'Right Wingback'
				END AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals  = '-' THEN CAST(REPLACE(goals, '-', '0') AS INT)
					ELSE CAST(goals AS INT)
				END AS goals,
				CASE 
					WHEN goals_outside_the_box  = '-' THEN CAST(REPLACE(goals_outside_the_box, '-', '0') AS INT)
					ELSE CAST(goals_outside_the_box AS INT)
				END AS goals_outside_the_box,
				CASE 
					WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_per90 AS FLOAT)
				END AS shots_per90,
				CASE 
					WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per_shot AS FLOAT)
				END AS xGoals_per_shot,
				CASE 
					WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
					WHEN RIGHT(shot_accuracy, 1)  = '%' THEN (CAST(REPLACE(shot_accuracy, '%', '') AS FLOAT)/100)
				END AS shot_accuracy,
				CASE
					WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_on_target_per90 AS FLOAT)
				END AS shots_on_target_per90,
				CASE
					WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_outside_the_box_per90 AS FLOAT)
				END AS shots_outside_the_box_per90,
				CASE
					WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS FLOAT)
					ELSE CAST(goals_per90 AS FLOAT)
				END AS goals_per90,
				CASE
					WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per90 AS FLOAT)
				END AS xGoals_per90,
				CASE
					WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(non_penalty_xGoals_per90 AS FLOAT)
				END AS non_penalty_xGoals_per90,
				CASE
					WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_overperformance AS FLOAT)
				END AS xGoals_overperformance,
				CASE 
					WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
					WHEN RIGHT(conversion_rate, 1)  = '%' THEN (CAST(REPLACE(conversion_rate, '%', '') AS FLOAT)/100)
				END AS conversion_rate,
				CASE 
					WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
					ELSE CAST(assists AS INT)
				END AS assists,
				CASE
					WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS FLOAT)
					ELSE CAST(assists_per90 AS FLOAT)
				END AS assists_per90,
				CASE
					WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(passes_attempted_per90 AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE
					WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS FLOAT)
					ELSE CAST(xAssits_per90 AS FLOAT)
				END AS xAssits_per90,
				CASE
					WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_key_passes_per90 AS FLOAT)
				END AS open_play_key_passes_per90,
				CASE
					WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS FLOAT)
					ELSE CAST(chances_created_per90 AS FLOAT)
				END AS chances_created_per90,
				CASE
					WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS FLOAT)
					ELSE CAST(dribbles_made_per90 AS FLOAT)
				END AS dribbles_made_per90,
				CASE
					WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(progressive_passes_per90 AS FLOAT)
				END AS progressive_passes_per90,
				CASE
					WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_crosses_attempted_per90 AS FLOAT)
				END AS open_play_crosses_attempted_per90,
				CASE 
					WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(open_play_cross_accuracy, '%', '') AS FLOAT)/100)
				END AS open_play_cross_accuracy,
				CASE
					WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(crosses_attempted_per90 AS FLOAT)
				END AS crosses_attempted_per90,
				CASE 
					WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(cross_accuracy, '%', '') AS FLOAT)/100)
				END AS cross_accuracy,
				CASE
					WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
					ELSE CAST(tackles_attempted AS FLOAT)
				END AS tackles_attempted,
				CASE 
					WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
					WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN (CAST(REPLACE(tackle_accuracy, '%', '') AS FLOAT)/100)
				END AS tackle_accuracy,
				CASE
					WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_attempted_per90 AS FLOAT)
				END AS pressures_attempted_per90,
				CASE
					WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_completed_per90 AS FLOAT)
				END AS pressures_completed_per90,
				CASE
					WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_won_per90 AS FLOAT)
				END AS possession_won_per90,
				CASE
					WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_lost_per90 AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_tackles_per90 AS FLOAT)
				END AS key_tackles_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
					WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS FLOAT)
					ELSE CAST(blocks_per90 AS FLOAT)
				END AS blocks_per90,
				CASE
					WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_blocked_per90 AS FLOAT)
				END AS shots_blocked_per90,
				CASE
					WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(headers_attempted_per90 AS FLOAT)
				END AS headers_attempted_per90,
				CASE 
					WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
					WHEN RIGHT(heading_accuracy, 1)  = '%' THEN (CAST(REPLACE(heading_accuracy, '%', '') AS FLOAT)/100)
				END AS heading_accuracy,
				CASE
					WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_headers_per90 AS FLOAT)
				END AS key_headers_per90,
				CASE
					WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS FLOAT)
					ELSE CAST(sprints_per90 AS FLOAT)
				END AS sprints_per90,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL( 5, 2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_made = '-' THEN CAST(REPLACE(fouls_made, '-', '0') AS INT)
					ELSE CAST(fouls_made AS DECIMAL( 5, 2))
				END AS fouls_made,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL( 5, 2))
				END AS fouls_against,
				CASE
					WHEN yellow_cards = '-' THEN CAST(REPLACE(yellow_cards, '-', '0') AS INT)
					ELSE CAST(yellow_cards AS DECIMAL( 5, 2))
				END AS yellow_cards,
				CASE
					WHEN red_cards = '-' THEN CAST(REPLACE(red_cards, '-', '0') AS INT)
					ELSE CAST(red_cards AS DECIMAL( 5, 2))
				END AS red_cards,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					WHEN wage_per_week = 'N/A' THEN NULL
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1)
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_interested_out_players2
        )
        INSERT INTO silver.fmdata_interested_out_players (
            player_id, player_name, club_name, league,	position, age, minutes_played, team_goals_scored_per90, team_goals_conceded_per90,	
			goals,	goals_outside_the_box, shots_per90, xGoals_per_shot, shot_accuracy, shots_on_target_per90, shots_outside_the_box_per90,
			goals_per90, xGoals_per90, non_penalty_xGoals_per90, xGoals_overperformance, conversion_rate, assists, assists_per90, passes_attempted_per90,
			pass_accuracy, xAssits_per90, open_play_key_passes_per90, chances_created_per90, dribbles_made_per90, progressive_passes_per90, 
			open_play_crosses_attempted_per90, open_play_cross_accuracy, crosses_attempted_per90, cross_accuracy, tackles_attempted_per90,
			tackle_accuracy, pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,	key_tackles_per90,
			interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90, headers_attempted_per90, heading_accuracy, key_headers_per90,
			sprints_per90, distance_covered_km_per90, mistakes_leading_to_goals_per90, fouls_made_per90, fouls_against_per90, yellow_cards_per90,
			red_cards_per90, wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
        )
		SELECT
			player_id,
			player_name,
			club_name,
			league,
			position,
			age,
			minutes_played,
			team_goals_scored_per90,
			team_goals_conceded_per90,	
			goals,	
			goals_outside_the_box,
			shots_per90,	
			xGoals_per_shot,
			shot_accuracy,
			shots_on_target_per90,
			shots_outside_the_box_per90,
			goals_per90,
			xGoals_per90,
			non_penalty_xGoals_per90,
			xGoals_overperformance,
			conversion_rate,
			assists,
			assists_per90,
			passes_attempted_per90,
			pass_accuracy,
			xAssits_per90,
			open_play_key_passes_per90,
			chances_created_per90,
			dribbles_made_per90,
			progressive_passes_per90,
			open_play_crosses_attempted_per90,
			open_play_cross_accuracy,
			crosses_attempted_per90,
			cross_accuracy,
			CAST(CASE WHEN tackles_attempted = 0 THEN 0
				ELSE (tackles_attempted/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS tackles_attempted_per90,
			tackle_accuracy,
			pressures_attempted_per90,
			pressures_completed_per90,
			possession_won_per90,
			possession_lost_per90,
			key_tackles_per90,
			interceptions_per90,
			clearances_per90,
			blocks_per90,
			shots_blocked_per90,
			headers_attempted_per90,
			heading_accuracy,
			key_headers_per90,
			sprints_per90,
			distance_covered_km_per90,
			CAST(CASE WHEN mistakes_leading_to_goals = 0 THEN 0
				ELSE (mistakes_leading_to_goals/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS mistakes_leading_to_goals_per90,
			CAST(CASE WHEN fouls_made = 0 THEN 0
				ELSE (fouls_made/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_made_per90,
			CAST(CASE WHEN fouls_against = 0 THEN 0
				ELSE (fouls_against/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_against_per90,
			CAST(CASE WHEN yellow_cards = 0 THEN 0
				ELSE (yellow_cards/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS yellow_cards_per90,
			CAST(CASE WHEN red_cards = 0 THEN 0
				ELSE (red_cards/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS red_cards_per90,
			wage_per_week_k,
			transfer_value_m,
			dwh_create_date,
			dwh_cd_valid_till,
			dwh_current_validity
		FROM cleaned_bronze2 AS B
		WHERE NOT EXISTS (
			SELECT 1
			FROM silver.fmdata_interested_out_players AS S
			WHERE S.player_id = B.player_id
			AND S.age = B.age
			AND S.position = B.position
			AND S.dwh_current_validity = 1);

		-- FMDATA_INTERESTED_OUT_PLAYERS3
		PRINT '---------------------------------------------';
        PRINT '>>> INSERTING NEW [bronze.fmdata_interested_out_players3] DATA INTO: silver.fmdata_interested_out_players';
		WITH cleaned_bronze3 AS (
            SELECT 
				CAST(player_id AS VARCHAR) AS player_id,
				TRIM(player_name) AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				CASE 
					WHEN position  = 'AM (C)' THEN 'Central Attacking Midfielder'
					WHEN position  = 'AM (L)' THEN 'Left Attacking Midfielder'
					WHEN position  = 'AM (R)' THEN 'Right Attacking Midfielder'
					WHEN position  = 'D (C)' THEN 'Central Defender'
					WHEN position  = 'D (L)' THEN 'Left Defender'
					WHEN position  = 'D (R)' THEN 'Right Defender'
					WHEN position  = 'DM' THEN 'Defensive Midfielder'
					WHEN position  = 'M (C)' THEN 'Central Midfielder'
					WHEN position  = 'M (L)' THEN 'Left Midfielder'
					WHEN position  = 'M (R)' THEN 'Right Midfielder'
					WHEN position  = 'ST (C)' THEN 'Striker'
					WHEN position  = 'WB (L)' THEN 'Left Wingback'
					WHEN position  = 'WB (R)' THEN 'Right Wingback'
				END AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals  = '-' THEN CAST(REPLACE(goals, '-', '0') AS INT)
					ELSE CAST(goals AS INT)
				END AS goals,
				CASE 
					WHEN goals_outside_the_box  = '-' THEN CAST(REPLACE(goals_outside_the_box, '-', '0') AS INT)
					ELSE CAST(goals_outside_the_box AS INT)
				END AS goals_outside_the_box,
				CASE 
					WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_per90 AS FLOAT)
				END AS shots_per90,
				CASE 
					WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per_shot AS FLOAT)
				END AS xGoals_per_shot,
				CASE 
					WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
					WHEN RIGHT(shot_accuracy, 1)  = '%' THEN (CAST(REPLACE(shot_accuracy, '%', '') AS FLOAT)/100)
				END AS shot_accuracy,
				CASE
					WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_on_target_per90 AS FLOAT)
				END AS shots_on_target_per90,
				CASE
					WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_outside_the_box_per90 AS FLOAT)
				END AS shots_outside_the_box_per90,
				CASE
					WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS FLOAT)
					ELSE CAST(goals_per90 AS FLOAT)
				END AS goals_per90,
				CASE
					WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_per90 AS FLOAT)
				END AS xGoals_per90,
				CASE
					WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS FLOAT)
					ELSE CAST(non_penalty_xGoals_per90 AS FLOAT)
				END AS non_penalty_xGoals_per90,
				CASE
					WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS FLOAT)
					ELSE CAST(xGoals_overperformance AS FLOAT)
				END AS xGoals_overperformance,
				CASE 
					WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
					WHEN RIGHT(conversion_rate, 1)  = '%' THEN (CAST(REPLACE(conversion_rate, '%', '') AS FLOAT)/100)
				END AS conversion_rate,
				CASE 
					WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
					ELSE CAST(assists AS INT)
				END AS assists,
				CASE
					WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS FLOAT)
					ELSE CAST(assists_per90 AS FLOAT)
				END AS assists_per90,
				CASE
					WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(passes_attempted_per90 AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE
					WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS FLOAT)
					ELSE CAST(xAssits_per90 AS FLOAT)
				END AS xAssits_per90,
				CASE
					WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_key_passes_per90 AS FLOAT)
				END AS open_play_key_passes_per90,
				CASE
					WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS FLOAT)
					ELSE CAST(chances_created_per90 AS FLOAT)
				END AS chances_created_per90,
				CASE
					WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS FLOAT)
					ELSE CAST(dribbles_made_per90 AS FLOAT)
				END AS dribbles_made_per90,
				CASE
					WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS FLOAT)
					ELSE CAST(progressive_passes_per90 AS FLOAT)
				END AS progressive_passes_per90,
				CASE
					WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(open_play_crosses_attempted_per90 AS FLOAT)
				END AS open_play_crosses_attempted_per90,
				CASE 
					WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(open_play_cross_accuracy, '%', '') AS FLOAT)/100)
				END AS open_play_cross_accuracy,
				CASE
					WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(crosses_attempted_per90 AS FLOAT)
				END AS crosses_attempted_per90,
				CASE 
					WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
					WHEN RIGHT(cross_accuracy, 1)  = '%' THEN (CAST(REPLACE(cross_accuracy, '%', '') AS FLOAT)/100)
				END AS cross_accuracy,
				CASE
					WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
					ELSE CAST(tackles_attempted AS FLOAT)
				END AS tackles_attempted,
				CASE 
					WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
					WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN (CAST(REPLACE(tackle_accuracy, '%', '') AS FLOAT)/100)
				END AS tackle_accuracy,
				CASE
					WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_attempted_per90 AS FLOAT)
				END AS pressures_attempted_per90,
				CASE
					WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS FLOAT)
					ELSE CAST(pressures_completed_per90 AS FLOAT)
				END AS pressures_completed_per90,
				CASE
					WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_won_per90 AS FLOAT)
				END AS possession_won_per90,
				CASE
					WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					ELSE CAST(possession_lost_per90 AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_tackles_per90 AS FLOAT)
				END AS key_tackles_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
					WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS FLOAT)
					ELSE CAST(blocks_per90 AS FLOAT)
				END AS blocks_per90,
				CASE
					WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS FLOAT)
					ELSE CAST(shots_blocked_per90 AS FLOAT)
				END AS shots_blocked_per90,
				CASE
					WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS FLOAT)
					ELSE CAST(headers_attempted_per90 AS FLOAT)
				END AS headers_attempted_per90,
				CASE 
					WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
					WHEN RIGHT(heading_accuracy, 1)  = '%' THEN (CAST(REPLACE(heading_accuracy, '%', '') AS FLOAT)/100)
				END AS heading_accuracy,
				CASE
					WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS FLOAT)
					ELSE CAST(key_headers_per90 AS FLOAT)
				END AS key_headers_per90,
				CASE
					WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS FLOAT)
					ELSE CAST(sprints_per90 AS FLOAT)
				END AS sprints_per90,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL( 5, 2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_made = '-' THEN CAST(REPLACE(fouls_made, '-', '0') AS INT)
					ELSE CAST(fouls_made AS DECIMAL( 5, 2))
				END AS fouls_made,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL( 5, 2))
				END AS fouls_against,
				CASE
					WHEN yellow_cards = '-' THEN CAST(REPLACE(yellow_cards, '-', '0') AS INT)
					ELSE CAST(yellow_cards AS DECIMAL( 5, 2))
				END AS yellow_cards,
				CASE
					WHEN red_cards = '-' THEN CAST(REPLACE(red_cards, '-', '0') AS INT)
					ELSE CAST(red_cards AS DECIMAL( 5, 2))
				END AS red_cards,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					WHEN wage_per_week = 'N/A' THEN NULL
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1)
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_interested_out_players3
        )
        INSERT INTO silver.fmdata_interested_out_players (
            player_id, player_name, club_name, league,	position, age, minutes_played, team_goals_scored_per90, team_goals_conceded_per90,	
			goals,	goals_outside_the_box, shots_per90, xGoals_per_shot, shot_accuracy, shots_on_target_per90, shots_outside_the_box_per90,
			goals_per90, xGoals_per90, non_penalty_xGoals_per90, xGoals_overperformance, conversion_rate, assists, assists_per90, passes_attempted_per90,
			pass_accuracy, xAssits_per90, open_play_key_passes_per90, chances_created_per90, dribbles_made_per90, progressive_passes_per90, 
			open_play_crosses_attempted_per90, open_play_cross_accuracy, crosses_attempted_per90, cross_accuracy, tackles_attempted_per90,
			tackle_accuracy, pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,	key_tackles_per90,
			interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90, headers_attempted_per90, heading_accuracy, key_headers_per90,
			sprints_per90, distance_covered_km_per90, mistakes_leading_to_goals_per90, fouls_made_per90, fouls_against_per90, yellow_cards_per90,
			red_cards_per90, wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
        )
		SELECT
			player_id,
			player_name,
			club_name,
			league,
			position,
			age,
			minutes_played,
			team_goals_scored_per90,
			team_goals_conceded_per90,	
			goals,	
			goals_outside_the_box,
			shots_per90,	
			xGoals_per_shot,
			shot_accuracy,
			shots_on_target_per90,
			shots_outside_the_box_per90,
			goals_per90,
			xGoals_per90,
			non_penalty_xGoals_per90,
			xGoals_overperformance,
			conversion_rate,
			assists,
			assists_per90,
			passes_attempted_per90,
			pass_accuracy,
			xAssits_per90,
			open_play_key_passes_per90,
			chances_created_per90,
			dribbles_made_per90,
			progressive_passes_per90,
			open_play_crosses_attempted_per90,
			open_play_cross_accuracy,
			crosses_attempted_per90,
			cross_accuracy,
			CAST(CASE WHEN tackles_attempted = 0 THEN 0
				ELSE (tackles_attempted/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS tackles_attempted_per90,
			tackle_accuracy,
			pressures_attempted_per90,
			pressures_completed_per90,
			possession_won_per90,
			possession_lost_per90,
			key_tackles_per90,
			interceptions_per90,
			clearances_per90,
			blocks_per90,
			shots_blocked_per90,
			headers_attempted_per90,
			heading_accuracy,
			key_headers_per90,
			sprints_per90,
			distance_covered_km_per90,
			CAST(CASE WHEN mistakes_leading_to_goals = 0 THEN 0
				ELSE (mistakes_leading_to_goals/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS mistakes_leading_to_goals_per90,
			CAST(CASE WHEN fouls_made = 0 THEN 0
				ELSE (fouls_made/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_made_per90,
			CAST(CASE WHEN fouls_against = 0 THEN 0
				ELSE (fouls_against/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_against_per90,
			CAST(CASE WHEN yellow_cards = 0 THEN 0
				ELSE (yellow_cards/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS yellow_cards_per90,
			CAST(CASE WHEN red_cards = 0 THEN 0
				ELSE (red_cards/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS red_cards_per90,
			wage_per_week_k,
			transfer_value_m,
			dwh_create_date,
			dwh_cd_valid_till,
			dwh_current_validity
		FROM cleaned_bronze3 AS B
		WHERE NOT EXISTS (
			SELECT 1
			FROM silver.fmdata_interested_out_players AS S
			WHERE S.player_id = B.player_id
			AND S.age = B.age
			AND S.position = B.position
			AND S.dwh_current_validity = 1);

		SET @end_time = GETDATE();
		PRINT 'APPENDING COMPLETE INTO: fmdata.interested_out_players';
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		-- FMDATA_INTERESTED_GKS
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_gks';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>IDENTIFYING OLD RECORDS: silver.fmdata_interested_gks';
		        WITH cleaned_bronze AS (
            SELECT
				CAST(player_id AS VARCHAR) AS player_id,
				REPLACE(player_name, ' - Pick Player', '') AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				REPLACE(position, 'GK', 'Goalkeeper') AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals_conceded_per90  = '-' THEN CAST(REPLACE(goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN goals_conceded_per90  != '-' THEN CAST(REPLACE(goals_conceded_per90, ',', '') AS FLOAT)
				END AS goals_conceded_per90,
				CASE 
					WHEN saves_made_per90  = '-' THEN CAST(REPLACE(saves_made_per90, '-', '0') AS FLOAT)
					WHEN saves_made_per90  != '-' THEN CAST(REPLACE(saves_made_per90, ',', '') AS FLOAT)
				END AS saves_made_per90,
				CASE 
					WHEN xGoals_prevented_per90  = '-' THEN CAST(REPLACE(xGoals_prevented_per90, '-', '0') AS FLOAT)
					WHEN xGoals_prevented_per90  != '-' THEN CAST(REPLACE(xGoals_prevented_per90, ',', '') AS FLOAT)
				END AS xGoals_prevented_per90,
				CASE 
					WHEN xSave_rate  = '-' THEN CAST(REPLACE(xSave_rate, '-', '0') AS INT)
					WHEN RIGHT(xSave_rate, 1)  = '%' THEN (CAST(REPLACE(xSave_rate, '%', '') AS FLOAT)/100)
				END AS xSave_rate,
				CASE
					WHEN saves_tipped = '-' THEN CAST(REPLACE(saves_tipped, '-', '0') AS INT)
					ELSE CAST(saves_tipped AS FLOAT)
				END AS saves_tipped,
				CASE
					WHEN saves_parried = '-' THEN CAST(REPLACE(saves_parried, '-', '0') AS INT)
					ELSE CAST(saves_parried AS FLOAT)
				END AS saves_parried,
				CASE
					WHEN saves_held = '-' THEN CAST(REPLACE(saves_held, '-', '0') AS INT)
					ELSE CAST(saves_held AS FLOAT)
				END AS saves_held,
				CASE 
					WHEN saves_percentage  = '-' THEN CAST(REPLACE(saves_percentage, '-', '0') AS INT)
					WHEN RIGHT(saves_percentage, 1)  = '%' THEN (CAST(REPLACE(saves_percentage, '%', '') AS FLOAT)/100)
				END AS saves_percentage,
				CASE 
					WHEN passes_attempted_per90  = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					WHEN passes_attempted_per90  != '-' THEN CAST(REPLACE(passes_attempted_per90, ',', '') AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE 
					WHEN possession_won_per90  = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					WHEN possession_won_per90  != '-' THEN CAST(REPLACE(possession_won_per90, ',', '') AS FLOAT)
				END AS possession_won_per90,
				CASE 
					WHEN possession_lost_per90  = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					WHEN possession_lost_per90  != '-' THEN CAST(REPLACE(possession_lost_per90, ',', '') AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
						WHEN penalties_faced = '-' THEN CAST(REPLACE(penalties_faced, '-', '0') AS INT)
						ELSE CAST(penalties_faced AS FLOAT)
					END AS penalties_faced,
				CASE 
					WHEN penalties_save_percentage  = '-' THEN CAST(REPLACE(penalties_save_percentage, '-', '0') AS INT)
					WHEN RIGHT(penalties_save_percentage, 1)  = '%' THEN (CAST(REPLACE(penalties_save_percentage, '%', '') AS FLOAT)/100)
				END AS penalties_save_percentage,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL(4,2))
				END AS fouls_against,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1) -- Adjust precision (10) and scale (1) as needed
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_interested_gks
        )
        
        UPDATE S
		SET 
			dwh_cd_valid_till = GETDATE(),
			dwh_current_validity = 0
		FROM silver.fmdata_interested_gks AS S
		JOIN cleaned_bronze AS B
			ON S.player_id = B.player_id
		WHERE S.player_id = B.player_id
			AND S.age != B.age
			AND S.club_name != B.club_name
			AND S.dwh_current_validity = 1;

		PRINT '>>> INSERTING NEW DATA INTO: silver.fmdata_interested_gks';
        WITH cleaned_bronze AS (
            SELECT
				CAST(player_id AS VARCHAR) AS player_id,
				REPLACE(player_name, ' - Pick Player', '') AS player_name,
				TRIM(club_name) AS club_name,
				UPPER(league) AS league,
				REPLACE(position, 'GK', 'Goalkeeper') AS position,
				CAST(age AS INT) AS age,
				CASE 
					WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
					WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
				END AS minutes_played,
				CASE 
					WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS FLOAT)
					WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS FLOAT)
				END AS team_goals_scored_per90,
				CASE 
					WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS FLOAT)
				END AS team_goals_conceded_per90,
				CASE 
					WHEN goals_conceded_per90  = '-' THEN CAST(REPLACE(goals_conceded_per90, '-', '0') AS FLOAT)
					WHEN goals_conceded_per90  != '-' THEN CAST(REPLACE(goals_conceded_per90, ',', '') AS FLOAT)
				END AS goals_conceded_per90,
				CASE 
					WHEN saves_made_per90  = '-' THEN CAST(REPLACE(saves_made_per90, '-', '0') AS FLOAT)
					WHEN saves_made_per90  != '-' THEN CAST(REPLACE(saves_made_per90, ',', '') AS FLOAT)
				END AS saves_made_per90,
				CASE 
					WHEN xGoals_prevented_per90  = '-' THEN CAST(REPLACE(xGoals_prevented_per90, '-', '0') AS FLOAT)
					WHEN xGoals_prevented_per90  != '-' THEN CAST(REPLACE(xGoals_prevented_per90, ',', '') AS FLOAT)
				END AS xGoals_prevented_per90,
				CASE 
					WHEN xSave_rate  = '-' THEN CAST(REPLACE(xSave_rate, '-', '0') AS INT)
					WHEN RIGHT(xSave_rate, 1)  = '%' THEN (CAST(REPLACE(xSave_rate, '%', '') AS FLOAT)/100)
				END AS xSave_rate,
				CASE
					WHEN saves_tipped = '-' THEN CAST(REPLACE(saves_tipped, '-', '0') AS INT)
					ELSE CAST(saves_tipped AS FLOAT)
				END AS saves_tipped,
				CASE
					WHEN saves_parried = '-' THEN CAST(REPLACE(saves_parried, '-', '0') AS INT)
					ELSE CAST(saves_parried AS FLOAT)
				END AS saves_parried,
				CASE
					WHEN saves_held = '-' THEN CAST(REPLACE(saves_held, '-', '0') AS INT)
					ELSE CAST(saves_held AS FLOAT)
				END AS saves_held,
				CASE 
					WHEN saves_percentage  = '-' THEN CAST(REPLACE(saves_percentage, '-', '0') AS INT)
					WHEN RIGHT(saves_percentage, 1)  = '%' THEN (CAST(REPLACE(saves_percentage, '%', '') AS FLOAT)/100)
				END AS saves_percentage,
				CASE 
					WHEN passes_attempted_per90  = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS FLOAT)
					WHEN passes_attempted_per90  != '-' THEN CAST(REPLACE(passes_attempted_per90, ',', '') AS FLOAT)
				END AS passes_attempted_per90,
				CASE 
					WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
					WHEN RIGHT(pass_accuracy, 1)  = '%' THEN (CAST(REPLACE(pass_accuracy, '%', '') AS FLOAT)/100)
				END AS pass_accuracy,
				CASE 
					WHEN possession_won_per90  = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS FLOAT)
					WHEN possession_won_per90  != '-' THEN CAST(REPLACE(possession_won_per90, ',', '') AS FLOAT)
				END AS possession_won_per90,
				CASE 
					WHEN possession_lost_per90  = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS FLOAT)
					WHEN possession_lost_per90  != '-' THEN CAST(REPLACE(possession_lost_per90, ',', '') AS FLOAT)
				END AS possession_lost_per90,
				CASE
					WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS FLOAT)
					ELSE CAST(interceptions_per90 AS FLOAT)
				END AS interceptions_per90,
				CASE
					WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS FLOAT)
					ELSE CAST(clearances_per90 AS FLOAT)
				END AS clearances_per90,
				CASE
						WHEN penalties_faced = '-' THEN CAST(REPLACE(penalties_faced, '-', '0') AS INT)
						ELSE CAST(penalties_faced AS FLOAT)
					END AS penalties_faced,
				CASE 
					WHEN penalties_save_percentage  = '-' THEN CAST(REPLACE(penalties_save_percentage, '-', '0') AS INT)
					WHEN RIGHT(penalties_save_percentage, 1)  = '%' THEN (CAST(REPLACE(penalties_save_percentage, '%', '') AS FLOAT)/100)
				END AS penalties_save_percentage,
				CASE
					WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS FLOAT)
					ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS FLOAT)
				END AS distance_covered_km_per90,
				CASE
					WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
					ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
				END AS mistakes_leading_to_goals,
				CASE
					WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
					ELSE CAST(fouls_against AS DECIMAL(4,2))
				END AS fouls_against,
				CASE
					WHEN wage_per_week = '-' THEN CAST(REPLACE(wage_per_week, '-', '0') AS INT)
					ELSE CAST(
						(CAST(REPLACE(TRIM(SUBSTRING(wage_per_week, 2, (CHARINDEX('p', wage_per_week) - 3))), ',', '') AS DECIMAL(6, 0)) * 0.001) 
						AS DECIMAL(10, 1) -- Adjust precision (10) and scale (1) as needed
					)
				END AS wage_per_week_k,
				CASE
					WHEN transfer_value = 'Not for Sale' THEN NULL
					WHEN RIGHT(transfer_value, 1) = 'K' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'K', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT) * 0.001,
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'K' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'K', ''), 2, LEN(transfer_value)) AS FLOAT) * 0.001, ' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND NOT LEN(transfer_value) <= 7 THEN 
						REPLACE(
							CAST(
								REPLACE(SUBSTRING(REPLACE(transfer_value, 'M', ''), 
								(CAST(CHARINDEX('-', transfer_value) AS DECIMAL( 9, 0)) + 2), 
								LEN(transfer_value)), '£', '') 
							AS FLOAT), 
						' ', '')
					WHEN RIGHT(transfer_value, 1) = 'M' AND LEN(transfer_value) <= 7 THEN
						REPLACE(CAST(SUBSTRING(REPLACE(transfer_value, 'M', ''), 2, LEN(transfer_value)) AS FLOAT), ' ', '')
					WHEN RIGHT(transfer_value, 1) LIKE '%[0-9]' THEN REPLACE(CAST(SUBSTRING(transfer_value, 2, LEN(transfer_value)) AS DECIMAL( 9, 0)), ' ', '')
				END AS transfer_value_m,
				GETDATE() AS dwh_create_date,
				'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
				1 AS dwh_current_validity
			FROM bronze.fmdata_interested_gks
        )
		INSERT INTO silver.fmdata_interested_gks (
            player_id, player_name, club_name, league,	position, age, minutes_played, team_goals_scored_per90,	team_goals_conceded_per90,	goals_conceded_per90,	
			saves_made_per90, xGoals_prevented_per90, xSave_rate, saves_tipped_per90, saves_parried_per90, saves_held_per90, saves_percentage, passes_attempted_per90,
			pass_accuracy, possession_won_per90, possession_lost_per90,	interceptions_per90, clearances_per90, penalties_faced_per90, penalties_save_percentage,
			distance_covered_km_per90, mistakes_leading_to_goals_per90, fouls_against_per90, wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
        )
		SELECT
			player_id,
			player_name,
			club_name,
			league,
			position,
			age,
			minutes_played,
			team_goals_scored_per90,
			team_goals_conceded_per90,	
			goals_conceded_per90,	
			saves_made_per90,
			xGoals_prevented_per90,
			xSave_rate,
			CAST(CASE WHEN saves_tipped = 0 THEN 0
				ELSE (saves_tipped/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS saves_tipped_per90,
			CAST(CASE WHEN saves_parried = 0 THEN 0
				ELSE (saves_parried/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS saves_parried_per90,
			CAST(CASE WHEN saves_held = 0 THEN 0
				ELSE (saves_held/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS saves_held_per90,
			saves_percentage,
			passes_attempted_per90,
			pass_accuracy,
			possession_won_per90,
			possession_lost_per90,
			interceptions_per90,
			clearances_per90,
			CAST(CASE WHEN penalties_faced = 0 THEN 0
				ELSE (penalties_faced/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS penalties_faced_per90,
			penalties_save_percentage,
			distance_covered_km_per90,
			CAST(CASE WHEN mistakes_leading_to_goals = 0 THEN 0
				ELSE (mistakes_leading_to_goals/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS mistakes_leading_to_goals_per90,
			CAST(CASE WHEN fouls_against = 0 THEN 0
				ELSE (fouls_against/minutes_played * 90) 
			END AS DECIMAL(3,2)) AS fouls_against_per90,
			wage_per_week_k,
			transfer_value_m,
			dwh_create_date,
			dwh_cd_valid_till,
			dwh_current_validity
		FROM cleaned_bronze AS B
		WHERE NOT EXISTS (
			SELECT 1
			FROM silver.fmdata_interested_gks AS S
			WHERE S.player_id = B.player_id
			AND S.age = B.age
			AND S.dwh_current_validity = 1);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		-- FMDATA_POSSESSION_DATA
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_possession_data';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: silver.fmdata_possession_data';
		TRUNCATE TABLE silver.fmdata_possession_data;

		PRINT '>>> INSERTING DATA INTO: silver.fmdata_possession_data';
		INSERT INTO silver.fmdata_possession_data(
			club_name, average_possession)
		SELECT
			UPPER(club_name),
			average_possession
			FROM 
			(
				SELECT 
				CASE
					WHEN club_name = 'Alaves' THEN 'Alavés'
					WHEN club_name = 'Atletico de Madrid' THEN 'Atlético de Madrid'
					WHEN club_name = 'Famalicao' THEN 'Famalicão'
					WHEN club_name = 'FC Koln' THEN 'FC Köln'
					WHEN club_name = 'Hull' THEN 'Hull City'
					WHEN club_name = 'NEC' THEN 'N.E.C.'
					WHEN club_name = 'RAAL La Louviere' THEN 'RAAL La Louvière'
					WHEN club_name = 'Rio' THEN 'Rio Ave'
					WHEN club_name = 'Standard Liege' THEN 'Standard Liège'
					WHEN club_name = 'Vitoria de Cuimaraes' THEN 'Vitória de Guimarães'
				ELSE club_name
				END club_name,
				CASE
					WHEN LEN(REPLACE(average_possession, '-', '')) > 2 THEN CAST(SUBSTRING(REPLACE(average_possession, '-', ''), 1, 2) AS FLOAT)
					ELSE CAST(REPLACE(average_possession, '-', '') AS FLOAT)
				END AS average_possession
		FROM bronze.fmdata_possession_data
		) AS OLD;

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=================================';
		PRINT 'LOADING OF SILVER LAYER COMPLETED';
		PRINT '	TOTAL DURATION: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '=================================';
	END TRY
	BEGIN CATCH
		PRINT '=================================';
		PRINT 'ERROR DURING LOADING SILVER LAYER';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================';
	END CATCH
END