/*
======================================================================
Creates a Stored Procedure: Load Gold Layer (Silver Layer -> Gold Layer)
======================================================================
   This script is designed to create a stored procedure that cleans and 
   loads tables from the silver layer into the normalized gold schema. 
   This procedure:
    - checks for updates in the silver tables before loading the data.
	- uses the 'UPDATE' function change certain columns to indicate record
	 validity.
    - uses the 'INSERT INTO' function to load new data into the tables.

  This Stored Procedure does not require any parameters.
  Example: EXECUTE gold.load_gold;
======================================================================
*/

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '====================';
	PRINT 'LOADING GOLD LAYER';
	PRINT '====================';

	--GOLD.DIM_LEAGUE
		PRINT '=============================================';
		PRINT 'GOLD.DIM_LEAGUE';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_out_players';
		
		
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Inserting New Records in GOLD.DIM_LEAGUE';
			WITH dim_league AS (
				SELECT league
				FROM (
					SELECT DISTINCT(TRIM(league)) AS league
					FROM silver.fmdata_interested_out_players AS B
			) AS dim_league
			)
			INSERT INTO gold.dim_league (league)
			SELECT league FROM dim_league AS N
			WHERE NOT EXISTS (
				SELECT 1
				FROM gold.dim_league AS O
				WHERE N.league = O.league
			)

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.DIM_CLUB
		PRINT '=============================================';
		PRINT 'GOLD.DIM_CLUB';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_out_players';
		
		
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Inserting New Records in GOLD.DIM_CLUB';
			WITH dim_club AS (
				SELECT club_name
				FROM (
					SELECT DISTINCT(TRIM(club_name)) AS club_name
					FROM silver.fmdata_interested_out_players AS B
			) AS dim_club
			)
			INSERT INTO gold.dim_club (club)
			SELECT club_name FROM dim_club AS N
			WHERE NOT EXISTS (
				SELECT 1
				FROM gold.dim_club AS O
				WHERE N.club_name = O.club
			)

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.DIM_TEAM_INFO
		PRINT '=============================================';
		PRINT 'GOLD.DIM_TEAM_INFO';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_out_players';
		PRINT 'LOADING DATA FROM: gold.dim_league';
		PRINT 'LOADING DATA FROM: gold.dim_club';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in GOLD.DIM_TEAM_INFO';
			WITH cleaned AS (
				SELECT
					*,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM (
					SELECT
						CONCAT(S.league_key, '.', S1.club_key) AS team_key,
						S.league,
						S1.club
					FROM silver.fmdata_interested_out_players AS B
					INNER JOIN gold.dim_league AS S
						ON B.league = S.league
					INNER JOIN gold.dim_club AS S1
						ON B.club_name = S1.club
				) AS NEW
			)
			UPDATE S
			SET
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.dim_team_info AS S
			WHERE
				team_key NOT IN (SELECT team_key FROM gold.dim_team_info);

		PRINT '>>> Inserting New Records in GOLD.DIM_TEAM_INFO';
			WITH cleaned AS (
				SELECT
					*,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM (
					SELECT
						CONCAT(S.league_key, '.', S1.club_key) AS team_key,
						S.league,
						S1.club
					FROM silver.fmdata_interested_out_players AS B
					INNER JOIN gold.dim_league AS S
						ON B.league = S.league
					INNER JOIN gold.dim_club AS S1
						ON B.club_name = S1.club
				) AS NEW
			)
			INSERT INTO gold.dim_team_info (
				team_key, league, club_name, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT DISTINCT
				team_key,
				league,
				club,
				dwh_create_date,
				dwh_cd_valid_till,
				dwh_current_validity
			FROM cleaned
			WHERE
				team_key NOT IN (SELECT team_key FROM gold.dim_team_info);

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	-- GOLD.FACT_TEAMS_POSSESSION
		PRINT '=============================================';
		PRINT 'GOLD.FACT_TEAMS_POSSESSION';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_possession_data';
		PRINT 'LOADING DATA FROM: gold.dim_team_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in GOLD.FACT_TEAMS_POSSESSION';
		    WITH OLD AS (
				SELECT
					S.team_key,
					S.club_name,
					O.average_possession,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM silver.fmdata_possession_data AS O
				INNER JOIN gold.dim_team_info AS S
					ON O.club_name = S.club_name
			)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_teams_possession AS S
			INNER JOIN OLD AS O
				ON S.team_key = O.team_key
			WHERE S.team_key = O.team_key
				AND S.average_possession != O.average_possession
				AND S.dwh_current_validity = 1;
		PRINT '>>> Inserting New Records in GOLD.FACT_TEAMS_POSSESSION';
			WITH OLD AS (
				SELECT
					S.team_key,
					S.club_name,
					O.average_possession,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM silver.fmdata_possession_data AS O
				INNER JOIN gold.dim_team_info AS S
					ON O.club_name = S.club_name
			)
			INSERT INTO gold.fact_teams_possession (
				team_key, average_possession, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT
				S.team_key,
				O.average_possession,
				O.dwh_create_date,
				O.dwh_cd_valid_till,
				O.dwh_current_validity
			FROM OLD AS O
			INNER JOIN gold.dim_team_info AS S
				ON O.club_name = S.club_name
			WHERE NOT EXISTS (
				SELECT * FROM gold.fact_teams_possession AS F
				WHERE F.dwh_current_validity = 1
			)

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.DIM_PLAYSTYLE
		PRINT '=============================================';
		PRINT 'GOLD.DIM_PLAYSTYLE';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_manager_data';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Inserting New Records in GOLD.DIM_PLAYSTYLE';
			WITH playstyle AS (
				SELECT
					*
				FROM(
					SELECT
						DISTINCT tactical_style, playing_mentality, preferred_formation, pressing_style, marking_style
					FROM silver.fmdata_manager_data
					WHERE job_at_club = 'Manager') AS styles_of_play
				)
			INSERT INTO gold.dim_playstyle (
				tactical_style, playing_mentality, preferred_formation, pressing_style, marking_style
			)
			SELECT
				CASE
					WHEN tactical_style = '-' THEN 'n/a'
				ELSE tactical_style END AS tactical_style,
				TRIM(playing_mentality) AS playing_mentality,
				CASE
					WHEN preferred_formation = '4/4/2002' THEN REPLACE(SUBSTRING(preferred_formation, 1, 5), '/', '-')
				ELSE TRIM(CAST(preferred_formation AS NVARCHAR)) END AS preferred_formation,
				TRIM(pressing_style) AS pressing_style,
				TRIM(marking_style) AS marking_style
			FROM playstyle
			WHERE NOT EXISTS (
				SELECT 1 FROM gold.dim_playstyle AS D
				WHERE D.tactical_style = 
					CASE
						WHEN tactical_style = '-' THEN 'n/a'
					ELSE tactical_style END
				AND D.playing_mentality = TRIM(playing_mentality)
				AND D.preferred_formation = 
					CASE
						WHEN preferred_formation = '4/4/2002' THEN REPLACE(SUBSTRING(preferred_formation, 1, 5), '/', '-')
					ELSE TRIM(CAST(preferred_formation AS NVARCHAR)) END
				AND D.pressing_style = TRIM(pressing_style)
				AND D.marking_style = TRIM(marking_style)
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.DIM_MANAGER_INFO
		PRINT '=============================================';
		PRINT 'GOLD.DIM_MANAGER_INFO';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_manager_data';
		PRINT 'LOADING DATA FROM: gold.dim_playstyle';
		PRINT 'LOADING DATA FROM: gold.dim_team_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in GOLD.DIM_MANAGER_INFO';
			WITH cleaned AS (
				SELECT
					A.staff_id,
					A.staff_name,
					C.team_key AS current_club_key,
					D.team_key AS previous_club_key,
					B.playstyle_key,
					A.contract_begins,
					CASE
						WHEN A.contract_expires = '-' OR A.contract_expires IS NULL THEN CAST('9999-12-31' AS DATE)
						ELSE A.contract_expires
					END AS contract_expires,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM silver.fmdata_manager_data AS A
				INNER JOIN gold.dim_playstyle AS B
					ON A.tactical_style = B.tactical_style
					AND A.playing_mentality = B.playing_mentality
					AND A.preferred_formation = B.preferred_formation
					AND A.pressing_style = B.pressing_style
					AND A.marking_style = B.marking_style
				LEFT JOIN gold.dim_team_info AS C
					ON A.club_name = C.club_name
					AND C.dwh_current_validity = 1
				LEFT JOIN gold.dim_team_info AS D
					ON A.previous_club_name = D.club_name
					AND D.dwh_current_validity = 1
				
			)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.dim_manager_info AS S
			INNER JOIN cleaned AS N
				ON S.staff_id = N.staff_id
			WHERE S.dwh_current_validity = 1
				AND (
					S.current_club_key != N.current_club_key
					OR S.previous_club_key != N.previous_club_key
					OR S.playstyle_key != N.playstyle_key
					OR (S.contract_begins = N.contract_begins AND S.contract_expires != N.contract_expires)
					OR (S.contract_begins != N.contract_begins AND S.contract_expires != N.contract_expires)
				);

			PRINT '>>> Inserting New Records in GOLD.DIM_MANAGER_INFO';
			WITH cleaned AS (
				SELECT
					A.staff_id,
					A.staff_name,
					C.team_key AS current_club_key,
					D.team_key AS previous_club_key,
					B.playstyle_key,
					A.contract_begins,
					A.contract_expires,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM silver.fmdata_manager_data AS A
				INNER JOIN gold.dim_playstyle AS B
					ON A.tactical_style = B.tactical_style
					AND A.playing_mentality = B.playing_mentality
					AND A.preferred_formation = B.preferred_formation
					AND A.pressing_style = B.pressing_style
					AND A.marking_style = B.marking_style
				LEFT JOIN gold.dim_team_info AS C
					ON A.club_name = C.club_name
					AND C.dwh_current_validity = 1
				LEFT JOIN gold.dim_team_info AS D
					ON A.previous_club_name = D.club_name
					AND D.dwh_current_validity = 1
				
			)
			INSERT INTO gold.dim_manager_info (
				staff_id, staff_name, current_club_key, previous_club_key, playstyle_key, 
				contract_begins, contract_expires, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT 
				N.staff_id,
				N.staff_name,
				N.current_club_key,
				N.previous_club_key,
				N.playstyle_key,
				N.contract_begins,
				N.contract_expires,
				N.dwh_create_date,
				N.dwh_cd_valid_till,
				N.dwh_current_validity
			FROM cleaned AS N
			WHERE NOT EXISTS (
				SELECT 1
				FROM gold.dim_manager_info AS O
				WHERE O.staff_id = N.staff_id
					AND O.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.DIM_PLAYER_INFO
		PRINT '=============================================';
		PRINT 'GOLD.DIM_PLAYER_INFO';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_out_players';
		
		
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_gks';
		PRINT 'LOADING DATA FROM: silver.fmdata_team_players';
		PRINT 'LOADING DATA FROM: gold.dim_team_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in GOLD.DIM_PLAYER_INFO';
			WITH position AS (
				SELECT DISTINCT(position) FROM (
						SELECT position FROM silver.fmdata_interested_out_players
						UNION
						SELECT position FROM silver.fmdata_interested_gks
					) AS Q
			), cleaned AS (
				SELECT
					player_id,
					player_name,
					team_key,
					contracted,
					age,
					position
				FROM (
					SELECT
						A.player_id,
						A.player_name,
						B.team_key AS team_key,
						0 AS contracted,
						A.age,
						C.position
					FROM silver.fmdata_interested_out_players AS A
					INNER JOIN gold.dim_team_info AS B
						ON A.club_name = B.club_name
					INNER JOIN position AS C
						ON A.position = C.position
					WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players) -- If present, ignore our players
				UNION
					SELECT
						A.player_id,
						A.player_name,
						B.team_key AS team_key,
						1 AS contracted,
						A.age,
						C.position
					FROM silver.fmdata_team_players AS A
					INNER JOIN gold.dim_team_info AS B
						ON A.club_name = B.club_name
					INNER JOIN position AS C
						ON A.position = C.position
				UNION
					SELECT
						A.player_id,
						A.player_name,
						B.team_key AS team_key,
						0 AS contracted,
						A.age,
						C.position
					FROM silver.fmdata_interested_gks AS A
					INNER JOIN gold.dim_team_info AS B
						ON A.club_name = B.club_name
					INNER JOIN position AS C
						ON A.position = C.position
					WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks) -- If present, ignore our players
				UNION
					SELECT
						A.player_id,
						A.player_name,
						B.team_key AS team_key,
						1 AS contracted,
						A.age,
						C.position
					FROM silver.fmdata_team_gks AS A
					INNER JOIN gold.dim_team_info AS B
						ON A.club_name = B.club_name
					INNER JOIN position AS C
						ON A.position = C.position
						) AS player_info
					)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.dim_player_info AS S
			INNER JOIN cleaned AS N
				ON S.player_id = N.player_id
			WHERE S.player_id = N.player_id
				AND S.team_key != N.team_key
				OR S.age != N.age
				OR S.position != N.position
				OR S.contracted != N.contracted
				AND S.dwh_current_validity = 1;

		PRINT '>>> Inserting New Records in GOLD.DIM_PLAYER_INFO';
			WITH position AS (
				SELECT DISTINCT(position) FROM (
						SELECT position FROM silver.fmdata_interested_out_players
						UNION
						SELECT position FROM silver.fmdata_interested_gks
					) AS Q
			), cleaned AS (
				SELECT 
					*,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM (
					SELECT
						player_id,
						player_name,
						team_key,
						contracted,
						age,
						position
					FROM (
						SELECT
							A.player_id,
							A.player_name,
							B.team_key AS team_key,
							0 AS contracted,
							A.age,
							C.position
						FROM silver.fmdata_interested_out_players AS A
						INNER JOIN gold.dim_team_info AS B
							ON A.club_name = B.club_name
						INNER JOIN position AS C
							ON A.position = C.position
						WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players) -- If present, ignore our players
					UNION
						SELECT
							A.player_id,
							A.player_name,
							B.team_key AS team_key,
							1 AS contracted,
							A.age,
							C.position
						FROM silver.fmdata_team_players AS A
						INNER JOIN gold.dim_team_info AS B
							ON A.club_name = B.club_name
						INNER JOIN position AS C
							ON A.position = C.position
					UNION
						SELECT
							A.player_id,
							A.player_name,
							B.team_key AS team_key,
							0 AS contracted,
							A.age,
							C.position
						FROM silver.fmdata_interested_gks AS A
						INNER JOIN gold.dim_team_info AS B
							ON A.club_name = B.club_name
						INNER JOIN position AS C
							ON A.position = C.position
						WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks) -- If present, ignore our players
					UNION
						SELECT
							A.player_id,
							A.player_name,
							B.team_key AS team_key,
							1 AS contracted,
							A.age,
							C.position
						FROM silver.fmdata_team_gks AS A
						INNER JOIN gold.dim_team_info AS B
							ON A.club_name = B.club_name
						INNER JOIN position AS C
							ON A.position = C.position
							) AS B
				) AS player_info
					)
			INSERT INTO gold.dim_player_info (
				player_id, player_name, team_key, contracted, age, position, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT DISTINCT
				S.player_id,
				S.player_name,
				S.team_key,
				S.contracted,
				S.age,
				S.position,
				S.dwh_create_date,
				S.dwh_cd_valid_till,
				S.dwh_current_validity
			FROM cleaned AS S
			LEFT JOIN gold.dim_player_info AS O
				ON S.player_id = O.player_id
				AND O.dwh_current_validity = 1
			WHERE (
					S.team_key != O.team_key
					OR S.age != O.age
					OR S.position != O.position
					OR S.contracted != O.contracted
				)
				OR NOT EXISTS (
					SELECT 1
					FROM gold.dim_player_info AS D
					WHERE D.player_id = S.player_id
						AND D.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.FACT_OUTFIELD_STATS
		PRINT '=============================================';
		PRINT 'GOLD.FACT_OUTFIELD_STATS';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_out_players';
		
		
		PRINT 'LOADING DATA FROM: silver.fmdata_team_players';
		PRINT 'LOADING DATA FROM: gold.dim_team_info';
		PRINT 'LOADING DATA FROM: gold.dim_player_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in GOLD.FACT_OUTFIELD_STATS';
			WITH cleaned AS (
				SELECT
					B.player_key,
					C.team_key,
					A.minutes_played,
					A.team_goals_scored_per90,
					A.team_goals_conceded_per90,	
					A.goals,	
					A.goals_outside_the_box,
					A.shots_per90,
					A.xGoals_per_shot,	
					A.shot_accuracy,
					A.shots_on_target_per90,
					A.shots_outside_the_box_per90,
					A.goals_per90,
					A.xGoals_per90,
					A.non_penalty_xGoals_per90,
					A.xGoals_overperformance,
					A.conversion_rate,
					A.assists,
					A.assists_per90,
					A.passes_attempted_per90,
					A.pass_accuracy,
					A.xAssits_per90,
					A.open_play_key_passes_per90,
					A.chances_created_per90,
					A.dribbles_made_per90,
					A.progressive_passes_per90,
					A.open_play_crosses_attempted_per90,
					A.open_play_cross_accuracy,
					A.crosses_attempted_per90,
					A.cross_accuracy,
					A.tackles_attempted_per90,
					A.tackle_accuracy,
					A.pressures_attempted_per90,
					A.pressures_completed_per90,
					A.possession_won_per90,
					A.possession_lost_per90,
					A.key_tackles_per90,
					A.interceptions_per90,
					A.clearances_per90,
					A.blocks_per90,
					A.shots_blocked_per90,
					A.headers_attempted_per90,
					A.heading_accuracy,
					A.key_headers_per90,
					A.sprints_per90,
					A.distance_covered_km_per90,
					A.mistakes_leading_to_goals_per90,
					A.fouls_made_per90,
					A.fouls_against_per90,
					A.yellow_cards_per90,
					A.red_cards_per90,
					A.dwh_create_date,
					A.dwh_cd_valid_till,
					A.dwh_current_validity
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
					A.minutes_played,
					A.team_goals_scored_per90,
					A.team_goals_conceded_per90,	
					A.goals,	
					A.goals_outside_the_box,
					A.shots_per90,
					A.xGoals_per_shot,	
					A.shot_accuracy,
					A.shots_on_target_per90,
					A.shots_outside_the_box_per90,
					A.goals_per90,
					A.xGoals_per90,
					A.non_penalty_xGoals_per90,
					A.xGoals_overperformance,
					A.conversion_rate,
					A.assists,
					A.assists_per90,
					A.passes_attempted_per90,
					A.pass_accuracy,
					A.xAssits_per90,
					A.open_play_key_passes_per90,
					A.chances_created_per90,
					A.dribbles_made_per90,
					A.progressive_passes_per90,
					A.open_play_crosses_attempted_per90,
					A.open_play_cross_accuracy,
					A.crosses_attempted_per90,
					A.cross_accuracy,
					A.tackles_attempted_per90,
					A.tackle_accuracy,
					A.pressures_attempted_per90,
					A.pressures_completed_per90,
					A.possession_won_per90,
					A.possession_lost_per90,
					A.key_tackles_per90,
					A.interceptions_per90,
					A.clearances_per90,
					A.blocks_per90,
					A.shots_blocked_per90,
					A.headers_attempted_per90,
					A.heading_accuracy,
					A.key_headers_per90,
					A.sprints_per90,
					A.distance_covered_km_per90,
					A.mistakes_leading_to_goals_per90,
					A.fouls_made_per90,
					A.fouls_against_per90,
					A.yellow_cards_per90,
					A.red_cards_per90,
					A.dwh_create_date,
					A.dwh_cd_valid_till,
					A.dwh_current_validity
				FROM silver.fmdata_team_players AS A
				INNER JOIN gold.dim_player_info AS B
					ON A.player_id = B.player_id
				INNER JOIN gold.dim_team_info AS C
					ON A.club_name = C.club_name
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_outfield_player_stats AS S
			INNER JOIN cleaned AS N
				ON S.player_key = N.player_key
			WHERE S.dwh_current_validity = 1
				AND (
					S.team_key != N.team_key
					OR S.minutes_played != N.minutes_played
					OR S.team_goals_scored_per90 != N.team_goals_scored_per90
					OR S.team_goals_conceded_per90 != N.team_goals_conceded_per90
					OR S.goals != N.goals
					OR S.assists != N.assists
					OR S.dribbles_made_per90 != N.dribbles_made_per90
					OR S.passes_attempted_per90 != N.passes_attempted_per90
					OR S.pressures_attempted_per90 != N.pressures_attempted_per90
					OR S.headers_attempted_per90 != N.headers_attempted_per90
					OR S.distance_covered_km_per90 != N.distance_covered_km_per90
					OR S.fouls_made_per90 != N.fouls_made_per90
				);

		PRINT '>>> Inserting New Records in GOLD.FACT_OUTFIELD_STATS';
			WITH cleaned AS (
				SELECT
					B.player_key,
					C.team_key,
					A.minutes_played,
					A.team_goals_scored_per90,
					A.team_goals_conceded_per90,	
					A.goals,	
					A.goals_outside_the_box,
					A.shots_per90,
					A.xGoals_per_shot,	
					A.shot_accuracy,
					A.shots_on_target_per90,
					A.shots_outside_the_box_per90,
					A.goals_per90,
					A.xGoals_per90,
					A.non_penalty_xGoals_per90,
					A.xGoals_overperformance,
					A.conversion_rate,
					A.assists,
					A.assists_per90,
					A.passes_attempted_per90,
					A.pass_accuracy,
					A.xAssits_per90,
					A.open_play_key_passes_per90,
					A.chances_created_per90,
					A.dribbles_made_per90,
					A.progressive_passes_per90,
					A.open_play_crosses_attempted_per90,
					A.open_play_cross_accuracy,
					A.crosses_attempted_per90,
					A.cross_accuracy,
					A.tackles_attempted_per90,
					A.tackle_accuracy,
					A.pressures_attempted_per90,
					A.pressures_completed_per90,
					A.possession_won_per90,
					A.possession_lost_per90,
					A.key_tackles_per90,
					A.interceptions_per90,
					A.clearances_per90,
					A.blocks_per90,
					A.shots_blocked_per90,
					A.headers_attempted_per90,
					A.heading_accuracy,
					A.key_headers_per90,
					A.sprints_per90,
					A.distance_covered_km_per90,
					A.mistakes_leading_to_goals_per90,
					A.fouls_made_per90,
					A.fouls_against_per90,
					A.yellow_cards_per90,
					A.red_cards_per90,
					A.dwh_create_date,
					A.dwh_cd_valid_till,
					A.dwh_current_validity
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
					A.minutes_played,
					A.team_goals_scored_per90,
					A.team_goals_conceded_per90,	
					A.goals,	
					A.goals_outside_the_box,
					A.shots_per90,
					A.xGoals_per_shot,	
					A.shot_accuracy,
					A.shots_on_target_per90,
					A.shots_outside_the_box_per90,
					A.goals_per90,
					A.xGoals_per90,
					A.non_penalty_xGoals_per90,
					A.xGoals_overperformance,
					A.conversion_rate,
					A.assists,
					A.assists_per90,
					A.passes_attempted_per90,
					A.pass_accuracy,
					A.xAssits_per90,
					A.open_play_key_passes_per90,
					A.chances_created_per90,
					A.dribbles_made_per90,
					A.progressive_passes_per90,
					A.open_play_crosses_attempted_per90,
					A.open_play_cross_accuracy,
					A.crosses_attempted_per90,
					A.cross_accuracy,
					A.tackles_attempted_per90,
					A.tackle_accuracy,
					A.pressures_attempted_per90,
					A.pressures_completed_per90,
					A.possession_won_per90,
					A.possession_lost_per90,
					A.key_tackles_per90,
					A.interceptions_per90,
					A.clearances_per90,
					A.blocks_per90,
					A.shots_blocked_per90,
					A.headers_attempted_per90,
					A.heading_accuracy,
					A.key_headers_per90,
					A.sprints_per90,
					A.distance_covered_km_per90,
					A.mistakes_leading_to_goals_per90,
					A.fouls_made_per90,
					A.fouls_against_per90,
					A.yellow_cards_per90,
					A.red_cards_per90,
					A.dwh_create_date,
					A.dwh_cd_valid_till,
					A.dwh_current_validity
				FROM silver.fmdata_team_players AS A
				INNER JOIN gold.dim_player_info AS B
					ON A.player_id = B.player_id
				INNER JOIN gold.dim_team_info AS C
					ON A.club_name = C.club_name
				)
			INSERT INTO gold.fact_outfield_player_stats (
					player_key, team_key, minutes_played, team_goals_scored_per90, team_goals_conceded_per90, goals, goals_outside_the_box,
					shots_per90, xGoals_per_shot, shot_accuracy, shots_on_target_per90, shots_outside_the_box_per90, goals_per90,
					xGoals_per90, non_penalty_xGoals_per90, xGoals_overperformance, conversion_rate, assists, assists_per90,
					passes_attempted_per90, pass_accuracy, xAssits_per90, open_play_key_passes_per90, chances_created_per90, dribbles_made_per90,
					progressive_passes_per90, open_play_crosses_attempted_per90, open_play_cross_accuracy, crosses_attempted_per90, cross_accuracy,
					tackles_attempted_per90, tackle_accuracy, pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,
					key_tackles_per90, interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90, headers_attempted_per90,
					heading_accuracy, key_headers_per90, sprints_per90, distance_covered_km_per90, mistakes_leading_to_goals_per90, fouls_made_per90,
					fouls_against_per90, yellow_cards_per90, red_cards_per90, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
				)
			SELECT DISTINCT
				S.player_key,
				S.team_key,
				S.minutes_played,
				S.team_goals_scored_per90,
				S.team_goals_conceded_per90,	
				S.goals,    
				S.goals_outside_the_box,
				S.shots_per90,	
				S.xGoals_per_shot,
				S.shot_accuracy,
				S.shots_on_target_per90,
				S.shots_outside_the_box_per90,
				S.goals_per90,
				S.xGoals_per90,
				S.non_penalty_xGoals_per90,
				S.xGoals_overperformance,
				S.conversion_rate,
				S.assists,
				S.assists_per90,
				S.passes_attempted_per90,
				S.pass_accuracy,
				S.xAssits_per90,
				S.open_play_key_passes_per90,
				S.chances_created_per90,
				S.dribbles_made_per90,
				S.progressive_passes_per90,
				S.open_play_crosses_attempted_per90,
				S.open_play_cross_accuracy,
				S.crosses_attempted_per90,
				S.cross_accuracy,
				S.tackles_attempted_per90,
				S.tackle_accuracy,
				S.pressures_attempted_per90,
				S.pressures_completed_per90,
				S.possession_won_per90,
				S.possession_lost_per90,
				S.key_tackles_per90,
				S.interceptions_per90,
				S.clearances_per90,
				S.blocks_per90,
				S.shots_blocked_per90,
				S.headers_attempted_per90,
				S.heading_accuracy,
				S.key_headers_per90,
				S.sprints_per90,
				S.distance_covered_km_per90,
				S.mistakes_leading_to_goals_per90,
				S.fouls_made_per90,
				S.fouls_against_per90,
				S.yellow_cards_per90,
				S.red_cards_per90,
				S.dwh_create_date,
				S.dwh_cd_valid_till,
				S.dwh_current_validity
			FROM cleaned AS S
			LEFT JOIN gold.fact_outfield_player_stats AS O
				ON S.player_key = O.player_key
				AND O.dwh_current_validity = 1
			WHERE (
					S.team_key != O.team_key
					OR S.minutes_played != O.minutes_played
					OR S.team_goals_scored_per90 != O.team_goals_scored_per90
					OR S.team_goals_conceded_per90 != O.team_goals_conceded_per90
					OR S.goals != O.goals
					OR S.assists != O.assists
					OR S.dribbles_made_per90 != O.dribbles_made_per90
					OR S.passes_attempted_per90 != O.passes_attempted_per90
					OR S.pressures_attempted_per90 != O.pressures_attempted_per90
					OR S.headers_attempted_per90 != O.headers_attempted_per90
					OR S.distance_covered_km_per90 != O.distance_covered_km_per90
					OR S.fouls_made_per90 != O.fouls_made_per90
				)
			OR NOT EXISTS (
				SELECT 1
				FROM gold.fact_outfield_player_stats AS N
				WHERE N.player_key = S.player_key
					AND S.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.FACT_GK_STATS
		PRINT '=============================================';
		PRINT 'GOLD.FACT_GK_STATS';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_gks';
		PRINT 'LOADING DATA FROM: silver.fmdata_team_gks';
		PRINT 'LOADING DATA FROM: gold.dim_team_info';
		PRINT 'LOADING DATA FROM: gold.dim_player_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in GOLD.FACT_GK_STATS';
			WITH cleaned AS (
					SELECT
						B.player_key,
						C.team_key,
						A.minutes_played,
						A.team_goals_scored_per90,
						A.team_goals_conceded_per90,	
						A.goals_conceded_per90,	
						A.saves_made_per90,
						A.xGoals_prevented_per90,
						A.xSave_rate,
						A.saves_tipped_per90, 
						A.saves_parried_per90, 
						A.saves_held_per90,
						A.saves_percentage,
						A.passes_attempted_per90,
						A.pass_accuracy,
						A.possession_won_per90,
						A.possession_lost_per90,
						A.interceptions_per90,
						A.clearances_per90,
						A.penalties_faced_per90,
						A.penalties_save_percentage,
						A.distance_covered_km_per90,
						A.mistakes_leading_to_goals_per90,
						A.fouls_against_per90,
						A.dwh_create_date,
						A.dwh_cd_valid_till,
						A.dwh_current_validity
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
						A.minutes_played,
						A.team_goals_scored_per90,
						A.team_goals_conceded_per90,	
						A.goals_conceded_per90,	
						A.saves_made_per90,
						A.xGoals_prevented_per90,
						A.xSave_rate,
						A.saves_tipped_per90, 
						A.saves_parried_per90, 
						A.saves_held_per90,
						A.saves_percentage,
						A.passes_attempted_per90,
						A.pass_accuracy,
						A.possession_won_per90,
						A.possession_lost_per90,
						A.interceptions_per90,
						A.clearances_per90,
						A.penalties_faced_per90,
						A.penalties_save_percentage,
						A.distance_covered_km_per90,
						A.mistakes_leading_to_goals_per90,
						A.fouls_against_per90,
						A.dwh_create_date,
						A.dwh_cd_valid_till,
						A.dwh_current_validity
					FROM silver.fmdata_team_gks AS A
					INNER JOIN gold.dim_player_info AS B
						ON A.player_id = B.player_id
					INNER JOIN gold.dim_team_info AS C
						ON A.club_name = C.club_name
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_gk_stats AS S
			INNER JOIN cleaned AS N
				ON S.player_key = N.player_key
			WHERE S.dwh_current_validity = 1
				AND (
					S.team_key != N.team_key
					OR S.minutes_played != N.minutes_played
					OR S.team_goals_scored_per90 != N.team_goals_scored_per90
					OR S.team_goals_conceded_per90 != N.team_goals_conceded_per90
					OR S.goals_conceded_per90 != N.goals_conceded_per90
					OR S.saves_tipped_per90 != N.saves_tipped_per90
					OR S.saves_parried_per90 != N.saves_parried_per90
					OR S.passes_attempted_per90 != N.passes_attempted_per90
					OR S.passes_attempted_per90 != N.passes_attempted_per90
					OR S.clearances_per90 != N.clearances_per90
					OR S.distance_covered_km_per90 != N.distance_covered_km_per90
					OR S.possession_lost_per90 != N.possession_lost_per90
				);
		PRINT '>>> Inserting New Records in GOLD.FACT_GK_STATS';
			WITH cleaned AS (
					SELECT
						B.player_key,
						C.team_key,
						A.minutes_played,
						A.team_goals_scored_per90,
						A.team_goals_conceded_per90,	
						A.goals_conceded_per90,	
						A.saves_made_per90,
						A.xGoals_prevented_per90,
						A.xSave_rate,
						A.saves_tipped_per90, 
						A.saves_parried_per90, 
						A.saves_held_per90,
						A.saves_percentage,
						A.passes_attempted_per90,
						A.pass_accuracy,
						A.possession_won_per90,
						A.possession_lost_per90,
						A.interceptions_per90,
						A.clearances_per90,
						A.penalties_faced_per90,
						A.penalties_save_percentage,
						A.distance_covered_km_per90,
						A.mistakes_leading_to_goals_per90,
						A.fouls_against_per90,
						A.dwh_create_date,
						A.dwh_cd_valid_till,
						A.dwh_current_validity
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
						A.minutes_played,
						A.team_goals_scored_per90,
						A.team_goals_conceded_per90,	
						A.goals_conceded_per90,	
						A.saves_made_per90,
						A.xGoals_prevented_per90,
						A.xSave_rate,
						A.saves_tipped_per90, 
						A.saves_parried_per90, 
						A.saves_held_per90,
						A.saves_percentage,
						A.passes_attempted_per90,
						A.pass_accuracy,
						A.possession_won_per90,
						A.possession_lost_per90,
						A.interceptions_per90,
						A.clearances_per90,
						A.penalties_faced_per90,
						A.penalties_save_percentage,
						A.distance_covered_km_per90,
						A.mistakes_leading_to_goals_per90,
						A.fouls_against_per90,
						A.dwh_create_date,
						A.dwh_cd_valid_till,
						A.dwh_current_validity
					FROM silver.fmdata_team_gks AS A
					INNER JOIN gold.dim_player_info AS B
						ON A.player_id = B.player_id
					INNER JOIN gold.dim_team_info AS C
						ON A.club_name = C.club_name
				)
			INSERT INTO gold.fact_gk_stats (
				player_key, team_key, minutes_played, team_goals_scored_per90, team_goals_conceded_per90,	 goals_conceded_per90,	
				saves_made_per90, xGoals_prevented_per90, xSave_rate, saves_tipped_per90, saves_parried_per90, saves_held_per90,
				saves_percentage, passes_attempted_per90, pass_accuracy, possession_won_per90, possession_lost_per90,
				interceptions_per90, clearances_per90, penalties_faced_per90, penalties_save_percentage, distance_covered_km_per90,
				mistakes_leading_to_goals_per90, fouls_against_per90, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT DISTINCT
				S.player_key,
				S.team_key,
				S.minutes_played,
				S.team_goals_scored_per90,
				S.team_goals_conceded_per90,	
				S.goals_conceded_per90,	
				S.saves_made_per90,
				S.xGoals_prevented_per90,
				S.xSave_rate,
				S.saves_tipped_per90, 
				S.saves_parried_per90, 
				S.saves_held_per90,
				S.saves_percentage,
				S.passes_attempted_per90,
				S.pass_accuracy,
				S.possession_won_per90,
				S.possession_lost_per90,
				S.interceptions_per90,
				S.clearances_per90,
				S.penalties_faced_per90,
				S.penalties_save_percentage,
				S.distance_covered_km_per90,
				S.mistakes_leading_to_goals_per90,
				S.fouls_against_per90,
				S.dwh_create_date,
				S.dwh_cd_valid_till,
				S.dwh_current_validity
			FROM cleaned AS S
			LEFT JOIN gold.fact_gk_stats AS O
				ON S.player_key = O.player_key
				AND O.dwh_current_validity = 1
			WHERE (
					S.team_key != O.team_key
					OR S.minutes_played != O.minutes_played
					OR S.team_goals_scored_per90 != O.team_goals_scored_per90
					OR S.team_goals_conceded_per90 != O.team_goals_conceded_per90
					OR S.goals_conceded_per90 != O.goals_conceded_per90
					OR S.saves_tipped_per90 != O.saves_tipped_per90
					OR S.saves_parried_per90 != O.saves_parried_per90
					OR S.passes_attempted_per90 != O.passes_attempted_per90
					OR S.passes_attempted_per90 != O.passes_attempted_per90
					OR S.clearances_per90 != O.clearances_per90
					OR S.distance_covered_km_per90 != O.distance_covered_km_per90
					OR S.possession_lost_per90 != O.possession_lost_per90
				)
			OR NOT EXISTS (
				SELECT 1
				FROM gold.fact_gk_stats AS N
				WHERE N.player_key = S.player_key
					AND S.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.FACT_PLAYERS_VALUE
		PRINT '=============================================';
		PRINT 'GOLD.FACT_PLAYERS_VALUE';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_out_players';
		PRINT 'LOADING DATA FROM: silver.fmdata_interested_out_gks';
		PRINT 'LOADING DATA FROM: silver.fmdata_team_players';
		PRINT 'LOADING DATA FROM: silver.fmdata_team_gks';
		PRINT 'LOADING DATA FROM: gold.dim_player_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in GOLD.FACT_PLAYERS_VALUE';
			WITH cleaned_silver AS (
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_interested_out_players
					WHERE player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players) -- If present, ignore our players
					UNION
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_interested_gks
					WHERE player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks) -- If present, ignore our players
				),
				cleaned_silver_team AS (
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_team_players
					UNION
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_team_gks
				),
				cleaned AS (
					SELECT
						A.player_key,
						A.team_key,
						B.wage_per_week_k,
						B.transfer_value_m,
						B.dwh_create_date,
						B.dwh_cd_valid_till,
						B.dwh_current_validity
					FROM gold.dim_player_info AS A
					INNER JOIN cleaned_silver AS B
						ON A.player_id = B.player_id
					UNION
					SELECT
						A.player_key,
						A.team_key,
						B.wage_per_week_k,
						B.transfer_value_m,
						B.dwh_create_date,
						B.dwh_cd_valid_till,
						B.dwh_current_validity
					FROM gold.dim_player_info AS A
					INNER JOIN cleaned_silver_team AS B
						ON A.player_id = B.player_id
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_players_value AS S
			INNER JOIN cleaned AS N
				ON S.player_key = N.player_key
			WHERE S.dwh_current_validity = 1
				AND (
					S.team_key != N.team_key
					OR S.wage_per_week_k != N.wage_per_week_k
					OR S.transfer_value_m != N.transfer_value_m
				);

		PRINT '>>> Inserting New Records in GOLD.FACT_PLAYERS_VALUE';
			WITH cleaned_silver AS (
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_interested_out_players
					WHERE player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players) -- If present, ignore our players
					UNION
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_interested_gks
					WHERE player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks) -- If present, ignore our players
				),
				cleaned_silver_team AS (
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_team_players
					UNION
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_team_gks
				),
				cleaned AS (
					SELECT
						A.player_key,
						A.team_key,
						B.wage_per_week_k,
						B.transfer_value_m,
						B.dwh_create_date,
						B.dwh_cd_valid_till,
						B.dwh_current_validity
					FROM gold.dim_player_info AS A
					INNER JOIN cleaned_silver AS B
						ON A.player_id = B.player_id
					UNION
					SELECT
						A.player_key,
						A.team_key,
						B.wage_per_week_k,
						B.transfer_value_m,
						B.dwh_create_date,
						B.dwh_cd_valid_till,
						B.dwh_current_validity
					FROM gold.dim_player_info AS A
					INNER JOIN cleaned_silver_team AS B
						ON A.player_id = B.player_id
				)
			INSERT INTO gold.fact_players_value (
				player_key, team_key, wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT
				S.player_key,
				S.team_key,
				S.wage_per_week_k,
				S.transfer_value_m,
				S.dwh_create_date,
				S.dwh_cd_valid_till,
				S.dwh_current_validity
			FROM cleaned AS S
			LEFT JOIN gold.fact_players_value AS O
				ON S.player_key = O.player_key
				AND O.dwh_current_validity = 1
				WHERE (
					S.team_key != O.team_key
					OR S.wage_per_week_k != O.wage_per_week_k
					OR S.transfer_value_m != O.transfer_value_m
				)
			OR NOT EXISTS (
				SELECT 1
				FROM gold.fact_players_value AS N
				WHERE N.player_key = S.player_key
					AND S.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=================================';
		PRINT 'LOADING OF GOLD LAYER COMPLETED';
		PRINT 'TOTAL DURATION: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '=================================';
	END TRY
	BEGIN CATCH
		PRINT '=================================';
		PRINT 'ERROR DURING LOADING GOLD LAYER';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================';
	END CATCH
END