/*
======================================================================
Creates a Stored Procedure: Load Silver Layer (Bronze Layer -> Silver Layer)
======================================================================
   This script is designed to create a stored procedure that cleans and 
   loads tables from the bronze layer into the normalized silver schema. 
   This procedure:
    - checks for updates in the bronze tables before loading the data.
	- uses the 'UPDATE' function change certain columns to indicate record
	 validity.
    - uses the 'INSERT INTO' function to load new data into the tables.

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

	--SILVER.DIM_LEAGUE
		PRINT '=============================================';
		PRINT 'SILVER.DIM_LEAGUE';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players1';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players2';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players3';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Inserting New Records in SILVER.DIM_LEAGUE';
			WITH dim_league AS (
				SELECT UPPER(league) AS league
				FROM (
					SELECT DISTINCT(TRIM(league)) AS league
					FROM (
						SELECT * FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players2
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players3
					) AS B
			) AS dim_league
			)
			INSERT INTO silver.dim_league (league)
			SELECT league FROM dim_league AS N
			WHERE NOT EXISTS (
				SELECT 1
				FROM silver.dim_league AS O
				WHERE N.league = O.league
			)

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--SILVER.DIM_CLUB
		PRINT '=============================================';
		PRINT 'SILVER.DIM_CLUB';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players1';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players2';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players3';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Inserting New Records in SILVER.DIM_CLUB';
			WITH dim_club AS (
				SELECT UPPER(club_name) AS club_name
				FROM (
					SELECT DISTINCT(TRIM(club_name)) AS club_name
					FROM (
						SELECT * FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players2
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players3
					) AS B
			) AS dim_club
			)
			INSERT INTO silver.dim_club (club)
			SELECT club_name FROM dim_club AS N
			WHERE NOT EXISTS (
				SELECT 1
				FROM silver.dim_club AS O
				WHERE N.club_name = O.club
			)

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--SILVER.DIM_TEAM_INFO
		PRINT '=============================================';
		PRINT 'SILVER.DIM_TEAM_INFO';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players1';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players2';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players3';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players3';
		PRINT 'LOADING DATA FROM: silver.dim_league';
		PRINT 'LOADING DATA FROM: silver.dim_club';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in SILVER.DIM_TEAM_INFO';
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
					FROM (
						SELECT * FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players2
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players3
					) AS B
					INNER JOIN silver.dim_league AS S
						ON B.league = S.league
					INNER JOIN silver.dim_club AS S1
						ON B.club_name = S1.club
				) AS NEW
			)
			UPDATE S
			SET
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM silver.dim_team_info AS S
			WHERE
				team_key NOT IN (SELECT team_key FROM silver.dim_team_info);

		PRINT '>>> Inserting New Records in SILVER.DIM_TEAM_INFO';
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
					FROM (
						SELECT * FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players2
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players3
					) AS B
					INNER JOIN silver.dim_league AS S
						ON B.league = S.league
					INNER JOIN silver.dim_club AS S1
						ON B.club_name = S1.club
				) AS NEW
			)
			INSERT INTO silver.dim_team_info (
				team_key, league, club_name, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT
				team_key,
				league,
				club,
				dwh_create_date,
				dwh_cd_valid_till,
				dwh_current_validity
			FROM cleaned
			WHERE
				team_key NOT IN (SELECT team_key FROM silver.dim_team_info);

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	-- SILVER.FACT_TEAMS_POSSESSION
		PRINT '=============================================';
		PRINT 'SILVER.FACT_TEAMS_POSSESSION';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_possession_data';
		PRINT 'LOADING DATA FROM: silver.dim_team_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in SILVER.FACT_TEAMS_POSSESSION';
		    WITH cleaned_bronze AS (
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
					END AS average_possession,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM bronze.fmdata_possession_data  
			), OLD AS (
				SELECT
					S.team_key,
					S.club_name,
					O.average_possession
				FROM cleaned_bronze AS O
				INNER JOIN silver.dim_team_info AS S
					ON O.club_name = S.club_name
			)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM silver.fact_teams_possession AS S
			INNER JOIN OLD AS O
				ON S.team_key = O.team_key
			WHERE S.team_key = O.team_key
				AND S.average_possession != O.average_possession
				AND S.dwh_current_validity = 1;
		PRINT '>>> Inserting New Records in SILVER.FACT_TEAMS_POSSESSION';
			WITH cleaned_bronze AS (
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
					END AS average_possession,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM bronze.fmdata_possession_data
			)
			INSERT INTO silver.fact_teams_possession (
				team_key, average_possession, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT
				S.team_key,
				O.average_possession,
				O.dwh_create_date,
				O.dwh_cd_valid_till,
				O.dwh_current_validity
			FROM cleaned_bronze AS O
			INNER JOIN silver.dim_team_info AS S
				ON O.club_name = S.club_name
			WHERE NOT EXISTS (
				SELECT 1 FROM silver.fact_teams_possession AS F
				WHERE O.club_name = S.club_name
				AND O.average_possession = F.average_possession
				AND F.dwh_current_validity = 1
			)

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--SILVER.DIM_PLAYSTYLE
		PRINT '=============================================';
		PRINT 'SILVER.DIM_PLAYSTYLE';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_manager_data';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Inserting New Records in SILVER.DIM_PLAYSTYLE';
			WITH playstyle AS (
				SELECT
					*
				FROM(
					SELECT
						DISTINCT tactical_style, playing_mentality, preferred_formation, pressing_style, marking_style
					FROM bronze.fmdata_manager_data
					WHERE job_at_club = 'Manager') AS styles_of_play
				)
			INSERT INTO silver.dim_playstyle (
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
				SELECT 1 FROM silver.dim_playstyle AS D
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

	--SILVER.DIM_MANAGER_INFO
		PRINT '=============================================';
		PRINT 'SILVER.DIM_MANAGER_INFO';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_manager_data';
		PRINT 'LOADING DATA FROM: silver.dim_playstyle';
		PRINT 'LOADING DATA FROM: silver.dim_team_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in SILVER.DIM_MANAGER_INFO';
			WITH cleaned AS (
				SELECT
					DISTINCT(A.staff_id),
					TRIM(A.staff_name) AS staff_name,
					C.team_key AS current_club_key,
					D.team_key AS previous_club_key,
					B.playstyle_key,
					CONVERT(DATE, A.contract_begins, 103) AS contract_begins,
					CASE
						WHEN A.contract_expires = '-' THEN '9999-12-31 23:59:59.9999999'
						ELSE(CONVERT(DATE, A.contract_expires, 103))
					END AS contract_expires,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM bronze.fmdata_manager_data AS A
				INNER JOIN silver.dim_playstyle AS B
					ON A.tactical_style = B.tactical_style
					AND A.playing_mentality = B.playing_mentality
					AND A.preferred_formation = B.preferred_formation
					AND A.pressing_style = B.pressing_style
					AND A.marking_style = B.marking_style
				LEFT JOIN silver.dim_team_info AS C
					ON A.club_name = C.club_name
					AND C.dwh_current_validity = 1
				LEFT JOIN silver.dim_team_info AS D
					ON A.previous_club_name = D.club_name
					AND D.dwh_current_validity = 1
				WHERE A.job_at_club = 'Manager'
			)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM silver.dim_manager_info AS S
			INNER JOIN cleaned AS N
				ON S.staff_id = N.staff_id
			WHERE S.staff_id = N.staff_id
				AND (
					S.current_club_key != N.current_club_key
					OR S.previous_club_key != N.previous_club_key
				) OR (S.contract_begins = N.contract_begins AND S.contract_expires != S.contract_expires)
				OR (S.contract_begins != N.contract_begins AND S.contract_expires != S.contract_expires)
				OR S.playstyle_key != N.playstyle_key
				AND S.dwh_current_validity = 1;

		PRINT '>>> Inserting New Records in SILVER.DIM_MANAGER_INFO';
			WITH cleaned AS (
				SELECT
					DISTINCT(A.staff_id),
					TRIM(A.staff_name) AS staff_name,
					C.team_key AS current_club_key,
					D.team_key AS previous_club_key,
					B.playstyle_key,
					CONVERT(DATE, A.contract_begins, 103) AS contract_begins,
					CASE
						WHEN A.contract_expires = '-' THEN '9999-12-31 23:59:59.9999999'
						ELSE(CONVERT(DATE, A.contract_expires, 103))
					END AS contract_expires,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM bronze.fmdata_manager_data AS A
				INNER JOIN silver.dim_playstyle AS B
					ON A.tactical_style = B.tactical_style
					AND A.playing_mentality = B.playing_mentality
					AND A.preferred_formation = B.preferred_formation
					AND A.pressing_style = B.pressing_style
					AND A.marking_style = B.marking_style
				LEFT JOIN silver.dim_team_info AS C
					ON A.club_name = C.club_name
					AND C.dwh_current_validity = 1
				LEFT JOIN silver.dim_team_info AS D
					ON A.previous_club_name = D.club_name
					AND D.dwh_current_validity = 1
				WHERE A.job_at_club = 'Manager'
			)
			INSERT INTO silver.dim_manager_info (
				staff_id, staff_name, current_club_key, previous_club_key, playstyle_key, contract_begins, contract_expires, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT 
				staff_id,
				staff_name,
				current_club_key,
				previous_club_key,
				playstyle_key,
				contract_begins,
				contract_expires,
				dwh_create_date,
				dwh_cd_valid_till,
				dwh_current_validity
			FROM cleaned AS N
			WHERE NOT EXISTS (
				SELECT 1
				FROM silver.dim_manager_info AS S
				WHERE S.staff_id = N.staff_id
				AND (
					S.current_club_key = N.current_club_key
					OR S.previous_club_key = N.previous_club_key
				) OR (S.contract_begins = N.contract_begins AND S.contract_expires = S.contract_expires)
				OR (S.contract_begins = N.contract_begins AND S.contract_expires = S.contract_expires)
				OR S.playstyle_key = N.playstyle_key
				AND S.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--SILVER.DIM_PLAYER_INFO
		PRINT '=============================================';
		PRINT 'SILVER.DIM_PLAYER_INFO';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players1';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players2';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players3';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_gks';
		PRINT 'LOADING DATA FROM: bronze.fmdata_team_players';
		PRINT 'LOADING DATA FROM: silver.dim_team_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in SILVER.DIM_PLAYER_INFO';
			WITH position AS (
				SELECT 
					position AS base,
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
							WHEN position  = 'GK' THEN 'Goalkeeper'
						END AS position
				FROM (
					SELECT DISTINCT(position) FROM (
						SELECT position FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT position FROM bronze.fmdata_interested_gks
					) AS Q
				) AS Z
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
						CAST(A.age AS INT) AS age,
						C.position
					FROM (
						SELECT * FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players2
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players3
					) AS A
					INNER JOIN silver.dim_team_info AS B
						ON A.club_name = B.club_name
					INNER JOIN position AS C
						ON A.position = C.base
					WHERE A.player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_players) -- If present, ignore our players
				UNION
					SELECT
						A.player_id,
						REPLACE(A.player_name, ' - Pick Player', '') AS player_name,
						B.team_key AS team_key,
						1 AS contracted,
						CAST(A.age AS INT) AS age,
						C.position
					FROM bronze.fmdata_team_players AS A
					INNER JOIN silver.dim_team_info AS B
						ON A.club_name = B.club_name
					INNER JOIN position AS C
						ON A.position = C.base
				UNION
					SELECT
						A.player_id,
						A.player_name,
						B.team_key AS team_key,
						0 AS contracted,
						CAST(A.age AS INT) AS age,
						C.position
					FROM bronze.fmdata_interested_gks AS A
					INNER JOIN silver.dim_team_info AS B
						ON A.club_name = B.club_name
					INNER JOIN position AS C
						ON A.position = C.base
					WHERE A.player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_gks) -- If present, ignore our players
				UNION
					SELECT
						A.player_id,
						REPLACE(A.player_name, ' - Pick Player', '') AS player_name,
						B.team_key AS team_key,
						1 AS contracted,
						CAST(A.age AS INT) AS age,
						C.position
					FROM bronze.fmdata_team_gks AS A
					INNER JOIN silver.dim_team_info AS B
						ON A.club_name = B.club_name
					INNER JOIN position AS C
						ON A.position = C.base
						) AS player_info
					)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM silver.dim_player_info AS S
			INNER JOIN cleaned AS N
				ON S.player_id = N.player_id
			WHERE S.player_id = N.player_id
				AND S.team_key != N.team_key
				OR S.age != N.age
				OR S.position != N.position
				OR S.contracted != N.contracted
				AND S.dwh_current_validity = 1;

		PRINT '>>> Inserting New Records in SILVER.DIM_PLAYER_INFO';
			WITH position AS (
				SELECT 
					position AS base,
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
							WHEN position  = 'GK' THEN 'Goalkeeper'
						END AS position
				FROM (
					SELECT DISTINCT(position) FROM (
						SELECT position FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT position FROM bronze.fmdata_interested_gks
					) AS Q
				) AS Z
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
							CAST(A.age AS INT) AS age,
							C.position
						FROM (
							SELECT * FROM bronze.fmdata_interested_out_players1
							UNION
							SELECT * FROM bronze.fmdata_interested_out_players2
							UNION
							SELECT * FROM bronze.fmdata_interested_out_players3
						) AS A
						INNER JOIN silver.dim_team_info AS B
							ON A.club_name = B.club_name
						INNER JOIN position AS C
							ON A.position = C.base
						WHERE A.player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_players) -- If present, ignore our players
					UNION
						SELECT
							A.player_id,
							REPLACE(A.player_name, ' - Pick Player', '') AS player_name,
							B.team_key AS team_key,
							1 AS contracted,
							CAST(A.age AS INT) AS age,
							C.position
						FROM bronze.fmdata_team_players AS A
						INNER JOIN silver.dim_team_info AS B
							ON A.club_name = B.club_name
						INNER JOIN position AS C
							ON A.position = C.base
					UNION
						SELECT
							A.player_id,
							A.player_name,
							B.team_key AS team_key,
							0 AS contracted,
							CAST(A.age AS INT) AS age,
							C.position
						FROM bronze.fmdata_interested_gks AS A
						INNER JOIN silver.dim_team_info AS B
							ON A.club_name = B.club_name
						INNER JOIN position AS C
							ON A.position = C.base
						WHERE A.player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_gks) -- If present, ignore our players
					UNION
						SELECT
							A.player_id,
							REPLACE(A.player_name, ' - Pick Player', '') AS player_name,
							B.team_key AS team_key,
							1 AS contracted,
							CAST(A.age AS INT) AS age,
							C.position
						FROM bronze.fmdata_team_gks AS A
						INNER JOIN silver.dim_team_info AS B
							ON A.club_name = B.club_name
						INNER JOIN position AS C
							ON A.position = C.base
							) AS B
				) AS player_info
					)
			INSERT INTO silver.dim_player_info (
				player_id, player_name, team_key, contracted, age, position, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT 
				player_id,
				player_name,
				team_key,
				contracted,
				age,
				position,
				dwh_create_date,
				dwh_cd_valid_till,
				dwh_current_validity
			FROM cleaned AS S
			WHERE NOT EXISTS (
				SELECT 1
				FROM silver.dim_player_info AS D
				WHERE D.player_id = S.player_id
					AND D.team_key = S.team_key
					AND D.age = S.age
					AND D.position = S.position
					AND D.contracted = S.contracted
					AND D.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--SILVER.FACT_OUTFIELD_STATS
		PRINT '=============================================';
		PRINT 'SILVER.FACT_OUTFIELD_STATS';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players1';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players2';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players3';
		PRINT 'LOADING DATA FROM: bronze.fmdata_team_players';
		PRINT 'LOADING DATA FROM: silver.dim_team_info';
		PRINT 'LOADING DATA FROM: silver.dim_player_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in SILVER.FACT_OUTFIELD_STATS';
			WITH cleaned_bronze AS (
				SELECT 
					CAST(player_id AS INT) AS player_id,
					TRIM(UPPER(club_name)) AS club_name,
					CASE 
						WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
						WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
					END AS minutes_played,
					CASE 
						WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS DECIMAL(5, 2))
						WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS DECIMAL(5, 2))
					END AS team_goals_scored_per90,
					CASE 
						WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
						WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
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
						WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_per90 AS DECIMAL(5, 2))
					END AS shots_per90,
					CASE 
						WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_per_shot AS DECIMAL(5, 2))
					END AS xGoals_per_shot,
					CASE 
						WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
						WHEN RIGHT(shot_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(shot_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS shot_accuracy,
					CASE
						WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_on_target_per90 AS DECIMAL(5, 2))
					END AS shots_on_target_per90,
					CASE
						WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_outside_the_box_per90 AS DECIMAL(5, 2))
					END AS shots_outside_the_box_per90,
					CASE
						WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(goals_per90 AS DECIMAL(5, 2))
					END AS goals_per90,
					CASE
						WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_per90 AS DECIMAL(5, 2))
					END AS xGoals_per90,
					CASE
						WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(non_penalty_xGoals_per90 AS DECIMAL(5, 2))
					END AS non_penalty_xGoals_per90,
					CASE
						WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_overperformance AS DECIMAL(5, 2))
					END AS xGoals_overperformance,
					CASE 
						WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
						WHEN RIGHT(conversion_rate, 1)  = '%' THEN CAST((CAST(REPLACE(conversion_rate, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS conversion_rate,
					CASE 
						WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
						ELSE CAST(assists AS INT)
					END AS assists,
					CASE
						WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(assists_per90 AS DECIMAL(5, 2))
					END AS assists_per90,
					CASE
						WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(passes_attempted_per90 AS DECIMAL(5, 2))
					END AS passes_attempted_per90,
					CASE 
						WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
						WHEN RIGHT(pass_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(pass_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS pass_accuracy,
					CASE
						WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xAssits_per90 AS DECIMAL(5, 2))
					END AS xAssits_per90,
					CASE
						WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(open_play_key_passes_per90 AS DECIMAL(5, 2))
					END AS open_play_key_passes_per90,
					CASE
						WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(chances_created_per90 AS DECIMAL(5, 2))
					END AS chances_created_per90,
					CASE
						WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(dribbles_made_per90 AS DECIMAL(5, 2))
					END AS dribbles_made_per90,
					CASE
						WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(progressive_passes_per90 AS DECIMAL(5, 2))
					END AS progressive_passes_per90,
					CASE
						WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(open_play_crosses_attempted_per90 AS DECIMAL(5, 2))
					END AS open_play_crosses_attempted_per90,
					CASE 
						WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
						WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(open_play_cross_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS open_play_cross_accuracy,
					CASE
						WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(crosses_attempted_per90 AS DECIMAL(5, 2))
					END AS crosses_attempted_per90,
					CASE 
						WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
						WHEN RIGHT(cross_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(cross_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS cross_accuracy,
					CASE
						WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
						ELSE CAST(tackles_attempted AS DECIMAL(5, 2))
					END AS tackles_attempted,
					CASE 
						WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
						WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(tackle_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS tackle_accuracy,
					CASE
						WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(pressures_attempted_per90 AS DECIMAL(5, 2))
					END AS pressures_attempted_per90,
					CASE
						WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(pressures_completed_per90 AS DECIMAL(5, 2))
					END AS pressures_completed_per90,
					CASE
						WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(possession_won_per90 AS DECIMAL(5, 2))
					END AS possession_won_per90,
					CASE
						WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(possession_lost_per90 AS DECIMAL(5, 2))
					END AS possession_lost_per90,
					CASE
						WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(key_tackles_per90 AS DECIMAL(5, 2))
					END AS key_tackles_per90,
					CASE
						WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(interceptions_per90 AS DECIMAL(5, 2))
					END AS interceptions_per90,
					CASE
						WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(clearances_per90 AS DECIMAL(5, 2))
					END AS clearances_per90,
					CASE
						WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(blocks_per90 AS DECIMAL(5, 2))
					END AS blocks_per90,
					CASE
						WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_blocked_per90 AS DECIMAL(5, 2))
					END AS shots_blocked_per90,
					CASE
						WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(headers_attempted_per90 AS DECIMAL(5, 2))
					END AS headers_attempted_per90,
					CASE 
						WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
						WHEN RIGHT(heading_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(heading_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS heading_accuracy,
					CASE
						WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(key_headers_per90 AS DECIMAL(5, 2))
					END AS key_headers_per90,
					CASE
						WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(sprints_per90 AS DECIMAL(5, 2))
					END AS sprints_per90,
					CASE
						WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS DECIMAL(5, 2))
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
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM (
					SELECT * FROM bronze.fmdata_interested_out_players1
					UNION
					SELECT * FROM bronze.fmdata_interested_out_players2
					UNION
					SELECT * FROM bronze.fmdata_interested_out_players3
				) AS fmdata_interested_out_players
				WHERE player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_players) -- If present, ignore our players
				),
			cleaned_bronze_team AS(
				SELECT 
					CAST(player_id AS INT) AS player_id,
					TRIM(UPPER(club_name)) AS club_name,
					CASE 
						WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
						WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
					END AS minutes_played,
					CASE 
						WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS DECIMAL(5, 2))
						WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS DECIMAL(5, 2))
					END AS team_goals_scored_per90,
					CASE 
						WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
						WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
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
						WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_per90 AS DECIMAL(5, 2))
					END AS shots_per90,
					CASE 
						WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_per_shot AS DECIMAL(5, 2))
					END AS xGoals_per_shot,
					CASE 
						WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
						WHEN RIGHT(shot_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(shot_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS shot_accuracy,
					CASE
						WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_on_target_per90 AS DECIMAL(5, 2))
					END AS shots_on_target_per90,
					CASE
						WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_outside_the_box_per90 AS DECIMAL(5, 2))
					END AS shots_outside_the_box_per90,
					CASE
						WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(goals_per90 AS DECIMAL(5, 2))
					END AS goals_per90,
					CASE
						WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_per90 AS DECIMAL(5, 2))
					END AS xGoals_per90,
					CASE
						WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(non_penalty_xGoals_per90 AS DECIMAL(5, 2))
					END AS non_penalty_xGoals_per90,
					CASE
						WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_overperformance AS DECIMAL(5, 2))
					END AS xGoals_overperformance,
					CASE 
						WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
						WHEN RIGHT(conversion_rate, 1)  = '%' THEN CAST((CAST(REPLACE(conversion_rate, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS conversion_rate,
					CASE 
						WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
						ELSE CAST(assists AS INT)
					END AS assists,
					CASE
						WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(assists_per90 AS DECIMAL(5, 2))
					END AS assists_per90,
					CASE
						WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(passes_attempted_per90 AS DECIMAL(5, 2))
					END AS passes_attempted_per90,
					CASE 
						WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
						WHEN RIGHT(pass_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(pass_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS pass_accuracy,
					CASE
						WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xAssits_per90 AS DECIMAL(5, 2))
					END AS xAssits_per90,
					CASE
						WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(open_play_key_passes_per90 AS DECIMAL(5, 2))
					END AS open_play_key_passes_per90,
					CASE
						WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(chances_created_per90 AS DECIMAL(5, 2))
					END AS chances_created_per90,
					CASE
						WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(dribbles_made_per90 AS DECIMAL(5, 2))
					END AS dribbles_made_per90,
					CASE
						WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(progressive_passes_per90 AS DECIMAL(5, 2))
					END AS progressive_passes_per90,
					CASE
						WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(open_play_crosses_attempted_per90 AS DECIMAL(5, 2))
					END AS open_play_crosses_attempted_per90,
					CASE 
						WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
						WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(open_play_cross_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS open_play_cross_accuracy,
					CASE
						WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(crosses_attempted_per90 AS DECIMAL(5, 2))
					END AS crosses_attempted_per90,
					CASE 
						WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
						WHEN RIGHT(cross_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(cross_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS cross_accuracy,
					CASE
						WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
						ELSE CAST(tackles_attempted AS DECIMAL(5, 2))
					END AS tackles_attempted,
					CASE 
						WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
						WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(tackle_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS tackle_accuracy,
					CASE
						WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(pressures_attempted_per90 AS DECIMAL(5, 2))
					END AS pressures_attempted_per90,
					CASE
						WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(pressures_completed_per90 AS DECIMAL(5, 2))
					END AS pressures_completed_per90,
					CASE
						WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(possession_won_per90 AS DECIMAL(5, 2))
					END AS possession_won_per90,
					CASE
						WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(possession_lost_per90 AS DECIMAL(5, 2))
					END AS possession_lost_per90,
					CASE
						WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(key_tackles_per90 AS DECIMAL(5, 2))
					END AS key_tackles_per90,
					CASE
						WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(interceptions_per90 AS DECIMAL(5, 2))
					END AS interceptions_per90,
					CASE
						WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(clearances_per90 AS DECIMAL(5, 2))
					END AS clearances_per90,
					CASE
						WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(blocks_per90 AS DECIMAL(5, 2))
					END AS blocks_per90,
					CASE
						WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_blocked_per90 AS DECIMAL(5, 2))
					END AS shots_blocked_per90,
					CASE
						WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(headers_attempted_per90 AS DECIMAL(5, 2))
					END AS headers_attempted_per90,
					CASE 
						WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
						WHEN RIGHT(heading_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(heading_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS heading_accuracy,
					CASE
						WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(key_headers_per90 AS DECIMAL(5, 2))
					END AS key_headers_per90,
					CASE
						WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(sprints_per90 AS DECIMAL(5, 2))
					END AS sprints_per90,
					CASE
						WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS DECIMAL(5, 2))
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
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM bronze.fmdata_team_players
			),
			cleaned AS (
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
					CASE WHEN A.tackles_attempted = 0 THEN 0
						ELSE CAST((A.tackles_attempted/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS tackles_attempted_per90,
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
					CASE WHEN A.mistakes_leading_to_goals = 0 THEN 0
						ELSE CAST((A.mistakes_leading_to_goals/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS mistakes_leading_to_goals_per90,
					CASE WHEN A.fouls_made = 0 THEN 0
						ELSE CAST((A.fouls_made/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS fouls_made_per90,
					CASE WHEN A.fouls_against = 0 THEN 0
						ELSE CAST((A.fouls_against/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS fouls_against_per90,
					CASE WHEN A.yellow_cards = 0 THEN 0
						ELSE CAST((A.yellow_cards/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS yellow_cards_per90,
					CASE WHEN A.red_cards = 0 THEN 0
						ELSE CAST((A.red_cards/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS red_cards_per90,
					A.dwh_create_date,
					A.dwh_cd_valid_till,
					A.dwh_current_validity
				FROM cleaned_bronze AS A
				INNER JOIN silver.dim_player_info AS B
					ON A.player_id = B.player_id
				INNER JOIN silver.dim_team_info AS C
					ON A.club_name = C.club_name
				WHERE A.player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_players) -- If present, ignore our players
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
					CASE WHEN A.tackles_attempted = 0 THEN 0
						ELSE CAST((A.tackles_attempted/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS tackles_attempted_per90,
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
					CASE WHEN mistakes_leading_to_goals = 0 THEN 0
						ELSE CAST((mistakes_leading_to_goals/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS mistakes_leading_to_goals_per90,
					CASE WHEN fouls_made = 0 THEN 0
						ELSE CAST((fouls_made/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS fouls_made_per90,
					CASE WHEN fouls_against = 0 THEN 0
						ELSE CAST((fouls_against/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS fouls_against_per90,
					CASE WHEN yellow_cards = 0 THEN 0
						ELSE CAST((yellow_cards/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS yellow_cards_per90,
					CASE WHEN red_cards = 0 THEN 0
						ELSE CAST((red_cards/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS red_cards_per90,
					A.dwh_create_date,
					A.dwh_cd_valid_till,
					A.dwh_current_validity
				FROM cleaned_bronze_team AS A
				INNER JOIN silver.dim_player_info AS B
					ON A.player_id = B.player_id
				INNER JOIN silver.dim_team_info AS C
					ON A.club_name = C.club_name
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM silver.fact_outfield_player_stats AS S
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

		PRINT '>>> Inserting New Records in SILVER.FACT_OUTFIELD_STATS';
			WITH cleaned_bronze AS (
				SELECT 
					CAST(player_id AS INT) AS player_id,
					TRIM(UPPER(club_name)) AS club_name,
					CASE 
						WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
						WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
					END AS minutes_played,
					CASE 
						WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS DECIMAL(5, 2))
						WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS DECIMAL(5, 2))
					END AS team_goals_scored_per90,
					CASE 
						WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
						WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
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
						WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_per90 AS DECIMAL(5, 2))
					END AS shots_per90,
					CASE 
						WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_per_shot AS DECIMAL(5, 2))
					END AS xGoals_per_shot,
					CASE 
						WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
						WHEN RIGHT(shot_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(shot_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS shot_accuracy,
					CASE
						WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_on_target_per90 AS DECIMAL(5, 2))
					END AS shots_on_target_per90,
					CASE
						WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_outside_the_box_per90 AS DECIMAL(5, 2))
					END AS shots_outside_the_box_per90,
					CASE
						WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(goals_per90 AS DECIMAL(5, 2))
					END AS goals_per90,
					CASE
						WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_per90 AS DECIMAL(5, 2))
					END AS xGoals_per90,
					CASE
						WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(non_penalty_xGoals_per90 AS DECIMAL(5, 2))
					END AS non_penalty_xGoals_per90,
					CASE
						WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_overperformance AS DECIMAL(5, 2))
					END AS xGoals_overperformance,
					CASE 
						WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
						WHEN RIGHT(conversion_rate, 1)  = '%' THEN CAST((CAST(REPLACE(conversion_rate, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS conversion_rate,
					CASE 
						WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
						ELSE CAST(assists AS INT)
					END AS assists,
					CASE
						WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(assists_per90 AS DECIMAL(5, 2))
					END AS assists_per90,
					CASE
						WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(passes_attempted_per90 AS DECIMAL(5, 2))
					END AS passes_attempted_per90,
					CASE 
						WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
						WHEN RIGHT(pass_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(pass_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS pass_accuracy,
					CASE
						WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xAssits_per90 AS DECIMAL(5, 2))
					END AS xAssits_per90,
					CASE
						WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(open_play_key_passes_per90 AS DECIMAL(5, 2))
					END AS open_play_key_passes_per90,
					CASE
						WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(chances_created_per90 AS DECIMAL(5, 2))
					END AS chances_created_per90,
					CASE
						WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(dribbles_made_per90 AS DECIMAL(5, 2))
					END AS dribbles_made_per90,
					CASE
						WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(progressive_passes_per90 AS DECIMAL(5, 2))
					END AS progressive_passes_per90,
					CASE
						WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(open_play_crosses_attempted_per90 AS DECIMAL(5, 2))
					END AS open_play_crosses_attempted_per90,
					CASE 
						WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
						WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(open_play_cross_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS open_play_cross_accuracy,
					CASE
						WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(crosses_attempted_per90 AS DECIMAL(5, 2))
					END AS crosses_attempted_per90,
					CASE 
						WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
						WHEN RIGHT(cross_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(cross_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS cross_accuracy,
					CASE
						WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
						ELSE CAST(tackles_attempted AS DECIMAL(5, 2))
					END AS tackles_attempted,
					CASE 
						WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
						WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(tackle_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS tackle_accuracy,
					CASE
						WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(pressures_attempted_per90 AS DECIMAL(5, 2))
					END AS pressures_attempted_per90,
					CASE
						WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(pressures_completed_per90 AS DECIMAL(5, 2))
					END AS pressures_completed_per90,
					CASE
						WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(possession_won_per90 AS DECIMAL(5, 2))
					END AS possession_won_per90,
					CASE
						WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(possession_lost_per90 AS DECIMAL(5, 2))
					END AS possession_lost_per90,
					CASE
						WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(key_tackles_per90 AS DECIMAL(5, 2))
					END AS key_tackles_per90,
					CASE
						WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(interceptions_per90 AS DECIMAL(5, 2))
					END AS interceptions_per90,
					CASE
						WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(clearances_per90 AS DECIMAL(5, 2))
					END AS clearances_per90,
					CASE
						WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(blocks_per90 AS DECIMAL(5, 2))
					END AS blocks_per90,
					CASE
						WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_blocked_per90 AS DECIMAL(5, 2))
					END AS shots_blocked_per90,
					CASE
						WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(headers_attempted_per90 AS DECIMAL(5, 2))
					END AS headers_attempted_per90,
					CASE 
						WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
						WHEN RIGHT(heading_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(heading_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS heading_accuracy,
					CASE
						WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(key_headers_per90 AS DECIMAL(5, 2))
					END AS key_headers_per90,
					CASE
						WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(sprints_per90 AS DECIMAL(5, 2))
					END AS sprints_per90,
					CASE
						WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS DECIMAL(5, 2))
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
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM (
					SELECT * FROM bronze.fmdata_interested_out_players1
					UNION
					SELECT * FROM bronze.fmdata_interested_out_players2
					UNION
					SELECT * FROM bronze.fmdata_interested_out_players3
				) AS fmdata_interested_out_players
				WHERE player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_players) -- If present, ignore our players
				),
			cleaned_bronze_team AS(
				SELECT 
					CAST(player_id AS INT) AS player_id,
					TRIM(UPPER(club_name)) AS club_name,
					CASE 
						WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
						WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
					END AS minutes_played,
					CASE 
						WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS DECIMAL(5, 2))
						WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS DECIMAL(5, 2))
					END AS team_goals_scored_per90,
					CASE 
						WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
						WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
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
						WHEN shots_per90  = '-' THEN CAST(REPLACE(shots_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_per90 AS DECIMAL(5, 2))
					END AS shots_per90,
					CASE 
						WHEN xGoals_per_shot  = '-' THEN CAST(REPLACE(xGoals_per_shot, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_per_shot AS DECIMAL(5, 2))
					END AS xGoals_per_shot,
					CASE 
						WHEN shot_accuracy  = '-' THEN CAST(REPLACE(shot_accuracy, '-', '0') AS INT)
						WHEN RIGHT(shot_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(shot_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS shot_accuracy,
					CASE
						WHEN shots_on_target_per90 = '-' THEN CAST(REPLACE(shots_on_target_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_on_target_per90 AS DECIMAL(5, 2))
					END AS shots_on_target_per90,
					CASE
						WHEN shots_outside_the_box_per90 = '-' THEN CAST(REPLACE(shots_outside_the_box_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_outside_the_box_per90 AS DECIMAL(5, 2))
					END AS shots_outside_the_box_per90,
					CASE
						WHEN goals_per90 = '-' THEN CAST(REPLACE(goals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(goals_per90 AS DECIMAL(5, 2))
					END AS goals_per90,
					CASE
						WHEN xGoals_per90 = '-' THEN CAST(REPLACE(xGoals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_per90 AS DECIMAL(5, 2))
					END AS xGoals_per90,
					CASE
						WHEN non_penalty_xGoals_per90 = '-' THEN CAST(REPLACE(non_penalty_xGoals_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(non_penalty_xGoals_per90 AS DECIMAL(5, 2))
					END AS non_penalty_xGoals_per90,
					CASE
						WHEN xGoals_overperformance = '-' THEN CAST(REPLACE(xGoals_overperformance, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xGoals_overperformance AS DECIMAL(5, 2))
					END AS xGoals_overperformance,
					CASE 
						WHEN conversion_rate  = '-' THEN CAST(REPLACE(conversion_rate, '-', '0') AS INT)
						WHEN RIGHT(conversion_rate, 1)  = '%' THEN CAST((CAST(REPLACE(conversion_rate, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS conversion_rate,
					CASE 
						WHEN assists  = '-' THEN CAST(REPLACE(assists, '-', '0') AS INT)
						ELSE CAST(assists AS INT)
					END AS assists,
					CASE
						WHEN assists_per90 = '-' THEN CAST(REPLACE(assists_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(assists_per90 AS DECIMAL(5, 2))
					END AS assists_per90,
					CASE
						WHEN passes_attempted_per90 = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(passes_attempted_per90 AS DECIMAL(5, 2))
					END AS passes_attempted_per90,
					CASE 
						WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
						WHEN RIGHT(pass_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(pass_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS pass_accuracy,
					CASE
						WHEN xAssits_per90 = '-' THEN CAST(REPLACE(xAssits_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(xAssits_per90 AS DECIMAL(5, 2))
					END AS xAssits_per90,
					CASE
						WHEN open_play_key_passes_per90 = '-' THEN CAST(REPLACE(open_play_key_passes_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(open_play_key_passes_per90 AS DECIMAL(5, 2))
					END AS open_play_key_passes_per90,
					CASE
						WHEN chances_created_per90 = '-' THEN CAST(REPLACE(chances_created_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(chances_created_per90 AS DECIMAL(5, 2))
					END AS chances_created_per90,
					CASE
						WHEN dribbles_made_per90 = '-' THEN CAST(REPLACE(dribbles_made_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(dribbles_made_per90 AS DECIMAL(5, 2))
					END AS dribbles_made_per90,
					CASE
						WHEN progressive_passes_per90 = '-' THEN CAST(REPLACE(progressive_passes_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(progressive_passes_per90 AS DECIMAL(5, 2))
					END AS progressive_passes_per90,
					CASE
						WHEN open_play_crosses_attempted_per90 = '-' THEN CAST(REPLACE(open_play_crosses_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(open_play_crosses_attempted_per90 AS DECIMAL(5, 2))
					END AS open_play_crosses_attempted_per90,
					CASE 
						WHEN open_play_cross_accuracy  = '-' THEN CAST(REPLACE(open_play_cross_accuracy, '-', '0') AS INT)
						WHEN RIGHT(open_play_cross_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(open_play_cross_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS open_play_cross_accuracy,
					CASE
						WHEN crosses_attempted_per90 = '-' THEN CAST(REPLACE(crosses_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(crosses_attempted_per90 AS DECIMAL(5, 2))
					END AS crosses_attempted_per90,
					CASE 
						WHEN cross_accuracy  = '-' THEN CAST(REPLACE(cross_accuracy, '-', '0') AS INT)
						WHEN RIGHT(cross_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(cross_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS cross_accuracy,
					CASE
						WHEN tackles_attempted = '-' THEN CAST(REPLACE(tackles_attempted, '-', '0') AS INT)
						ELSE CAST(tackles_attempted AS DECIMAL(5, 2))
					END AS tackles_attempted,
					CASE 
						WHEN tackle_accuracy  = '-' THEN CAST(REPLACE(tackle_accuracy, '-', '0') AS INT)
						WHEN RIGHT(tackle_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(tackle_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS tackle_accuracy,
					CASE
						WHEN pressures_attempted_per90 = '-' THEN CAST(REPLACE(pressures_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(pressures_attempted_per90 AS DECIMAL(5, 2))
					END AS pressures_attempted_per90,
					CASE
						WHEN pressures_completed_per90 = '-' THEN CAST(REPLACE(pressures_completed_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(pressures_completed_per90 AS DECIMAL(5, 2))
					END AS pressures_completed_per90,
					CASE
						WHEN possession_won_per90 = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(possession_won_per90 AS DECIMAL(5, 2))
					END AS possession_won_per90,
					CASE
						WHEN possession_lost_per90 = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(possession_lost_per90 AS DECIMAL(5, 2))
					END AS possession_lost_per90,
					CASE
						WHEN key_tackles_per90 = '-' THEN CAST(REPLACE(key_tackles_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(key_tackles_per90 AS DECIMAL(5, 2))
					END AS key_tackles_per90,
					CASE
						WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(interceptions_per90 AS DECIMAL(5, 2))
					END AS interceptions_per90,
					CASE
						WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(clearances_per90 AS DECIMAL(5, 2))
					END AS clearances_per90,
					CASE
						WHEN blocks_per90 = '-' THEN CAST(REPLACE(blocks_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(blocks_per90 AS DECIMAL(5, 2))
					END AS blocks_per90,
					CASE
						WHEN shots_blocked_per90 = '-' THEN CAST(REPLACE(shots_blocked_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(shots_blocked_per90 AS DECIMAL(5, 2))
					END AS shots_blocked_per90,
					CASE
						WHEN headers_attempted_per90 = '-' THEN CAST(REPLACE(headers_attempted_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(headers_attempted_per90 AS DECIMAL(5, 2))
					END AS headers_attempted_per90,
					CASE 
						WHEN heading_accuracy  = '-' THEN CAST(REPLACE(heading_accuracy, '-', '0') AS INT)
						WHEN RIGHT(heading_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(heading_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
					END AS heading_accuracy,
					CASE
						WHEN key_headers_per90 = '-' THEN CAST(REPLACE(key_headers_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(key_headers_per90 AS DECIMAL(5, 2))
					END AS key_headers_per90,
					CASE
						WHEN sprints_per90 = '-' THEN CAST(REPLACE(sprints_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(sprints_per90 AS DECIMAL(5, 2))
					END AS sprints_per90,
					CASE
						WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS DECIMAL(5, 2))
						ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS DECIMAL(5, 2))
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
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM bronze.fmdata_team_players
			),
			cleaned AS (
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
					CASE WHEN A.tackles_attempted = 0 THEN 0
						ELSE CAST((A.tackles_attempted/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS tackles_attempted_per90,
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
					CASE WHEN A.mistakes_leading_to_goals = 0 THEN 0
						ELSE CAST((A.mistakes_leading_to_goals/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS mistakes_leading_to_goals_per90,
					CASE WHEN A.fouls_made = 0 THEN 0
						ELSE CAST((A.fouls_made/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS fouls_made_per90,
					CASE WHEN A.fouls_against = 0 THEN 0
						ELSE CAST((A.fouls_against/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS fouls_against_per90,
					CASE WHEN A.yellow_cards = 0 THEN 0
						ELSE CAST((A.yellow_cards/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS yellow_cards_per90,
					CASE WHEN A.red_cards = 0 THEN 0
						ELSE CAST((A.red_cards/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS red_cards_per90,
					A.dwh_create_date,
					A.dwh_cd_valid_till,
					A.dwh_current_validity
				FROM cleaned_bronze AS A
				INNER JOIN silver.dim_player_info AS B
					ON A.player_id = B.player_id
				INNER JOIN silver.dim_team_info AS C
					ON A.club_name = C.club_name
				WHERE A.player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_players) -- If present, ignore our players
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
					CASE WHEN A.tackles_attempted = 0 THEN 0
						ELSE CAST((A.tackles_attempted/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS tackles_attempted_per90,
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
					CASE WHEN A.mistakes_leading_to_goals = 0 THEN 0
						ELSE CAST((A.mistakes_leading_to_goals/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS mistakes_leading_to_goals_per90,
					CASE WHEN A.fouls_made = 0 THEN 0
						ELSE CAST((A.fouls_made/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS fouls_made_per90,
					CASE WHEN A.fouls_against = 0 THEN 0
						ELSE CAST((A.fouls_against/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS fouls_against_per90,
					CASE WHEN A.yellow_cards = 0 THEN 0
						ELSE CAST((A.yellow_cards/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS yellow_cards_per90,
					CASE WHEN A.red_cards = 0 THEN 0
						ELSE CAST((A.red_cards/A.minutes_played * 90) AS DECIMAL(5, 2))
					END AS red_cards_per90,
					A.dwh_create_date,
					A.dwh_cd_valid_till,
					A.dwh_current_validity
				FROM cleaned_bronze_team AS A
				INNER JOIN silver.dim_player_info AS B
					ON A.player_id = B.player_id
				INNER JOIN silver.dim_team_info AS C
					ON A.club_name = C.club_name
				)
			INSERT INTO silver.fact_outfield_player_stats (
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
			SELECT 
				player_key,
				team_key,
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
				tackles_attempted_per90,
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
				mistakes_leading_to_goals_per90,
				fouls_made_per90,
				fouls_against_per90,
				yellow_cards_per90,
				red_cards_per90,
				dwh_create_date,
				dwh_cd_valid_till,
				dwh_current_validity
			FROM cleaned AS S
			WHERE NOT EXISTS (
				SELECT 1
				FROM silver.fact_outfield_player_stats AS N
				WHERE N.player_key = S.player_key
					AND S.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--SILVER.FACT_GK_STATS
		PRINT '=============================================';
		PRINT 'SILVER.FACT_GK_STATS';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_gks';
		PRINT 'LOADING DATA FROM: bronze.fmdata_team_gks';
		PRINT 'LOADING DATA FROM: silver.dim_team_info';
		PRINT 'LOADING DATA FROM: silver.dim_player_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in SILVER.FACT_GK_STATS';
			WITH cleaned_bronze AS (
					SELECT
						CAST(player_id AS VARCHAR) AS player_id,
						TRIM(UPPER(club_name)) AS club_name,
						CASE 
							WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
							WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
						END AS minutes_played,
						CASE 
							WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS DECIMAL(5, 2))
						END AS team_goals_scored_per90,
						CASE 
							WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
						END AS team_goals_conceded_per90,
						CASE 
							WHEN goals_conceded_per90  = '-' THEN CAST(REPLACE(goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN goals_conceded_per90  != '-' THEN CAST(REPLACE(goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
						END AS goals_conceded_per90,
						CASE 
							WHEN saves_made_per90  = '-' THEN CAST(REPLACE(saves_made_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN saves_made_per90  != '-' THEN CAST(REPLACE(saves_made_per90, ',', '') AS DECIMAL(5, 2))
						END AS saves_made_per90,
						CASE 
							WHEN xGoals_prevented_per90  = '-' THEN CAST(REPLACE(xGoals_prevented_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN xGoals_prevented_per90  != '-' THEN CAST(REPLACE(xGoals_prevented_per90, ',', '') AS DECIMAL(5, 2))
						END AS xGoals_prevented_per90,
						CASE 
							WHEN xSave_rate  = '-' THEN CAST(REPLACE(xSave_rate, '-', '0') AS INT)
							WHEN RIGHT(xSave_rate, 1)  = '%' THEN (CAST(REPLACE(xSave_rate, '%', '') AS DECIMAL(5, 2))/100)
						END AS xSave_rate,
						CASE
							WHEN saves_tipped = '-' THEN CAST(REPLACE(saves_tipped, '-', '0') AS INT)
							ELSE CAST(saves_tipped AS DECIMAL(5, 2))
						END AS saves_tipped,
						CASE
							WHEN saves_parried = '-' THEN CAST(REPLACE(saves_parried, '-', '0') AS INT)
							ELSE CAST(saves_parried AS DECIMAL(5, 2))
						END AS saves_parried,
						CASE
							WHEN saves_held = '-' THEN CAST(REPLACE(saves_held, '-', '0') AS INT)
							ELSE CAST(saves_held AS DECIMAL(5, 2))
						END AS saves_held,
						CASE 
							WHEN saves_percentage  = '-' THEN CAST(REPLACE(saves_percentage, '-', '0') AS INT)
							WHEN RIGHT(saves_percentage, 1)  = '%' THEN (CAST(REPLACE(saves_percentage, '%', '') AS DECIMAL(5, 2))/100)
						END AS saves_percentage,
						CASE 
							WHEN passes_attempted_per90  = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN passes_attempted_per90  != '-' THEN CAST(REPLACE(passes_attempted_per90, ',', '') AS DECIMAL(5, 2))
						END AS passes_attempted_per90,
						CASE 
							WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
							WHEN RIGHT(pass_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(pass_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
						END AS pass_accuracy,
						CASE 
							WHEN possession_won_per90  = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN possession_won_per90  != '-' THEN CAST(REPLACE(possession_won_per90, ',', '') AS DECIMAL(5, 2))
						END AS possession_won_per90,
						CASE 
							WHEN possession_lost_per90  = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN possession_lost_per90  != '-' THEN CAST(REPLACE(possession_lost_per90, ',', '') AS DECIMAL(5, 2))
						END AS possession_lost_per90,
						CASE
							WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(interceptions_per90 AS DECIMAL(5, 2))
						END AS interceptions_per90,
						CASE
							WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(clearances_per90 AS DECIMAL(5, 2))
						END AS clearances_per90,
						CASE
								WHEN penalties_faced = '-' THEN CAST(REPLACE(penalties_faced, '-', '0') AS INT)
								ELSE CAST(penalties_faced AS DECIMAL(5, 2))
							END AS penalties_faced,
						CASE 
							WHEN penalties_save_percentage  = '-' THEN CAST(REPLACE(penalties_save_percentage, '-', '0') AS INT)
							WHEN RIGHT(penalties_save_percentage, 1)  = '%' THEN (CAST(REPLACE(penalties_save_percentage, '%', '') AS DECIMAL(5, 2))/100)
						END AS penalties_save_percentage,
						CASE
							WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS DECIMAL(5, 2))
						END AS distance_covered_km_per90,
						CASE
							WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
							ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
						END AS mistakes_leading_to_goals,
						CASE
							WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
							ELSE CAST(fouls_against AS DECIMAL(4,2))
						END AS fouls_against,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM bronze.fmdata_interested_gks
				),
				cleaned_bronze_team AS (
					SELECT
						CAST(player_id AS VARCHAR) AS player_id,
						TRIM(UPPER(club_name)) AS club_name,
						CASE 
							WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
							WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
						END AS minutes_played,
						CASE 
							WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS DECIMAL(5, 2))
						END AS team_goals_scored_per90,
						CASE 
							WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
						END AS team_goals_conceded_per90,
						CASE 
							WHEN goals_conceded_per90  = '-' THEN CAST(REPLACE(goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN goals_conceded_per90  != '-' THEN CAST(REPLACE(goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
						END AS goals_conceded_per90,
						CASE 
							WHEN saves_made_per90  = '-' THEN CAST(REPLACE(saves_made_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN saves_made_per90  != '-' THEN CAST(REPLACE(saves_made_per90, ',', '') AS DECIMAL(5, 2))
						END AS saves_made_per90,
						CASE 
							WHEN xGoals_prevented_per90  = '-' THEN CAST(REPLACE(xGoals_prevented_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN xGoals_prevented_per90  != '-' THEN CAST(REPLACE(xGoals_prevented_per90, ',', '') AS DECIMAL(5, 2))
						END AS xGoals_prevented_per90,
						CASE 
							WHEN xSave_rate  = '-' THEN CAST(REPLACE(xSave_rate, '-', '0') AS INT)
							WHEN RIGHT(xSave_rate, 1)  = '%' THEN (CAST(REPLACE(xSave_rate, '%', '') AS DECIMAL(5, 2))/100)
						END AS xSave_rate,
						CASE
							WHEN saves_tipped = '-' THEN CAST(REPLACE(saves_tipped, '-', '0') AS INT)
							ELSE CAST(saves_tipped AS DECIMAL(5, 2))
						END AS saves_tipped,
						CASE
							WHEN saves_parried = '-' THEN CAST(REPLACE(saves_parried, '-', '0') AS INT)
							ELSE CAST(saves_parried AS DECIMAL(5, 2))
						END AS saves_parried,
						CASE
							WHEN saves_held = '-' THEN CAST(REPLACE(saves_held, '-', '0') AS INT)
							ELSE CAST(saves_held AS DECIMAL(5, 2))
						END AS saves_held,
						CASE 
							WHEN saves_percentage  = '-' THEN CAST(REPLACE(saves_percentage, '-', '0') AS INT)
							WHEN RIGHT(saves_percentage, 1)  = '%' THEN (CAST(REPLACE(saves_percentage, '%', '') AS DECIMAL(5, 2))/100)
						END AS saves_percentage,
						CASE 
							WHEN passes_attempted_per90  = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN passes_attempted_per90  != '-' THEN CAST(REPLACE(passes_attempted_per90, ',', '') AS DECIMAL(5, 2))
						END AS passes_attempted_per90,
						CASE 
							WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
							WHEN RIGHT(pass_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(pass_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
						END AS pass_accuracy,
						CASE 
							WHEN possession_won_per90  = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN possession_won_per90  != '-' THEN CAST(REPLACE(possession_won_per90, ',', '') AS DECIMAL(5, 2))
						END AS possession_won_per90,
						CASE 
							WHEN possession_lost_per90  = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN possession_lost_per90  != '-' THEN CAST(REPLACE(possession_lost_per90, ',', '') AS DECIMAL(5, 2))
						END AS possession_lost_per90,
						CASE
							WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(interceptions_per90 AS DECIMAL(5, 2))
						END AS interceptions_per90,
						CASE
							WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(clearances_per90 AS DECIMAL(5, 2))
						END AS clearances_per90,
						CASE
								WHEN penalties_faced = '-' THEN CAST(REPLACE(penalties_faced, '-', '0') AS INT)
								ELSE CAST(penalties_faced AS DECIMAL(5, 2))
							END AS penalties_faced,
						CASE 
							WHEN penalties_save_percentage  = '-' THEN CAST(REPLACE(penalties_save_percentage, '-', '0') AS INT)
							WHEN RIGHT(penalties_save_percentage, 1)  = '%' THEN (CAST(REPLACE(penalties_save_percentage, '%', '') AS DECIMAL(5, 2))/100)
						END AS penalties_save_percentage,
						CASE
							WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS DECIMAL(5, 2))
						END AS distance_covered_km_per90,
						CASE
							WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
							ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
						END AS mistakes_leading_to_goals,
						CASE
							WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
							ELSE CAST(fouls_against AS DECIMAL(4,2))
						END AS fouls_against,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM bronze.fmdata_team_gks
				),
				cleaned AS (
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
						CASE WHEN A.saves_tipped = 0 THEN 0
							ELSE CAST((A.saves_tipped/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_tipped_per90,
						CASE WHEN A.saves_parried = 0 THEN 0
							ELSE CAST((A.saves_parried/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_parried_per90,
						CASE WHEN A.saves_held = 0 THEN 0
							ELSE CAST((A.saves_held/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_held_per90,
						A.saves_percentage,
						A.passes_attempted_per90,
						A.pass_accuracy,
						A.possession_won_per90,
						A.possession_lost_per90,
						A.interceptions_per90,
						A.clearances_per90,
						CASE WHEN penalties_faced = 0 THEN 0
							ELSE CAST((penalties_faced/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS penalties_faced_per90,
						A.penalties_save_percentage,
						A.distance_covered_km_per90,
						CASE WHEN A.mistakes_leading_to_goals = 0 THEN 0
							ELSE CAST((A.mistakes_leading_to_goals/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS mistakes_leading_to_goals_per90,
						CASE WHEN A.fouls_against = 0 THEN 0
							ELSE CAST((A.fouls_against/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS fouls_against_per90,
						A.dwh_create_date,
						A.dwh_cd_valid_till,
						A.dwh_current_validity
					FROM cleaned_bronze AS A
					INNER JOIN silver.dim_player_info AS B
						ON A.player_id = B.player_id
					INNER JOIN silver.dim_team_info AS C
						ON A.club_name = C.club_name
					WHERE A.player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_gks) -- If present, ignore our players
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
						CASE WHEN A.saves_tipped = 0 THEN 0
							ELSE CAST((A.saves_tipped/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_tipped_per90,
						CASE WHEN A.saves_parried = 0 THEN 0
							ELSE CAST((A.saves_parried/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_parried_per90,
						CASE WHEN A.saves_held = 0 THEN 0
							ELSE CAST((A.saves_held/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_held_per90,
						A.saves_percentage,
						A.passes_attempted_per90,
						A.pass_accuracy,
						A.possession_won_per90,
						A.possession_lost_per90,
						A.interceptions_per90,
						A.clearances_per90,
						CASE WHEN penalties_faced = 0 THEN 0
							ELSE CAST((penalties_faced/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS penalties_faced_per90,
						A.penalties_save_percentage,
						A.distance_covered_km_per90,
						CASE WHEN A.mistakes_leading_to_goals = 0 THEN 0
							ELSE CAST((A.mistakes_leading_to_goals/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS mistakes_leading_to_goals_per90,
						CASE WHEN A.fouls_against = 0 THEN 0
							ELSE CAST((A.fouls_against/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS fouls_against_per90,
						A.dwh_create_date,
						A.dwh_cd_valid_till,
						A.dwh_current_validity
					FROM cleaned_bronze_team AS A
					INNER JOIN silver.dim_player_info AS B
						ON A.player_id = B.player_id
					INNER JOIN silver.dim_team_info AS C
						ON A.club_name = C.club_name
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM silver.fact_gk_stats AS S
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
		PRINT '>>> Inserting New Records in SILVER.FACT_GK_STATS';
			WITH cleaned_bronze AS (
					SELECT
						CAST(player_id AS VARCHAR) AS player_id,
						TRIM(UPPER(club_name)) AS club_name,
						CASE 
							WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
							WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
						END AS minutes_played,
						CASE 
							WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS DECIMAL(5, 2))
						END AS team_goals_scored_per90,
						CASE 
							WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
						END AS team_goals_conceded_per90,
						CASE 
							WHEN goals_conceded_per90  = '-' THEN CAST(REPLACE(goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN goals_conceded_per90  != '-' THEN CAST(REPLACE(goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
						END AS goals_conceded_per90,
						CASE 
							WHEN saves_made_per90  = '-' THEN CAST(REPLACE(saves_made_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN saves_made_per90  != '-' THEN CAST(REPLACE(saves_made_per90, ',', '') AS DECIMAL(5, 2))
						END AS saves_made_per90,
						CASE 
							WHEN xGoals_prevented_per90  = '-' THEN CAST(REPLACE(xGoals_prevented_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN xGoals_prevented_per90  != '-' THEN CAST(REPLACE(xGoals_prevented_per90, ',', '') AS DECIMAL(5, 2))
						END AS xGoals_prevented_per90,
						CASE 
							WHEN xSave_rate  = '-' THEN CAST(REPLACE(xSave_rate, '-', '0') AS INT)
							WHEN RIGHT(xSave_rate, 1)  = '%' THEN (CAST(REPLACE(xSave_rate, '%', '') AS DECIMAL(5, 2))/100)
						END AS xSave_rate,
						CASE
							WHEN saves_tipped = '-' THEN CAST(REPLACE(saves_tipped, '-', '0') AS INT)
							ELSE CAST(saves_tipped AS DECIMAL(5, 2))
						END AS saves_tipped,
						CASE
							WHEN saves_parried = '-' THEN CAST(REPLACE(saves_parried, '-', '0') AS INT)
							ELSE CAST(saves_parried AS DECIMAL(5, 2))
						END AS saves_parried,
						CASE
							WHEN saves_held = '-' THEN CAST(REPLACE(saves_held, '-', '0') AS INT)
							ELSE CAST(saves_held AS DECIMAL(5, 2))
						END AS saves_held,
						CASE 
							WHEN saves_percentage  = '-' THEN CAST(REPLACE(saves_percentage, '-', '0') AS INT)
							WHEN RIGHT(saves_percentage, 1)  = '%' THEN (CAST(REPLACE(saves_percentage, '%', '') AS DECIMAL(5, 2))/100)
						END AS saves_percentage,
						CASE 
							WHEN passes_attempted_per90  = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN passes_attempted_per90  != '-' THEN CAST(REPLACE(passes_attempted_per90, ',', '') AS DECIMAL(5, 2))
						END AS passes_attempted_per90,
						CASE 
							WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
							WHEN RIGHT(pass_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(pass_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
						END AS pass_accuracy,
						CASE 
							WHEN possession_won_per90  = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN possession_won_per90  != '-' THEN CAST(REPLACE(possession_won_per90, ',', '') AS DECIMAL(5, 2))
						END AS possession_won_per90,
						CASE 
							WHEN possession_lost_per90  = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN possession_lost_per90  != '-' THEN CAST(REPLACE(possession_lost_per90, ',', '') AS DECIMAL(5, 2))
						END AS possession_lost_per90,
						CASE
							WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(interceptions_per90 AS DECIMAL(5, 2))
						END AS interceptions_per90,
						CASE
							WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(clearances_per90 AS DECIMAL(5, 2))
						END AS clearances_per90,
						CASE
								WHEN penalties_faced = '-' THEN CAST(REPLACE(penalties_faced, '-', '0') AS INT)
								ELSE CAST(penalties_faced AS DECIMAL(5, 2))
							END AS penalties_faced,
						CASE 
							WHEN penalties_save_percentage  = '-' THEN CAST(REPLACE(penalties_save_percentage, '-', '0') AS INT)
							WHEN RIGHT(penalties_save_percentage, 1)  = '%' THEN (CAST(REPLACE(penalties_save_percentage, '%', '') AS DECIMAL(5, 2))/100)
						END AS penalties_save_percentage,
						CASE
							WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS DECIMAL(5, 2))
						END AS distance_covered_km_per90,
						CASE
							WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
							ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
						END AS mistakes_leading_to_goals,
						CASE
							WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
							ELSE CAST(fouls_against AS DECIMAL(4,2))
						END AS fouls_against,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM bronze.fmdata_interested_gks
				),
				cleaned_bronze_team AS (
					SELECT
						CAST(player_id AS VARCHAR) AS player_id,
						TRIM(UPPER(club_name)) AS club_name,
						CASE 
							WHEN minutes_played  = '-' THEN CAST(REPLACE(minutes_played, '-', '0') AS INT)
							WHEN minutes_played  != '-' THEN CAST(REPLACE(minutes_played, ',', '') AS INT)
						END AS minutes_played,
						CASE 
							WHEN team_goals_scored_per90  = '-' THEN CAST(REPLACE(team_goals_scored_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN team_goals_scored_per90  != '-' THEN CAST(REPLACE(team_goals_scored_per90, ',', '') AS DECIMAL(5, 2))
						END AS team_goals_scored_per90,
						CASE 
							WHEN team_goals_conceded_per90  = '-' THEN CAST(REPLACE(team_goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN team_goals_conceded_per90  != '-' THEN CAST(REPLACE(team_goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
						END AS team_goals_conceded_per90,
						CASE 
							WHEN goals_conceded_per90  = '-' THEN CAST(REPLACE(goals_conceded_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN goals_conceded_per90  != '-' THEN CAST(REPLACE(goals_conceded_per90, ',', '') AS DECIMAL(5, 2))
						END AS goals_conceded_per90,
						CASE 
							WHEN saves_made_per90  = '-' THEN CAST(REPLACE(saves_made_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN saves_made_per90  != '-' THEN CAST(REPLACE(saves_made_per90, ',', '') AS DECIMAL(5, 2))
						END AS saves_made_per90,
						CASE 
							WHEN xGoals_prevented_per90  = '-' THEN CAST(REPLACE(xGoals_prevented_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN xGoals_prevented_per90  != '-' THEN CAST(REPLACE(xGoals_prevented_per90, ',', '') AS DECIMAL(5, 2))
						END AS xGoals_prevented_per90,
						CASE 
							WHEN xSave_rate  = '-' THEN CAST(REPLACE(xSave_rate, '-', '0') AS INT)
							WHEN RIGHT(xSave_rate, 1)  = '%' THEN (CAST(REPLACE(xSave_rate, '%', '') AS DECIMAL(5, 2))/100)
						END AS xSave_rate,
						CASE
							WHEN saves_tipped = '-' THEN CAST(REPLACE(saves_tipped, '-', '0') AS INT)
							ELSE CAST(saves_tipped AS DECIMAL(5, 2))
						END AS saves_tipped,
						CASE
							WHEN saves_parried = '-' THEN CAST(REPLACE(saves_parried, '-', '0') AS INT)
							ELSE CAST(saves_parried AS DECIMAL(5, 2))
						END AS saves_parried,
						CASE
							WHEN saves_held = '-' THEN CAST(REPLACE(saves_held, '-', '0') AS INT)
							ELSE CAST(saves_held AS DECIMAL(5, 2))
						END AS saves_held,
						CASE 
							WHEN saves_percentage  = '-' THEN CAST(REPLACE(saves_percentage, '-', '0') AS INT)
							WHEN RIGHT(saves_percentage, 1)  = '%' THEN (CAST(REPLACE(saves_percentage, '%', '') AS DECIMAL(5, 2))/100)
						END AS saves_percentage,
						CASE 
							WHEN passes_attempted_per90  = '-' THEN CAST(REPLACE(passes_attempted_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN passes_attempted_per90  != '-' THEN CAST(REPLACE(passes_attempted_per90, ',', '') AS DECIMAL(5, 2))
						END AS passes_attempted_per90,
						CASE 
							WHEN pass_accuracy  = '-' THEN CAST(REPLACE(pass_accuracy, '-', '0') AS INT)
							WHEN RIGHT(pass_accuracy, 1)  = '%' THEN CAST((CAST(REPLACE(pass_accuracy, '%', '') AS DECIMAL(5, 2))/100) AS DECIMAL(5, 2))
						END AS pass_accuracy,
						CASE 
							WHEN possession_won_per90  = '-' THEN CAST(REPLACE(possession_won_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN possession_won_per90  != '-' THEN CAST(REPLACE(possession_won_per90, ',', '') AS DECIMAL(5, 2))
						END AS possession_won_per90,
						CASE 
							WHEN possession_lost_per90  = '-' THEN CAST(REPLACE(possession_lost_per90, '-', '0') AS DECIMAL(5, 2))
							WHEN possession_lost_per90  != '-' THEN CAST(REPLACE(possession_lost_per90, ',', '') AS DECIMAL(5, 2))
						END AS possession_lost_per90,
						CASE
							WHEN interceptions_per90 = '-' THEN CAST(REPLACE(interceptions_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(interceptions_per90 AS DECIMAL(5, 2))
						END AS interceptions_per90,
						CASE
							WHEN clearances_per90 = '-' THEN CAST(REPLACE(clearances_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(clearances_per90 AS DECIMAL(5, 2))
						END AS clearances_per90,
						CASE
								WHEN penalties_faced = '-' THEN CAST(REPLACE(penalties_faced, '-', '0') AS INT)
								ELSE CAST(penalties_faced AS DECIMAL(5, 2))
							END AS penalties_faced,
						CASE 
							WHEN penalties_save_percentage  = '-' THEN CAST(REPLACE(penalties_save_percentage, '-', '0') AS INT)
							WHEN RIGHT(penalties_save_percentage, 1)  = '%' THEN (CAST(REPLACE(penalties_save_percentage, '%', '') AS DECIMAL(5, 2))/100)
						END AS penalties_save_percentage,
						CASE
							WHEN distance_covered_per90 = '-' THEN CAST(REPLACE(distance_covered_per90, '-', '0') AS DECIMAL(5, 2))
							ELSE CAST(REPLACE(distance_covered_per90, 'km', '') AS DECIMAL(5, 2))
						END AS distance_covered_km_per90,
						CASE
							WHEN mistakes_leading_to_goals = '-' THEN CAST(REPLACE(mistakes_leading_to_goals, '-', '0') AS INT)
							ELSE CAST(mistakes_leading_to_goals AS DECIMAL(4,2))
						END AS mistakes_leading_to_goals,
						CASE
							WHEN fouls_against = '-' THEN CAST(REPLACE(fouls_against, '-', '0') AS INT)
							ELSE CAST(fouls_against AS DECIMAL(4,2))
						END AS fouls_against,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM bronze.fmdata_team_gks
				),
				cleaned AS (
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
						CASE WHEN A.saves_tipped = 0 THEN 0
							ELSE CAST((A.saves_tipped/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_tipped_per90,
						CASE WHEN A.saves_parried = 0 THEN 0
							ELSE CAST((A.saves_parried/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_parried_per90,
						CASE WHEN A.saves_held = 0 THEN 0
							ELSE CAST((A.saves_held/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_held_per90,
						A.saves_percentage,
						A.passes_attempted_per90,
						A.pass_accuracy,
						A.possession_won_per90,
						A.possession_lost_per90,
						A.interceptions_per90,
						A.clearances_per90,
						CASE WHEN penalties_faced = 0 THEN 0
							ELSE CAST((penalties_faced/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS penalties_faced_per90,
						A.penalties_save_percentage,
						A.distance_covered_km_per90,
						CASE WHEN A.mistakes_leading_to_goals = 0 THEN 0
							ELSE CAST((A.mistakes_leading_to_goals/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS mistakes_leading_to_goals_per90,
						CASE WHEN A.fouls_against = 0 THEN 0
							ELSE CAST((A.fouls_against/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS fouls_against_per90,
						A.dwh_create_date,
						A.dwh_cd_valid_till,
						A.dwh_current_validity
					FROM cleaned_bronze AS A
					INNER JOIN silver.dim_player_info AS B
						ON A.player_id = B.player_id
					INNER JOIN silver.dim_team_info AS C
						ON A.club_name = C.club_name
					WHERE A.player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_gks) -- If present, ignore our players
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
						CASE WHEN A.saves_tipped = 0 THEN 0
							ELSE CAST((A.saves_tipped/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_tipped_per90,
						CASE WHEN A.saves_parried = 0 THEN 0
							ELSE CAST((A.saves_parried/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_parried_per90,
						CASE WHEN A.saves_held = 0 THEN 0
							ELSE CAST((A.saves_held/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS saves_held_per90,
						A.saves_percentage,
						A.passes_attempted_per90,
						A.pass_accuracy,
						A.possession_won_per90,
						A.possession_lost_per90,
						A.interceptions_per90,
						A.clearances_per90,
						CASE WHEN penalties_faced = 0 THEN 0
							ELSE CAST((penalties_faced/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS penalties_faced_per90,
						A.penalties_save_percentage,
						A.distance_covered_km_per90,
						CASE WHEN A.mistakes_leading_to_goals = 0 THEN 0
							ELSE CAST((A.mistakes_leading_to_goals/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS mistakes_leading_to_goals_per90,
						CASE WHEN A.fouls_against = 0 THEN 0
							ELSE CAST((A.fouls_against/A.minutes_played * 90) AS DECIMAL(5, 2))
						END AS fouls_against_per90,
						A.dwh_create_date,
						A.dwh_cd_valid_till,
						A.dwh_current_validity
					FROM cleaned_bronze_team AS A
					INNER JOIN silver.dim_player_info AS B
						ON A.player_id = B.player_id
					INNER JOIN silver.dim_team_info AS C
						ON A.club_name = C.club_name
				)
			INSERT INTO silver.fact_gk_stats (
				player_key, team_key, minutes_played, team_goals_scored_per90, team_goals_conceded_per90,	 goals_conceded_per90,	
				saves_made_per90, xGoals_prevented_per90, xSave_rate, saves_tipped_per90, saves_parried_per90, saves_held_per90,
				saves_percentage, passes_attempted_per90, pass_accuracy, possession_won_per90, possession_lost_per90,
				interceptions_per90, clearances_per90, penalties_faced_per90, penalties_save_percentage, distance_covered_km_per90,
				mistakes_leading_to_goals_per90, fouls_against_per90, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT
				player_key,
				team_key,
				minutes_played,
				team_goals_scored_per90,
				team_goals_conceded_per90,	
				goals_conceded_per90,	
				saves_made_per90,
				xGoals_prevented_per90,
				xSave_rate,
				saves_tipped_per90, 
				saves_parried_per90, 
				saves_held_per90,
				saves_percentage,
				passes_attempted_per90,
				pass_accuracy,
				possession_won_per90,
				possession_lost_per90,
				interceptions_per90,
				clearances_per90,
				penalties_faced_per90,
				penalties_save_percentage,
				distance_covered_km_per90,
				mistakes_leading_to_goals_per90,
				fouls_against_per90,
				dwh_create_date,
				dwh_cd_valid_till,
				dwh_current_validity
			FROM cleaned AS S
			WHERE NOT EXISTS (
				SELECT 1
				FROM silver.fact_gk_stats AS N
				WHERE N.player_key = S.player_key
					AND S.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--SILVER.FACT_PLAYERS_VALUE
		PRINT '=============================================';
		PRINT 'SILVER.FACT_PLAYERS_VALUE';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players1';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players2';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_players3';
		PRINT 'LOADING DATA FROM: bronze.fmdata_interested_out_gks';
		PRINT 'LOADING DATA FROM: bronze.fmdata_team_players';
		PRINT 'LOADING DATA FROM: bronze.fmdata_team_gks';
		PRINT 'LOADING DATA FROM: silver.dim_player_info';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Acknowledging Old Records in SILVER.FACT_PLAYERS_VALUE';
			WITH cleaned_bronze AS (
					SELECT
						player_id,
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
					FROM (
						SELECT * FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players2
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players3
					) AS fmdata_interested_out_players
					WHERE player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_players) -- If present, ignore our players
					UNION
					SELECT
						player_id,
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
					FROM bronze.fmdata_interested_gks
					WHERE player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_gks) -- If present, ignore our players
				),
				cleaned_bronze_team AS (
					SELECT
						player_id,
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
					UNION
					SELECT
						player_id,
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
				),
				cleaned AS (
					SELECT
						A.player_key,
						A.team_key,
						CAST(B.wage_per_week_k AS DECIMAL( 10, 2)) AS wage_per_week_k,
						CAST(B.transfer_value_m AS DECIMAL( 10, 2)) AS transfer_value_m
					FROM silver.dim_player_info AS A
					INNER JOIN cleaned_bronze AS B
						ON A.player_id = B.player_id
					UNION
					SELECT
						A.player_key,
						A.team_key,
						CAST(B.wage_per_week_k AS DECIMAL( 10, 2)) AS wage_per_week_k,
						CAST(B.transfer_value_m AS DECIMAL( 10, 2)) AS transfer_value_m
					FROM silver.dim_player_info AS A
					INNER JOIN cleaned_bronze_team AS B
						ON A.player_id = B.player_id
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM silver.fact_players_value AS S
			INNER JOIN cleaned AS N
				ON S.player_key = N.player_key
			WHERE S.dwh_current_validity = 1
				AND (
					S.team_key != N.team_key
					OR S.wage_per_week_k != N.wage_per_week_k
					OR S.transfer_value_m != N.transfer_value_m
				);

		PRINT '>>> Inserting New Records in SILVER.FACT_PLAYERS_VALUE';
			WITH cleaned_bronze AS (
					SELECT
						player_id,
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
					FROM (
						SELECT * FROM bronze.fmdata_interested_out_players1
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players2
						UNION
						SELECT * FROM bronze.fmdata_interested_out_players3
					) AS fmdata_interested_out_players
					WHERE player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_players) -- If present, ignore our players
					UNION
					SELECT
						player_id,
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
					FROM bronze.fmdata_interested_gks
					WHERE player_id NOT IN (SELECT player_id FROM bronze.fmdata_team_gks) -- If present, ignore our players
				),
				cleaned_bronze_team AS (
					SELECT
						player_id,
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
					UNION
					SELECT
						player_id,
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
				),
				cleaned AS (
					SELECT
						A.player_key,
						A.team_key,
						CAST(B.wage_per_week_k AS DECIMAL( 10, 2)) AS wage_per_week_k,
						CAST(B.transfer_value_m AS DECIMAL( 10, 2)) AS transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.dim_player_info AS A
					INNER JOIN cleaned_bronze AS B
						ON A.player_id = B.player_id
					UNION
					SELECT
						A.player_key,
						A.team_key,
						CAST(B.wage_per_week_k AS DECIMAL( 10, 2)) AS wage_per_week_k,
						CAST(B.transfer_value_m AS DECIMAL( 10, 2)) AS transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.dim_player_info AS A
					INNER JOIN cleaned_bronze_team AS B
						ON A.player_id = B.player_id
				)
			INSERT INTO silver.fact_players_value (
				player_key, team_key, wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT
				player_key,
				team_key,
				wage_per_week_k,
				transfer_value_m,
				dwh_create_date,
				dwh_cd_valid_till,
				dwh_current_validity
			FROM cleaned AS S
			WHERE NOT EXISTS (
				SELECT 1
				FROM silver.fact_players_value AS N
				WHERE N.player_key = S.player_key
					AND S.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=================================';
		PRINT 'LOADING OF SILVER LAYER COMPLETED';
		PRINT 'TOTAL DURATION: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
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