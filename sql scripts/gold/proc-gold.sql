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

CREATE OR ALTER PROCEDURE gold.load_gold
	@Season VARCHAR(9) = '2023/24'
AS
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
				SELECT DISTINCT TRIM(league) AS league
				FROM (
					SELECT league FROM silver.fmdata_team_players WHERE league IS NOT NULL AND TRIM(league) != '' AND dwh_current_validity = 1
					UNION
					SELECT league FROM silver.fmdata_team_gks WHERE league IS NOT NULL AND TRIM(league) != '' AND dwh_current_validity = 1
					UNION
					SELECT league FROM silver.fmdata_interested_out_players WHERE league IS NOT NULL AND TRIM(league) != '' AND dwh_current_validity = 1
					UNION
					SELECT league FROM silver.fmdata_interested_gks WHERE league IS NOT NULL AND TRIM(league) != '' AND dwh_current_validity = 1
				) AS all_leagues
			)
			INSERT INTO gold.dim_league (league, league_difficulty_coef)
			SELECT 
				league,
				CASE 
					-- =================================================================
					-- 1. APEX TIER: PREMIER LEAGUE (1.00)
					-- =================================================================
					WHEN league IN ('Premier League', 'English Premier Division', 'World Super League', 'WSL Premier League') THEN 1.00

					-- =================================================================
					-- 2. SUPER LEAGUE 1 (0.90)
					-- =================================================================
					WHEN league LIKE '%Brasileiro S%rie A%' OR league LIKE '%Brasileiro Serie A%' THEN 0.90
					WHEN league IN ('Bundesliga', '1. Bundesliga', 'German First Division') OR league LIKE '%Bundesliga' OR league = 'BUNDESLIGA' THEN 0.90
					WHEN league LIKE '%Championship%' THEN 0.90
					WHEN league IN ('La Liga', 'LaLiga', 'LALIGA EA SPORTS', 'Spanish First Division') OR league LIKE '%LaLiga' OR league = 'LALIGA' THEN 0.90
					WHEN league IN ('Ligue 1', 'Ligue 1 Uber Eats', 'Ligue 1 McDonald''s') OR league LIKE '%Ligue 1%' THEN 0.90
					WHEN league IN ('Serie A', 'Serie A TIM', 'Italian Serie A') OR league LIKE '%Serie A' OR league = 'SERIE A' THEN 0.90
					WHEN league LIKE '%A-League%' THEN 0.90
					WHEN league LIKE '%Eredivisie%' THEN 0.90
					WHEN league LIKE '%Major League Soccer%' OR league LIKE '%MLS%' THEN 0.90
					WHEN league LIKE '%Primeira Liga%' OR league LIKE '%Liga Portugal%' THEN 0.90
					WHEN league LIKE '%Primera Divisi%n%' OR league LIKE '%Primera Division%' OR league LIKE '%Superliga Argentina%' THEN 0.90
					WHEN league LIKE '%S%per Lig%' OR league LIKE '%Super Lig%' THEN 0.90

					-- =================================================================
					-- 3. SUPER LEAGUE 2 (0.70)
					-- =================================================================
					WHEN league LIKE '%Brasileiro S%rie B%' OR league LIKE '%Brasileiro Serie B%' THEN 0.70
					WHEN league LIKE '%Bundesliga 2%' OR league LIKE '%2. Bundesliga%' THEN 0.70
					WHEN league LIKE '%LaLiga 2%' OR league LIKE '%Segunda Divisi%n de Espa%a%' THEN 0.70
					WHEN league LIKE '%League One%' THEN 0.70
					WHEN league LIKE '%Ligue 2%' THEN 0.70
					WHEN league LIKE '%Serie B%' THEN 0.70
					WHEN league LIKE '%1. Lig%' OR league LIKE '%Lig (TUR)%' THEN 0.70
					WHEN league LIKE '%A2 League%' THEN 0.70
					WHEN league LIKE '%Allsvenskan%' THEN 0.70
					WHEN league LIKE '%Ekstraklasa%' THEN 0.70
					WHEN league LIKE '%Eliteserien%' THEN 0.70
					WHEN league LIKE '%Liga 2%' OR league LIKE '%Liga Portugal 2%' THEN 0.70
					WHEN league LIKE '%Liga MX%' THEN 0.70
					WHEN league LIKE '%Liga Prom%rica%' OR league LIKE '%Liga Promerica%' THEN 0.70
					WHEN league LIKE '%Prva Liga%' THEN 0.70
					WHEN league LIKE '%Segunda Divisi%n%' OR league LIKE '%Segunda Division%' THEN 0.70

					-- =================================================================
					-- 4. SUPER LEAGUE 3 (0.60)
					-- =================================================================
					WHEN league LIKE '%Brasileiro S%rie C%' OR league LIKE '%Brasileiro Serie C%' THEN 0.60
					WHEN league LIKE '%Bundesliga 3%' OR league LIKE '%3. Liga%' THEN 0.60
					WHEN league LIKE '%LaLiga 3%' OR league LIKE '%Primera Federaci%n%' THEN 0.60
					WHEN league LIKE '%League Two%' THEN 0.60
					WHEN league LIKE '%Ligue 3%' OR league LIKE '%National 1%' THEN 0.60
					WHEN league LIKE '%Serie C%' THEN 0.60
					WHEN league LIKE '%2. Lig%' OR league = '2. LIG' THEN 0.60
					WHEN league LIKE '%Liga 3%' OR league LIKE '%Liga Portugal 3%' THEN 0.60

					-- =================================================================
					-- 5. SUPER LEAGUE 4 (0.40)
					-- =================================================================
					WHEN league LIKE '%Brasileiro S%rie D%' OR league LIKE '%Brasileiro Serie D%' THEN 0.40
					WHEN league LIKE '%Bundesliga 4%' OR league LIKE '%Regionalliga%' THEN 0.40
					WHEN league LIKE '%LaLiga 4%' OR league LIKE '%Segunda Federaci%n%' THEN 0.40
					WHEN league LIKE '%Ligue 4%' OR league LIKE '%National 2%' THEN 0.40
					WHEN league IN ('National', 'National WSL', 'Vanarama National League', 'National League', 'English National League') OR league = 'NATIONAL LEAGUE' THEN 0.40
					WHEN league LIKE '%Serie D%' THEN 0.40
					WHEN league LIKE '%Tercera Divisi%n%' OR league LIKE '%Tercera Division%' THEN 0.40
					WHEN league LIKE '%Superettan%' THEN 0.40
					WHEN league LIKE '%Premier Liha%' THEN 0.40
					WHEN league LIKE '%Eerste Divisie%' THEN 0.40
					WHEN league LIKE '%A3 League%' THEN 0.40

					-- =================================================================
					-- 6. SUPER LEAGUE 5 (0.20)
					-- =================================================================
					WHEN league LIKE '%National%East%' OR league LIKE '%National League East%' THEN 0.20
					WHEN league LIKE '%National%North%' OR league LIKE '%National League North%' THEN 0.20
					WHEN league LIKE '%National%South%' OR league LIKE '%National League South%' THEN 0.20
					WHEN league LIKE '%National%West%' OR league LIKE '%National League West%' THEN 0.20

					-- =================================================================
					-- 7. DEFAULT FALLBACK
					-- =================================================================
					ELSE 0.60
				END AS league_difficulty_coef
			FROM dim_league AS N
			WHERE NOT EXISTS (
				SELECT 1
				FROM gold.dim_league AS O
				WHERE N.league = O.league
			);

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.DIM_CLUB
		PRINT '=============================================';
		PRINT 'GOLD.DIM_CLUB';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: ALL PLAYER SOURCES';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Inserting New Records in GOLD.DIM_CLUB';
			WITH dim_club AS (
				SELECT DISTINCT TRIM(club_name) AS club_name
				FROM (
					SELECT club_name FROM silver.fmdata_team_players WHERE club_name IS NOT NULL AND TRIM(club_name) != '' AND dwh_current_validity = 1
					UNION
					SELECT club_name FROM silver.fmdata_team_gks WHERE club_name IS NOT NULL AND TRIM(club_name) != '' AND dwh_current_validity = 1
					UNION
					SELECT club_name FROM silver.fmdata_interested_out_players WHERE club_name IS NOT NULL AND TRIM(club_name) != '' AND dwh_current_validity = 1
					UNION
					SELECT club_name FROM silver.fmdata_interested_gks WHERE club_name IS NOT NULL AND TRIM(club_name) != '' AND dwh_current_validity = 1
				) AS all_clubs
			)
			INSERT INTO gold.dim_club (club)
			SELECT club_name FROM dim_club AS N
			WHERE NOT EXISTS (
				SELECT 1
				FROM gold.dim_club AS O
				WHERE N.club_name = O.club
			);

		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

	--GOLD.DIM_TEAM_INFO
		PRINT '=============================================';
		PRINT 'GOLD.DIM_TEAM_INFO';
		PRINT '---------------------------------------------';
		PRINT 'LOADING DATA FROM: ALL PLAYER SOURCES, gold.dim_league, gold.dim_club';
		PRINT '---------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>> Inserting / Synchronizing Records in GOLD.DIM_TEAM_INFO';
			WITH source_teams AS (
				SELECT DISTINCT TRIM(club_name) AS club_name, TRIM(league) AS league
				FROM (
					SELECT club_name, league FROM silver.fmdata_team_players WHERE club_name IS NOT NULL AND league IS NOT NULL AND dwh_current_validity = 1
					UNION
					SELECT club_name, league FROM silver.fmdata_team_gks WHERE club_name IS NOT NULL AND league IS NOT NULL AND dwh_current_validity = 1
					UNION
					SELECT club_name, league FROM silver.fmdata_interested_out_players WHERE club_name IS NOT NULL AND league IS NOT NULL AND dwh_current_validity = 1
					UNION
					SELECT club_name, league FROM silver.fmdata_interested_gks WHERE club_name IS NOT NULL AND league IS NOT NULL AND dwh_current_validity = 1
				) AS raw_teams
			),
			cleaned AS (
				SELECT
					CONCAT(S.league_key, '.', S1.club_key) AS team_key,
					st.league,
					st.club_name,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM source_teams AS st
				INNER JOIN gold.dim_league AS S
					ON st.league = S.league
				INNER JOIN gold.dim_club AS S1
					ON st.club_name = S1.club
			)
			INSERT INTO gold.dim_team_info (
				team_key, league, club_name, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT DISTINCT
				team_key,
				league,
				club_name,
				dwh_create_date,
				dwh_cd_valid_till,
				dwh_current_validity
			FROM cleaned AS C
			WHERE NOT EXISTS (
				SELECT 1 FROM gold.dim_team_info AS E WHERE E.team_key = C.team_key
			);

		SET @end_time = GETDATE();
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
						SELECT position FROM silver.fmdata_team_players WHERE position IS NOT NULL AND dwh_current_validity = 1
						UNION
						SELECT position FROM silver.fmdata_team_gks WHERE position IS NOT NULL AND dwh_current_validity = 1
						UNION
						SELECT position FROM silver.fmdata_interested_out_players WHERE position IS NOT NULL AND dwh_current_validity = 1
						UNION
						SELECT position FROM silver.fmdata_interested_gks WHERE position IS NOT NULL AND dwh_current_validity = 1
					) AS Q
			), raw_player_info AS (
				SELECT
					A.player_id,
					A.player_name,
					B.team_key AS team_key,
					0 AS is_own_squad,
					0 AS contracted,
					A.age,
					C.position,
					A.dwh_create_date
				FROM silver.fmdata_interested_out_players AS A
				INNER JOIN gold.dim_team_info AS B
					ON A.club_name = B.club_name AND A.league = B.league
				INNER JOIN position AS C
					ON A.position = C.position
				WHERE A.dwh_current_validity = 1
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1)
			UNION ALL
				SELECT
					A.player_id,
					A.player_name,
					B.team_key AS team_key,
					1 AS is_own_squad,
					1 AS contracted,
					A.age,
					C.position,
					A.dwh_create_date
				FROM silver.fmdata_team_players AS A
				INNER JOIN gold.dim_team_info AS B
					ON A.club_name = B.club_name AND A.league = B.league
				INNER JOIN position AS C
					ON A.position = C.position
				WHERE A.dwh_current_validity = 1
			UNION ALL
				SELECT
					A.player_id,
					A.player_name,
					B.team_key AS team_key,
					0 AS is_own_squad,
					0 AS contracted,
					A.age,
					C.position,
					A.dwh_create_date
				FROM silver.fmdata_interested_gks AS A
				INNER JOIN gold.dim_team_info AS B
					ON A.club_name = B.club_name AND A.league = B.league
				INNER JOIN position AS C
					ON A.position = C.position
				WHERE A.dwh_current_validity = 1
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks WHERE dwh_current_validity = 1)
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1)
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_interested_out_players WHERE dwh_current_validity = 1)
			UNION ALL
				SELECT
					A.player_id,
					A.player_name,
					B.team_key AS team_key,
					1 AS is_own_squad,
					1 AS contracted,
					A.age,
					C.position,
					A.dwh_create_date
				FROM silver.fmdata_team_gks AS A
				INNER JOIN gold.dim_team_info AS B
					ON A.club_name = B.club_name AND A.league = B.league
				INNER JOIN position AS C
					ON A.position = C.position
				WHERE A.dwh_current_validity = 1
			), cleaned AS (
				SELECT
					player_id,
					player_name,
					team_key,
					is_own_squad,
					contracted,
					age,
					position
				FROM (
					SELECT *, ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY is_own_squad DESC, dwh_create_date DESC) AS rn
					FROM raw_player_info
				) p
				WHERE p.rn = 1
			)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.dim_player_info AS S
			INNER JOIN cleaned AS N
				ON S.player_id = N.player_id
			WHERE S.player_id = N.player_id
				AND (
					S.team_key != N.team_key
					OR S.age != N.age
					OR S.position != N.position
					OR S.is_own_squad != N.is_own_squad
					OR S.contracted != N.contracted
					OR S.season != @Season
				)
				AND S.dwh_current_validity = 1;

		PRINT '>>> Retiring Departed Squad Players in GOLD.DIM_PLAYER_INFO';
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.dim_player_info AS S
			WHERE S.dwh_current_validity = 1
			  AND S.is_own_squad = 1
			  AND S.player_id NOT IN (
					SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1
					UNION
					SELECT player_id FROM silver.fmdata_team_gks WHERE dwh_current_validity = 1
			  );

		PRINT '>>> Inserting New Records in GOLD.DIM_PLAYER_INFO';
			WITH position AS (
				SELECT DISTINCT(position) FROM (
						SELECT position FROM silver.fmdata_team_players WHERE position IS NOT NULL AND dwh_current_validity = 1
						UNION
						SELECT position FROM silver.fmdata_team_gks WHERE position IS NOT NULL AND dwh_current_validity = 1
						UNION
						SELECT position FROM silver.fmdata_interested_out_players WHERE position IS NOT NULL AND dwh_current_validity = 1
						UNION
						SELECT position FROM silver.fmdata_interested_gks WHERE position IS NOT NULL AND dwh_current_validity = 1
					) AS Q
			), raw_player_info AS (
				SELECT
					A.player_id,
					A.player_name,
					B.team_key AS team_key,
					0 AS is_own_squad,
					0 AS contracted,
					A.age,
					C.position,
					A.dwh_create_date AS silver_create_date
				FROM silver.fmdata_interested_out_players AS A
				INNER JOIN gold.dim_team_info AS B
					ON A.club_name = B.club_name AND A.league = B.league
				INNER JOIN position AS C
					ON A.position = C.position
				WHERE A.dwh_current_validity = 1
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1)
			UNION ALL
				SELECT
					A.player_id,
					A.player_name,
					B.team_key AS team_key,
					1 AS is_own_squad,
					1 AS contracted,
					A.age,
					C.position,
					A.dwh_create_date AS silver_create_date
				FROM silver.fmdata_team_players AS A
				INNER JOIN gold.dim_team_info AS B
					ON A.club_name = B.club_name AND A.league = B.league
				INNER JOIN position AS C
					ON A.position = C.position
				WHERE A.dwh_current_validity = 1
			UNION ALL
				SELECT
					A.player_id,
					A.player_name,
					B.team_key AS team_key,
					0 AS is_own_squad,
					0 AS contracted,
					A.age,
					C.position,
					A.dwh_create_date AS silver_create_date
				FROM silver.fmdata_interested_gks AS A
				INNER JOIN gold.dim_team_info AS B
					ON A.club_name = B.club_name AND A.league = B.league
				INNER JOIN position AS C
					ON A.position = C.position
				WHERE A.dwh_current_validity = 1
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks WHERE dwh_current_validity = 1)
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1)
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_interested_out_players WHERE dwh_current_validity = 1)
			UNION ALL
				SELECT
					A.player_id,
					A.player_name,
					B.team_key AS team_key,
					1 AS is_own_squad,
					1 AS contracted,
					A.age,
					C.position,
					A.dwh_create_date AS silver_create_date
				FROM silver.fmdata_team_gks AS A
				INNER JOIN gold.dim_team_info AS B
					ON A.club_name = B.club_name AND A.league = B.league
				INNER JOIN position AS C
					ON A.position = C.position
				WHERE A.dwh_current_validity = 1
			), cleaned AS (
				SELECT 
					player_id,
					player_name,
					team_key,
					is_own_squad,
					contracted,
					age,
					position,
					GETDATE() AS dwh_create_date,
					'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
					1 AS dwh_current_validity
				FROM (
					SELECT 
						player_id,
						player_name,
						team_key,
						is_own_squad,
						contracted,
						age,
						position,
						ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY is_own_squad DESC, silver_create_date DESC) AS rn
					FROM raw_player_info
				) p
				WHERE p.rn = 1
			)
			INSERT INTO gold.dim_player_info (
				player_id, player_name, team_key, is_own_squad, contracted, age, position, season, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT DISTINCT
				S.player_id,
				S.player_name,
				S.team_key,
				S.is_own_squad,
				S.contracted,
				S.age,
				S.position,
				@Season,
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
					OR S.is_own_squad != O.is_own_squad
					OR S.contracted != O.contracted
					OR O.season != @Season
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
					A.player_id,
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
					ON A.player_id = B.player_id AND B.dwh_current_validity = 1
				INNER JOIN gold.dim_team_info AS C
					ON A.club_name = C.club_name AND A.league = C.league
				WHERE A.dwh_current_validity = 1
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1) -- If present, ignore our players
				UNION
				SELECT
					A.player_id,
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
					ON A.player_id = B.player_id AND B.dwh_current_validity = 1
				INNER JOIN gold.dim_team_info AS C
					ON A.club_name = C.club_name AND A.league = C.league
				WHERE A.dwh_current_validity = 1
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_outfield_player_stats AS S
			INNER JOIN gold.dim_player_info AS P
				ON S.player_key = P.player_key
			INNER JOIN cleaned AS N
				ON P.player_id = N.player_id
			WHERE S.dwh_current_validity = 1
				AND (
					S.player_key != N.player_key
					OR S.team_key != N.team_key
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
					OR S.season != @Season
				);

		PRINT '>>> Retiring Departed Facts in GOLD.FACT_OUTFIELD_STATS';
			UPDATE F
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_outfield_player_stats AS F
			INNER JOIN gold.dim_player_info AS P
				ON F.player_key = P.player_key
			WHERE F.dwh_current_validity = 1
			  AND P.dwh_current_validity = 0
			  AND NOT EXISTS (
					SELECT 1 FROM gold.dim_player_info P2 
					WHERE P2.player_id = P.player_id AND P2.dwh_current_validity = 1
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
					ON A.player_id = B.player_id AND B.dwh_current_validity = 1
				INNER JOIN gold.dim_team_info AS C
					ON A.club_name = C.club_name AND A.league = C.league
				WHERE A.dwh_current_validity = 1
				  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1) -- If present, ignore our players
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
					ON A.player_id = B.player_id AND B.dwh_current_validity = 1
				INNER JOIN gold.dim_team_info AS C
					ON A.club_name = C.club_name AND A.league = C.league
				WHERE A.dwh_current_validity = 1
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
					fouls_against_per90, yellow_cards_per90, red_cards_per90, season, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
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
				@Season,
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
					OR O.season != @Season
				)
			OR NOT EXISTS (
				SELECT 1
				FROM gold.fact_outfield_player_stats AS N
				WHERE N.player_key = S.player_key
					AND N.dwh_current_validity = 1
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
						A.player_id,
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
						ON A.player_id = B.player_id AND B.dwh_current_validity = 1
					INNER JOIN gold.dim_team_info AS C
						ON A.club_name = C.club_name AND A.league = C.league
					WHERE A.dwh_current_validity = 1
					  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks WHERE dwh_current_validity = 1) -- If present, ignore our players
					UNION
					SELECT
						A.player_id,
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
						ON A.player_id = B.player_id AND B.dwh_current_validity = 1
					INNER JOIN gold.dim_team_info AS C
						ON A.club_name = C.club_name AND A.league = C.league
					WHERE A.dwh_current_validity = 1
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_gk_stats AS S
			INNER JOIN gold.dim_player_info AS P
				ON S.player_key = P.player_key
			INNER JOIN cleaned AS N
				ON P.player_id = N.player_id
			WHERE S.dwh_current_validity = 1
				AND (
					S.player_key != N.player_key
					OR S.team_key != N.team_key
					OR S.minutes_played != N.minutes_played
					OR S.team_goals_scored_per90 != N.team_goals_scored_per90
					OR S.team_goals_conceded_per90 != N.team_goals_conceded_per90
					OR S.goals_conceded_per90 != N.goals_conceded_per90
					OR S.saves_tipped_per90 != N.saves_tipped_per90
					OR S.saves_parried_per90 != N.saves_parried_per90
					OR S.passes_attempted_per90 != N.passes_attempted_per90
					OR S.clearances_per90 != N.clearances_per90
					OR S.distance_covered_km_per90 != N.distance_covered_km_per90
					OR S.possession_lost_per90 != N.possession_lost_per90
					OR S.season != @Season
				);

		PRINT '>>> Retiring Departed Facts in GOLD.FACT_GK_STATS';
			UPDATE F
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_gk_stats AS F
			INNER JOIN gold.dim_player_info AS P
				ON F.player_key = P.player_key
			WHERE F.dwh_current_validity = 1
			  AND P.dwh_current_validity = 0
			  AND NOT EXISTS (
					SELECT 1 FROM gold.dim_player_info P2 
					WHERE P2.player_id = P.player_id AND P2.dwh_current_validity = 1
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
						ON A.player_id = B.player_id AND B.dwh_current_validity = 1
					INNER JOIN gold.dim_team_info AS C
						ON A.club_name = C.club_name AND A.league = C.league
					WHERE A.dwh_current_validity = 1
					  AND A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks WHERE dwh_current_validity = 1) -- If present, ignore our players
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
						ON A.player_id = B.player_id AND B.dwh_current_validity = 1
					INNER JOIN gold.dim_team_info AS C
						ON A.club_name = C.club_name AND A.league = C.league
					WHERE A.dwh_current_validity = 1
				)
			INSERT INTO gold.fact_gk_stats (
				player_key, team_key, minutes_played, team_goals_scored_per90, team_goals_conceded_per90,	 goals_conceded_per90,	
				saves_made_per90, xGoals_prevented_per90, xSave_rate, saves_tipped_per90, saves_parried_per90, saves_held_per90,
				saves_percentage, passes_attempted_per90, pass_accuracy, possession_won_per90, possession_lost_per90,
				interceptions_per90, clearances_per90, penalties_faced_per90, penalties_save_percentage, distance_covered_km_per90,
				mistakes_leading_to_goals_per90, fouls_against_per90, season, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
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
				@Season,
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
					OR O.season != @Season
				)
			OR NOT EXISTS (
				SELECT 1
				FROM gold.fact_gk_stats AS N
				WHERE N.player_key = S.player_key
					AND N.dwh_current_validity = 1
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
					WHERE dwh_current_validity = 1
					  AND player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1) -- If present, ignore our players
					UNION
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_interested_gks
					WHERE dwh_current_validity = 1
					  AND player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks WHERE dwh_current_validity = 1) -- If present, ignore our players
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
					WHERE dwh_current_validity = 1
					UNION
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_team_gks
					WHERE dwh_current_validity = 1
				),
				cleaned AS (
					SELECT
						A.player_id,
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
					WHERE A.dwh_current_validity = 1
					  AND A.player_id NOT IN (SELECT player_id FROM cleaned_silver_team)
					UNION
					SELECT
						A.player_id,
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
					WHERE A.dwh_current_validity = 1
				)
			UPDATE S
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_players_value AS S
			INNER JOIN gold.dim_player_info AS P
				ON S.player_key = P.player_key
			INNER JOIN cleaned AS N
				ON P.player_id = N.player_id
			WHERE S.dwh_current_validity = 1
				AND (
					S.player_key != N.player_key
					OR S.team_key != N.team_key
					OR ISNULL(S.wage_per_week_k, -1) != ISNULL(N.wage_per_week_k, -1)
					OR ISNULL(S.transfer_value_m, -1) != ISNULL(N.transfer_value_m, -1)
					OR S.season != @Season
				);

		PRINT '>>> Retiring Departed Facts in GOLD.FACT_PLAYERS_VALUE';
			UPDATE F
			SET 
				dwh_cd_valid_till = GETDATE(),
				dwh_current_validity = 0
			FROM gold.fact_players_value AS F
			INNER JOIN gold.dim_player_info AS P
				ON F.player_key = P.player_key
			WHERE F.dwh_current_validity = 1
			  AND P.dwh_current_validity = 0
			  AND NOT EXISTS (
					SELECT 1 FROM gold.dim_player_info P2 
					WHERE P2.player_id = P.player_id AND P2.dwh_current_validity = 1
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
					WHERE dwh_current_validity = 1
					  AND player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players WHERE dwh_current_validity = 1) -- If present, ignore our players
					UNION
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_interested_gks
					WHERE dwh_current_validity = 1
					  AND player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks WHERE dwh_current_validity = 1) -- If present, ignore our players
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
					WHERE dwh_current_validity = 1
					UNION
					SELECT
						player_id,
						wage_per_week_k,
						transfer_value_m,
						GETDATE() AS dwh_create_date,
						'9999-12-31 23:59:59.9999999' AS dwh_cd_valid_till,
						1 AS dwh_current_validity
					FROM silver.fmdata_team_gks
					WHERE dwh_current_validity = 1
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
					WHERE A.dwh_current_validity = 1
					  AND A.player_id NOT IN (SELECT player_id FROM cleaned_silver_team)
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
					WHERE A.dwh_current_validity = 1
				)
			INSERT INTO gold.fact_players_value (
				player_key, team_key, wage_per_week_k, transfer_value_m, season, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
			)
			SELECT
				S.player_key,
				S.team_key,
				S.wage_per_week_k,
				S.transfer_value_m,
				@Season,
				S.dwh_create_date,
				S.dwh_cd_valid_till,
				S.dwh_current_validity
			FROM cleaned AS S
			LEFT JOIN gold.fact_players_value AS O
				ON S.player_key = O.player_key
				AND O.dwh_current_validity = 1
				WHERE (
					S.team_key != O.team_key
					OR ISNULL(S.wage_per_week_k, -1) != ISNULL(O.wage_per_week_k, -1)
					OR ISNULL(S.transfer_value_m, -1) != ISNULL(O.transfer_value_m, -1)
					OR O.season != @Season
				)
			OR NOT EXISTS (
				SELECT 1
				FROM gold.fact_players_value AS N
				WHERE N.player_key = S.player_key
					AND N.dwh_current_validity = 1
			);

		SET @end_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------------';

		-- ======================================================================
		-- GOLD.DIM_TEAM_TACTICAL_ARCHETYPES
		-- ======================================================================
		PRINT '=============================================';
		PRINT 'GOLD.DIM_TEAM_TACTICAL_ARCHETYPES';
		PRINT '---------------------------------------------';
		PRINT 'AGGREGATING TEAM METRICS & ARCHETYPES FROM FACT TABLES';
		SET @start_time = GETDATE();

		-- SCD Type 2: Expire existing current valid records for the season if updating
		UPDATE gold.dim_team_tactical_archetypes
		SET dwh_current_validity = 0,
			dwh_cd_valid_till = GETDATE()
		WHERE dwh_current_validity = 1
		  AND season = @Season;

		-- Insert fresh aggregated team tactical profiles
		WITH team_raw AS (
			SELECT 
				t.team_key,
				t.club_name,
				t.league,
				COUNT(f.player_key) AS active_players,
				SUM(f.minutes_played) AS total_minutes,
				
				-- Pressing & Work Rate
				ROUND(SUM(f.pressures_attempted_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS pressures_attempted_per90,
				ROUND(SUM(f.pressures_completed_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS pressures_completed_per90,
				ROUND(SUM(f.possession_won_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS possession_won_per90,
				ROUND(SUM(f.possession_lost_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS possession_lost_per90,
				ROUND(SUM(f.interceptions_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS interceptions_per90,
				ROUND(SUM(f.sprints_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS sprints_per90,
				ROUND(SUM(f.distance_covered_km_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS distance_covered_km_per90,
				ROUND(SUM(f.fouls_made_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS fouls_made_per90,

				-- Possession & Buildup
				ROUND(SUM(f.passes_attempted_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS passes_attempted_per90,
				ROUND(SUM(f.pass_accuracy * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 3) AS pass_accuracy,
				ROUND(SUM(f.progressive_passes_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS progressive_passes_per90,
				ROUND(SUM(f.open_play_key_passes_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS open_play_key_passes_per90,
				ROUND(SUM(f.chances_created_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS chances_created_per90,
				ROUND(SUM(f.xGoals_per_shot * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS xGoals_per_shot,

				-- Wing & Crossing
				ROUND(SUM(f.crosses_attempted_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS crosses_attempted_per90,
				ROUND(SUM(f.open_play_crosses_attempted_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS open_play_crosses_attempted_per90,

				-- Aerial & Route One
				ROUND(SUM(f.headers_attempted_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS headers_attempted_per90,
				ROUND(SUM(f.heading_accuracy * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 3) AS heading_accuracy,
				ROUND(SUM(f.clearances_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS clearances_per90,

				-- Deep Block & Defending
				ROUND(SUM(f.blocks_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS blocks_per90,
				ROUND(SUM(f.shots_blocked_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS shots_blocked_per90,
				ROUND(SUM(f.tackles_attempted_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS tackles_attempted_per90,
				ROUND(SUM(f.dribbles_made_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS dribbles_made_per90,
				ROUND(SUM(f.shots_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS shots_per90,
				ROUND(SUM(f.xGoals_per90 * f.minutes_played) / NULLIF(SUM(f.minutes_played), 0), 2) AS xGoals_per90
			FROM gold.fact_outfield_player_stats f
			JOIN gold.dim_team_info t ON f.team_key = t.team_key AND t.dwh_current_validity = 1
			WHERE f.minutes_played >= 180 AND f.dwh_current_validity = 1
			GROUP BY t.team_key, t.club_name, t.league
			HAVING SUM(f.minutes_played) >= 900
		),
		scored AS (
			SELECT 
				r.*,
				-- 1. Gegenpressing Score
				CAST(ROUND(
					(CASE WHEN pressures_attempted_per90 >= 9.0 THEN 35 WHEN pressures_attempted_per90 >= 8.0 THEN 25 ELSE (pressures_attempted_per90 / 8.0) * 20 END) +
					(CASE WHEN sprints_per90 >= 11.5 THEN 25 WHEN sprints_per90 >= 10.0 THEN 18 ELSE (sprints_per90 / 10.0) * 12 END) +
					(CASE WHEN possession_won_per90 >= 11.5 THEN 20 ELSE (possession_won_per90 / 11.5) * 15 END) +
					(CASE WHEN distance_covered_km_per90 >= 12.0 THEN 20 ELSE (distance_covered_km_per90 / 12.0) * 15 END)
				, 0) AS INT) AS gegenpressing_score,

				-- 2. Tiki-Taka Score
				CAST(ROUND(
					(CASE WHEN passes_attempted_per90 >= 46.0 THEN 40 WHEN passes_attempted_per90 >= 42.0 THEN 28 ELSE (passes_attempted_per90 / 42.0) * 20 END) +
					(CASE WHEN pass_accuracy >= 0.880 THEN 35 WHEN pass_accuracy >= 0.850 THEN 25 ELSE (pass_accuracy / 0.85) * 15 END) +
					(CASE WHEN clearances_per90 <= 0.95 THEN 25 WHEN clearances_per90 <= 1.10 THEN 15 ELSE 5 END)
				, 0) AS INT) AS tiki_taka_score,

				-- 3. Vertical Tiki-Taka Score
				CAST(ROUND(
					(CASE WHEN passes_attempted_per90 >= 44.0 THEN 30 ELSE (passes_attempted_per90 / 44.0) * 20 END) +
					(CASE WHEN progressive_passes_per90 >= 3.6 THEN 35 WHEN progressive_passes_per90 >= 3.2 THEN 25 ELSE (progressive_passes_per90 / 3.2) * 15 END) +
					(CASE WHEN open_play_key_passes_per90 >= 1.2 THEN 20 ELSE (open_play_key_passes_per90 / 1.2) * 12 END) +
					(CASE WHEN crosses_attempted_per90 <= 1.8 THEN 15 WHEN crosses_attempted_per90 <= 2.2 THEN 8 ELSE 2 END)
				, 0) AS INT) AS vertical_tiki_taka_score,

				-- 4. Fluid Counter-Attack Score
				CAST(ROUND(
					(CASE WHEN dribbles_made_per90 >= 1.20 THEN 40 WHEN dribbles_made_per90 >= 1.0 THEN 28 ELSE (dribbles_made_per90 / 1.0) * 20 END) +
					(CASE WHEN sprints_per90 >= 10.5 THEN 35 WHEN sprints_per90 >= 9.5 THEN 25 ELSE (sprints_per90 / 9.5) * 18 END) +
					(CASE WHEN blocks_per90 >= 0.42 THEN 25 ELSE (blocks_per90 / 0.42) * 15 END)
				, 0) AS INT) AS fluid_counter_score,

				-- 5. Direct Counter-Attack Score
				CAST(ROUND(
					(CASE WHEN progressive_passes_per90 >= 3.5 THEN 35 ELSE (progressive_passes_per90 / 3.5) * 20 END) +
					(CASE WHEN clearances_per90 >= 1.10 THEN 35 ELSE (clearances_per90 / 1.10) * 20 END) +
					(CASE WHEN pass_accuracy <= 0.860 THEN 30 WHEN pass_accuracy <= 0.875 THEN 18 ELSE 5 END)
				, 0) AS INT) AS direct_counter_score,

				-- 6. Wing Play Score
				CAST(ROUND(
					(CASE WHEN crosses_attempted_per90 >= 2.6 THEN 45 WHEN crosses_attempted_per90 >= 2.2 THEN 30 ELSE (crosses_attempted_per90 / 2.2) * 18 END) +
					(CASE WHEN open_play_crosses_attempted_per90 >= 1.6 THEN 25 ELSE (open_play_crosses_attempted_per90 / 1.6) * 15 END) +
					(CASE WHEN headers_attempted_per90 >= 6.2 THEN 30 ELSE (headers_attempted_per90 / 6.2) * 18 END)
				, 0) AS INT) AS wing_play_score,

				-- 7. Route One Score
				CAST(ROUND(
					(CASE WHEN headers_attempted_per90 >= 7.0 THEN 40 WHEN headers_attempted_per90 >= 6.2 THEN 28 ELSE (headers_attempted_per90 / 6.2) * 18 END) +
					(CASE WHEN clearances_per90 >= 1.15 THEN 30 ELSE (clearances_per90 / 1.15) * 18 END) +
					(CASE WHEN heading_accuracy >= 0.580 THEN 15 ELSE 8 END) +
					(CASE WHEN passes_attempted_per90 <= 40.0 THEN 15 WHEN passes_attempted_per90 <= 43.0 THEN 8 ELSE 2 END)
				, 0) AS INT) AS route_one_score,

				-- 8. Catenaccio Score
				CAST(ROUND(
					(CASE WHEN blocks_per90 >= 0.48 THEN 40 WHEN blocks_per90 >= 0.40 THEN 28 ELSE (blocks_per90 / 0.40) * 18 END) +
					(CASE WHEN shots_blocked_per90 >= 0.32 THEN 30 ELSE (shots_blocked_per90 / 0.32) * 18 END) +
					(CASE WHEN pressures_attempted_per90 <= 7.2 THEN 30 WHEN pressures_attempted_per90 <= 8.0 THEN 18 ELSE 5 END)
				, 0) AS INT) AS catenaccio_score
			FROM team_raw r
		),
		classified AS (
			SELECT 
				s.*,
				CASE 
					WHEN pressures_attempted_per90 >= 8.5 AND sprints_per90 >= 10.5 THEN 'Gegenpressing'
					WHEN passes_attempted_per90 >= 45.0 AND pass_accuracy >= 0.875 AND clearances_per90 <= 1.05 THEN 'Tiki-Taka'
					WHEN passes_attempted_per90 >= 43.0 AND progressive_passes_per90 >= 3.6 AND crosses_attempted_per90 <= 1.9 THEN 'Vertical Tiki-Taka'
					WHEN crosses_attempted_per90 >= 2.45 AND (headers_attempted_per90 >= 6.0 OR dribbles_made_per90 >= 1.1) THEN 'Wing Play'
					WHEN headers_attempted_per90 >= 6.8 AND clearances_per90 >= 1.15 AND pass_accuracy <= 0.86 THEN 'Route One'
					WHEN blocks_per90 >= 0.48 AND clearances_per90 >= 1.15 AND pressures_attempted_per90 <= 7.2 THEN 'Catenaccio'
					WHEN dribbles_made_per90 >= 1.15 AND sprints_per90 >= 10.0 THEN 'Fluid Counter-Attack'
					WHEN progressive_passes_per90 >= 3.5 AND clearances_per90 >= 1.1 THEN 'Direct Counter-Attack'
					WHEN passes_attempted_per90 >= 42.0 THEN 'Control Possession'
					WHEN clearances_per90 >= 1.1 THEN 'Direct Counter-Attack'
					ELSE 'Balanced / Adaptive'
				END AS tactical_archetype_calc
			FROM scored s
		)
		INSERT INTO gold.dim_team_tactical_archetypes (
			team_key, club_name, league, active_players, total_minutes,
			tactical_archetype, tactical_framework, tactical_description,
			pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,
			passes_attempted_per90, pass_accuracy, progressive_passes_per90, open_play_key_passes_per90, chances_created_per90,
			xGoals_per_shot, crosses_attempted_per90, open_play_crosses_attempted_per90, headers_attempted_per90, heading_accuracy,
			clearances_per90, blocks_per90, shots_blocked_per90, tackles_attempted_per90, dribbles_made_per90, sprints_per90,
			distance_covered_km_per90, fouls_made_per90, shots_per90, xGoals_per90,
			gegenpressing_score, tiki_taka_score, vertical_tiki_taka_score, fluid_counter_score, direct_counter_score,
			wing_play_score, route_one_score, catenaccio_score,
			season, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
		)
		SELECT 
			c.team_key, c.club_name, c.league, c.active_players, c.total_minutes,
			c.tactical_archetype_calc AS tactical_archetype,
			CASE 
				WHEN c.tactical_archetype_calc = 'Gegenpressing' THEN '1. High-Intensity Pressing'
				WHEN c.tactical_archetype_calc IN ('Tiki-Taka', 'Control Possession') THEN '2. Possession & Control'
				WHEN c.tactical_archetype_calc = 'Vertical Tiki-Taka' THEN '3. Penetrative Possession'
				WHEN c.tactical_archetype_calc IN ('Fluid Counter-Attack', 'Direct Counter-Attack') THEN '4. Transition & Space Exploitation'
				WHEN c.tactical_archetype_calc = 'Wing Play' THEN '5. Horizontal Stretching'
				WHEN c.tactical_archetype_calc = 'Route One' THEN '6. Aerial & Vertical Attrition'
				WHEN c.tactical_archetype_calc = 'Catenaccio' THEN '7. Deep Block Defending'
				ELSE '8. Balanced / Adaptive Framework'
			END AS tactical_framework,
			CASE 
				WHEN c.tactical_archetype_calc = 'Gegenpressing' THEN 'Bypasses defensive transition by aggressively hunting the ball in advanced territory immediately after turnover with elite work rate and sprints.'
				WHEN c.tactical_archetype_calc = 'Tiki-Taka' THEN 'Risk-averse, horizontal possession designed to monopolize the ball and dictate tempo through high volume, high accuracy passing.'
				WHEN c.tactical_archetype_calc = 'Control Possession' THEN 'Patient, possession-dominant game model focusing on ball security, territorial control, and systematic buildup.'
				WHEN c.tactical_archetype_calc = 'Vertical Tiki-Taka' THEN 'Blends possession dominance with aggressive, line-breaking vertical passes through central channels.'
				WHEN c.tactical_archetype_calc = 'Fluid Counter-Attack' THEN 'Absorbs pressure deep, then relies on mobile forwards and playmakers to carry the ball and combine at high speed.'
				WHEN c.tactical_archetype_calc = 'Direct Counter-Attack' THEN 'Bypasses midfield upon turnover, launching immediate vertical balls forward into channels and runners.'
				WHEN c.tactical_archetype_calc = 'Wing Play' THEN 'Bypasses central congestion by overloading the flanks, isolating wide creators, and flooding the penalty box with crosses.'
				WHEN c.tactical_archetype_calc = 'Route One' THEN 'Forgoes midfield buildup, relying on aerial combat, direct long distribution, and recovering knock-downs.'
				WHEN c.tactical_archetype_calc = 'Catenaccio' THEN 'Risk-averse survival block protecting the penalty box with high volume clearances and shots blocked.'
				ELSE 'Flexible tactical system balancing possession retention, controlled pressing, and measured counter-transitions.'
			END AS tactical_description,
			c.pressures_attempted_per90, c.pressures_completed_per90, c.possession_won_per90, c.possession_lost_per90,
			c.passes_attempted_per90, c.pass_accuracy, c.progressive_passes_per90, c.open_play_key_passes_per90, c.chances_created_per90,
			c.xGoals_per_shot, c.crosses_attempted_per90, c.open_play_crosses_attempted_per90, c.headers_attempted_per90, c.heading_accuracy,
			c.clearances_per90, c.blocks_per90, c.shots_blocked_per90, c.tackles_attempted_per90, c.dribbles_made_per90, c.sprints_per90,
			c.distance_covered_km_per90, c.fouls_made_per90, c.shots_per90, c.xGoals_per90,
			c.gegenpressing_score, c.tiki_taka_score, c.vertical_tiki_taka_score, c.fluid_counter_score, c.direct_counter_score,
			c.wing_play_score, c.route_one_score, c.catenaccio_score,
			@Season, GETDATE(), '9999-12-31 23:59:59.9999999', 1
		FROM classified c;

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
END;
GO

-- ======================================================================
-- Stored Procedure: Ad-hoc Player Replacement Scouting Engine
-- ======================================================================
-- Finds the most similar external transfer targets for ANY specified
-- player in the database (by name or player_key).
--
-- Examples:
--   EXEC gold.usp_scout_similar_players @TargetPlayerName = 'Edin Džeko', @TopN = 10;
--   EXEC gold.usp_scout_similar_players @TargetPlayerName = 'Rolando Mandragora', @TopN = 10;
--   EXEC gold.usp_scout_similar_players @TargetPlayerKey = 12345, @TopN = 5, @MinMinutes = 900;
-- ======================================================================
CREATE OR ALTER PROCEDURE gold.usp_scout_similar_players
    @TargetPlayerName NVARCHAR(100) = NULL,
    @TargetPlayerKey INT = NULL,
    @TopN INT = 10,
    @MinMinutes INT = 900
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Resolve Target Player
    DECLARE @ResolvedPlayerKey INT, @ResolvedName NVARCHAR(100), @Position NVARCHAR(50);

    IF @TargetPlayerKey IS NOT NULL
    BEGIN
        SELECT TOP 1 
            @ResolvedPlayerKey = player_key, 
            @ResolvedName = player_name, 
            @Position = position
        FROM gold.dim_player_info
        WHERE player_key = @TargetPlayerKey AND dwh_current_validity = 1;
    END
    ELSE IF @TargetPlayerName IS NOT NULL
    BEGIN
        SELECT TOP 1 
            @ResolvedPlayerKey = player_key, 
            @ResolvedName = player_name, 
            @Position = position
        FROM gold.dim_player_info
        WHERE player_name LIKE '%' + @TargetPlayerName + '%' AND dwh_current_validity = 1
        ORDER BY CASE WHEN player_name = @TargetPlayerName THEN 0 ELSE 1 END;
    END

    IF @ResolvedPlayerKey IS NULL
    BEGIN
        RAISERROR('Target player not found in gold.dim_player_info. Please provide a valid @TargetPlayerName or @TargetPlayerKey.', 16, 1);
        RETURN;
    END

    PRINT CONCAT('Scouting replacements for: ', @ResolvedName, ' (Key: ', @ResolvedPlayerKey, ', Position: ', @Position, ')');

    -- 2. Execute Position-Specific Similarity Search
    IF @Position = 'Goalkeeper'
    BEGIN
        WITH bmk AS (
            SELECT TOP 1
                shot_stopping_delta, xGoals_prevented_per90, hold_rate_pct, sweeper_index, distribution_value, pass_accuracy
            FROM gold.mart_gk_scouting_summary
            WHERE player_key = @ResolvedPlayerKey
        )
        SELECT TOP (@TopN)
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
            tgt.player_key, tgt.player_name, tgt.age, tgt.club_name, tgt.league,
            tgt.wage_per_week_k, tgt.transfer_value_m, tgt.minutes_played,
            tgt.shot_stopping_delta, tgt.xGoals_prevented_per90, tgt.saves_percentage, tgt.hold_rate_pct,
            tgt.sweeper_index, tgt.distribution_value, tgt.pass_accuracy
        FROM gold.mart_gk_scouting_summary AS tgt
        CROSS JOIN bmk
        WHERE tgt.is_own_squad = 0 AND tgt.player_key != @ResolvedPlayerKey AND tgt.minutes_played >= @MinMinutes
        ORDER BY similarity_distance ASC;
    END
    ELSE IF @Position = 'Central Defender'
    BEGIN
        WITH bmk AS (
            SELECT TOP 1
                aerial_duels_won_per90, heading_accuracy, prog_rate_per_pass, progressive_passes_per90,
                defensive_actions_per90, tackle_accuracy, reactive_vs_positional, discipline_index
            FROM gold.mart_outfield_scouting_summary
            WHERE player_key = @ResolvedPlayerKey
        )
        SELECT TOP (@TopN)
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
            tgt.player_key, tgt.player_name, tgt.age, tgt.club_name, tgt.league,
            tgt.wage_per_week_k, tgt.transfer_value_m, tgt.minutes_played,
            tgt.aerial_duels_won_per90, tgt.heading_accuracy, tgt.progressive_passes_per90,
            tgt.prog_rate_per_pass, tgt.defensive_actions_per90, tgt.tackle_accuracy, tgt.cost_per_xgi
        FROM gold.mart_outfield_scouting_summary AS tgt
        CROSS JOIN bmk
        WHERE tgt.is_own_squad = 0 AND tgt.position = 'Central Defender' AND tgt.player_key != @ResolvedPlayerKey AND tgt.minutes_played >= @MinMinutes
        ORDER BY similarity_distance ASC;
    END
    ELSE IF @Position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender')
    BEGIN
        WITH bmk AS (
            SELECT TOP 1
                completed_crosses_per90, open_play_cross_accuracy, prog_rate_per_pass, turnover_rate,
                defensive_actions_per90, tackle_accuracy, sprints_per90
            FROM gold.mart_outfield_scouting_summary
            WHERE player_key = @ResolvedPlayerKey
        )
        SELECT TOP (@TopN)
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
            tgt.player_key, tgt.player_name, tgt.age, tgt.position, tgt.club_name, tgt.league,
            tgt.wage_per_week_k, tgt.transfer_value_m, tgt.minutes_played,
            tgt.completed_crosses_per90, tgt.open_play_cross_accuracy, tgt.prog_rate_per_pass,
            tgt.turnover_rate, tgt.defensive_actions_per90, tgt.tackle_accuracy, tgt.sprints_per90, tgt.cost_per_xgi
        FROM gold.mart_outfield_scouting_summary AS tgt
        CROSS JOIN bmk
        WHERE tgt.is_own_squad = 0 AND tgt.position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender')
          AND tgt.player_key != @ResolvedPlayerKey AND tgt.minutes_played >= @MinMinutes
        ORDER BY similarity_distance ASC;
    END
    ELSE IF @Position IN ('Central Midfielder', 'Defensive Midfielder')
    BEGIN
        WITH bmk AS (
            SELECT TOP 1
                progressive_passes_per90, prog_rate_per_pass, passes_attempted_per90, pass_accuracy,
                turnover_rate, npxg_plus_xa_per90, defensive_actions_per90, pressure_efficiency
            FROM gold.mart_outfield_scouting_summary
            WHERE player_key = @ResolvedPlayerKey
        )
        SELECT TOP (@TopN)
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
            tgt.player_key, tgt.player_name, tgt.age, tgt.position, tgt.club_name, tgt.league,
            tgt.wage_per_week_k, tgt.transfer_value_m, tgt.minutes_played,
            tgt.passes_attempted_per90, tgt.pass_accuracy, tgt.progressive_passes_per90,
            tgt.prog_rate_per_pass, tgt.turnover_rate, tgt.npxg_plus_xa_per90,
            tgt.defensive_actions_per90, tgt.pressure_efficiency, tgt.cost_per_xgi
        FROM gold.mart_outfield_scouting_summary AS tgt
        CROSS JOIN bmk
        WHERE tgt.is_own_squad = 0 AND tgt.position IN ('Central Midfielder', 'Defensive Midfielder')
          AND tgt.player_key != @ResolvedPlayerKey AND tgt.minutes_played >= @MinMinutes
        ORDER BY similarity_distance ASC;
    END
    ELSE IF @Position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder')
    BEGIN
        WITH bmk AS (
            SELECT TOP 1
                npxg_plus_xa_per90, dribbles_made_per90, completed_crosses_per90, open_play_cross_accuracy,
                xa_per_op_kp, sprints_per90, prog_rate_per_pass, goal_threat_share, pressure_efficiency
            FROM gold.mart_outfield_scouting_summary
            WHERE player_key = @ResolvedPlayerKey
        )
        SELECT TOP (@TopN)
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
            tgt.player_key, tgt.player_name, tgt.age, tgt.position, tgt.club_name, tgt.league,
            tgt.wage_per_week_k, tgt.transfer_value_m, tgt.minutes_played,
            tgt.npxg_plus_xa_per90, tgt.dribbles_made_per90, tgt.completed_crosses_per90,
            tgt.open_play_cross_accuracy, tgt.xa_per_op_kp, tgt.sprints_per90, tgt.goal_threat_share, tgt.cost_per_xgi
        FROM gold.mart_outfield_scouting_summary AS tgt
        CROSS JOIN bmk
        WHERE tgt.is_own_squad = 0 AND tgt.position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder')
          AND tgt.player_key != @ResolvedPlayerKey AND tgt.minutes_played >= @MinMinutes
        ORDER BY similarity_distance ASC;
    END
    ELSE IF @Position = 'Central Attacking Midfielder'
    BEGIN
        WITH bmk AS (
            SELECT TOP 1
                npxg_plus_xa_per90, xa_per_op_kp, open_play_key_passes_per90, prog_rate_per_pass,
                dribbles_made_per90, turnover_rate, goal_threat_share, pressure_efficiency
            FROM gold.mart_outfield_scouting_summary
            WHERE player_key = @ResolvedPlayerKey
        )
        SELECT TOP (@TopN)
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
            tgt.player_key, tgt.player_name, tgt.age, tgt.position, tgt.club_name, tgt.league,
            tgt.wage_per_week_k, tgt.transfer_value_m, tgt.minutes_played,
            tgt.npxg_plus_xa_per90, tgt.xa_per_op_kp, tgt.open_play_key_passes_per90,
            tgt.dribbles_made_per90, tgt.prog_rate_per_pass, tgt.goal_threat_share, tgt.cost_per_xgi
        FROM gold.mart_outfield_scouting_summary AS tgt
        CROSS JOIN bmk
        WHERE tgt.is_own_squad = 0 AND tgt.position IN ('Central Attacking Midfielder', 'Right Attacking Midfielder', 'Left Attacking Midfielder')
          AND tgt.player_key != @ResolvedPlayerKey AND tgt.minutes_played >= @MinMinutes
        ORDER BY similarity_distance ASC;
    END
    ELSE -- Default to Striker engine
    BEGIN
        WITH bmk AS (
            SELECT TOP 1
                non_penalty_xGoals_per90, shot_on_target_rate, aerial_duels_won_per90,
                heading_accuracy, pressure_efficiency, xGoals_per_shot, goal_threat_share
            FROM gold.mart_outfield_scouting_summary
            WHERE player_key = @ResolvedPlayerKey
        )
        SELECT TOP (@TopN)
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
            tgt.player_key, tgt.player_name, tgt.age, tgt.club_name, tgt.league,
            tgt.wage_per_week_k, tgt.transfer_value_m, tgt.minutes_played,
            tgt.goals_per90, tgt.non_penalty_xGoals_per90, tgt.finishing_overperf_shrunk,
            tgt.goal_threat_share, tgt.shot_on_target_rate, tgt.xGoals_per_shot,
            tgt.aerial_duels_won_per90, tgt.heading_accuracy, tgt.pressure_efficiency, tgt.cost_per_xgi
        FROM gold.mart_outfield_scouting_summary AS tgt
        CROSS JOIN bmk
        WHERE tgt.is_own_squad = 0 AND tgt.position = 'Striker'
          AND tgt.player_key != @ResolvedPlayerKey AND tgt.minutes_played >= @MinMinutes
        ORDER BY similarity_distance ASC;
    END
END;
GO