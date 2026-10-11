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
					SELECT league FROM silver.fmdata_team_players WHERE league IS NOT NULL AND TRIM(league) != ''
					UNION
					SELECT league FROM silver.fmdata_team_gks WHERE league IS NOT NULL AND TRIM(league) != ''
					UNION
					SELECT league FROM silver.fmdata_interested_out_players WHERE league IS NOT NULL AND TRIM(league) != ''
					UNION
					SELECT league FROM silver.fmdata_interested_gks WHERE league IS NOT NULL AND TRIM(league) != ''
				) AS all_leagues
			)
			INSERT INTO gold.dim_league (league, league_difficulty_coef)
			SELECT 
				league,
				CASE 
					-- =================================================================
					-- 1. APEX TIER: PREMIER LEAGUE / WORLD SUPER LEAGUE (1.00)
					-- =================================================================
					WHEN league IN ('Premier League', 'English Premier Division', 'World Super League', 'WSL Premier League') THEN 1.00

					-- =================================================================
					-- 2. STEP 6 / SUPER LEAGUE 5 (0.28 - 0.26)
					-- =================================================================
					WHEN league LIKE '%National North%' OR league LIKE '%National South%'
					  OR league LIKE '%National League North%' OR league LIKE '%National League South%' THEN 0.28
					WHEN league LIKE '%National East%' OR league LIKE '%National West%'
					  OR league LIKE '%Super League 5%' THEN 0.26

					-- =================================================================
					-- 3. STEP 5 / SUPER LEAGUE 4 (0.44 - 0.36)
					-- =================================================================
					WHEN league IN ('National', 'National WSL', 'Vanarama National League', 'National League', 'English National League') THEN 0.44
					WHEN league LIKE '%Bundesliga 4%' OR league LIKE '%Regionalliga%' THEN 0.42
					WHEN league LIKE '%LaLiga 4%' OR league LIKE '%Segunda Federación%' OR league LIKE '%Spanish Federation Second%' THEN 0.41
					WHEN league LIKE '%Ligue 4%' OR league LIKE '%National 2%' OR league LIKE '%French National 2%' THEN 0.40
					WHEN league LIKE '%Brasileiro Série D%' OR league LIKE '%Brasileiro Serie D%'
					  OR league LIKE '%Brazilian National Fourth%' THEN 0.37
					WHEN league LIKE '%Serie D%' OR league LIKE '%Super League 4%' THEN 0.39
					WHEN league LIKE '%Liga Portugal 3%' OR league LIKE '%Liga 3%' THEN 0.38
					WHEN league LIKE '%Tercera%' THEN 0.36

					-- =================================================================
					-- 4. STEP 4 / SUPER LEAGUE 3 (0.62 - 0.50)
					-- =================================================================
					WHEN league LIKE '%Liga MX%' OR league LIKE '%Mexican First%' THEN 0.62
					WHEN league LIKE '%Bundesliga 3%' OR league LIKE '%3. Liga%' THEN 0.58
					WHEN league LIKE '%Scottish Premiership%' OR league LIKE '%SPFL%' THEN 0.58
					WHEN league LIKE '%Brasileiro Série C%' OR league LIKE '%Brasileiro Serie C%'
					  OR league LIKE '%Brazilian National Third%' THEN 0.53
					WHEN league LIKE '%League Two%' OR league LIKE '%LaLiga 3%'
					  OR league LIKE '%Primera Federación%' OR league LIKE '%Spanish Federation First%'
					  OR league LIKE '%Serie C%' OR league LIKE '%Super League 3%' THEN 0.56
					WHEN league LIKE '%Ligue 3%' OR league IN ('Championnat National', 'French National') THEN 0.55
					WHEN league LIKE '%Liga Portugal 2%' OR league LIKE '%LigaPro%'
					  OR league LIKE '%Allsvenskan%' OR league LIKE '%Ekstraklasa%' THEN 0.54
					WHEN league LIKE '%1. Lig%' THEN 0.53
					WHEN league LIKE '%Segunda División%' OR league LIKE '%Primera Nacional%'
					  OR league LIKE '%Eerste Divisie%' OR league LIKE '%Keuken Kampioen%' THEN 0.52
					WHEN league LIKE '%A-League%' THEN 0.51
					WHEN league LIKE '%Prva Liga%' THEN 0.50

					-- =================================================================
					-- 5. STEP 3 / SUPER LEAGUE 2 & SECONDARY EUROPEAN TOP TIERS (0.76 - 0.68)
					-- =================================================================
					WHEN league LIKE '%Eredivisie%' OR league LIKE '%Dutch%' THEN 0.76
					WHEN league LIKE '%Liga Portugal%' OR league LIKE '%Primeira Liga%' OR league LIKE '%Portuguese First%' THEN 0.75
					WHEN league LIKE '%Jupiler%' OR league LIKE '%Belgian Pro%' THEN 0.74
					WHEN league IN ('SAF', 'Superliga Argentina', 'Liga Profesional de Fútbol', 'Argentine Premier Division')
					  OR league LIKE '%Superliga Argentina%' OR league LIKE '%Argentine Primera%' THEN 0.73
					WHEN league LIKE '%Bundesliga 2%' OR league LIKE '%2. Bundesliga%'
					  OR league LIKE '%Austrian%' OR league LIKE '%ADMIRAL Bundesliga%'
					  OR league LIKE '%Super League 2%' THEN 0.72
					WHEN league LIKE '%LaLiga 2%' OR league LIKE '%Russian Premier%'
					  OR league LIKE '%Swiss Super League%' OR league LIKE '%3F Superliga%' THEN 0.71
					WHEN league LIKE '%Brasileiro Série B%' OR league LIKE '%Brasileiro Serie B%'
					  OR league LIKE '%Brazilian National Second%' THEN 0.69
					WHEN league LIKE '%Serie B%' OR league LIKE '%Süper Lig%' OR league LIKE '%Super Lig%'
					  OR league LIKE '%Turkish Super%' THEN 0.70
					WHEN league LIKE '%League One%' OR league LIKE '%Ligue 2%' THEN 0.69
					WHEN league LIKE '%MLS%' OR league LIKE '%Major League Soccer%' THEN 0.68

					-- =================================================================
					-- 6. STEP 2 / SUPER LEAGUE 1 & BIG 5 TOP FLIGHTS (0.90 - 0.80)
					-- =================================================================
					WHEN league IN ('La Liga', 'LaLiga', 'LALIGA EA SPORTS', 'Spanish First Division')
					  OR league LIKE '%LaLiga%' OR league LIKE '%La Liga%' THEN 0.90
					WHEN league IN ('Bundesliga', '1. Bundesliga', 'German First Division')
					  OR league LIKE '%Bundesliga%' OR league LIKE '%Super League 1%' THEN 0.88
					WHEN league LIKE '%Brasileiro Série A%' OR league LIKE '%Brasileiro Serie A%'
					  OR league LIKE '%Brazilian National First%' THEN 0.82
					WHEN league IN ('Serie A', 'Serie A TIM', 'Italian Serie A')
					  OR league LIKE '%Serie A%' THEN 0.87
					WHEN league IN ('Ligue 1', 'Ligue 1 Uber Eats', 'Ligue 1 McDonald''s')
					  OR league LIKE '%Ligue 1%' THEN 0.84
					WHEN league LIKE '%Championship%' THEN 0.80
					WHEN league LIKE '%Brasileiro%' OR league LIKE '%Brasileirão%' THEN 0.82

					-- =================================================================
					-- 7. DEFAULT FALLBACK
					-- =================================================================
					ELSE 0.65
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
					SELECT club_name FROM silver.fmdata_team_players WHERE club_name IS NOT NULL AND TRIM(club_name) != ''
					UNION
					SELECT club_name FROM silver.fmdata_team_gks WHERE club_name IS NOT NULL AND TRIM(club_name) != ''
					UNION
					SELECT club_name FROM silver.fmdata_interested_out_players WHERE club_name IS NOT NULL AND TRIM(club_name) != ''
					UNION
					SELECT club_name FROM silver.fmdata_interested_gks WHERE club_name IS NOT NULL AND TRIM(club_name) != ''
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
					SELECT club_name, league FROM silver.fmdata_team_players WHERE club_name IS NOT NULL AND league IS NOT NULL
					UNION
					SELECT club_name, league FROM silver.fmdata_team_gks WHERE club_name IS NOT NULL AND league IS NOT NULL
					UNION
					SELECT club_name, league FROM silver.fmdata_interested_out_players WHERE club_name IS NOT NULL AND league IS NOT NULL
					UNION
					SELECT club_name, league FROM silver.fmdata_interested_gks WHERE club_name IS NOT NULL AND league IS NOT NULL
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
						SELECT position FROM silver.fmdata_team_players WHERE position IS NOT NULL
						UNION
						SELECT position FROM silver.fmdata_team_gks WHERE position IS NOT NULL
						UNION
						SELECT position FROM silver.fmdata_interested_out_players WHERE position IS NOT NULL
						UNION
						SELECT position FROM silver.fmdata_interested_gks WHERE position IS NOT NULL
					) AS Q
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
					SELECT
						A.player_id,
						A.player_name,
						B.team_key AS team_key,
						0 AS is_own_squad,
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
						1 AS is_own_squad,
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
						0 AS is_own_squad,
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
						1 AS is_own_squad,
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
				AND (
					S.team_key != N.team_key
					OR S.age != N.age
					OR S.position != N.position
					OR S.is_own_squad != N.is_own_squad
					OR S.contracted != N.contracted
				)
				AND S.dwh_current_validity = 1;

		PRINT '>>> Inserting New Records in GOLD.DIM_PLAYER_INFO';
			WITH position AS (
				SELECT DISTINCT(position) FROM (
						SELECT position FROM silver.fmdata_team_players WHERE position IS NOT NULL
						UNION
						SELECT position FROM silver.fmdata_team_gks WHERE position IS NOT NULL
						UNION
						SELECT position FROM silver.fmdata_interested_out_players WHERE position IS NOT NULL
						UNION
						SELECT position FROM silver.fmdata_interested_gks WHERE position IS NOT NULL
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
						is_own_squad,
						contracted,
						age,
						position
					FROM (
						SELECT
							A.player_id,
							A.player_name,
							B.team_key AS team_key,
							0 AS is_own_squad,
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
							1 AS is_own_squad,
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
							0 AS is_own_squad,
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
							1 AS is_own_squad,
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