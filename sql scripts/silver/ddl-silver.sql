/*
==========================================
DDL Script: Create tables for silver layer
==========================================
  This script creates the tables for ingestion in the silver schema,
  dropping them if they exist beforehand.
  Run script to re-define the DDL structure of the silver tables.
====================================================================
*/

--
IF OBJECT_ID ('silver.fmdata_team_players', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_team_players;

CREATE TABLE silver.fmdata_team_players(
	player_id NVARCHAR(10),
	player_name	NVARCHAR(50),
	club_name NVARCHAR(40),
	league NVARCHAR(50),
	position NVARCHAR(40),
	age NVARCHAR(10),
	minutes_played NVARCHAR(10),
	team_goals_scored_per90 NVARCHAR(10),
	team_goals_conceded_per90 NVARCHAR(10),	
	goals NVARCHAR(10),	
	goals_outside_the_box NVARCHAR(10),
	shots_per90 NVARCHAR(10),	
	xGoals_per_shot NVARCHAR(10),	
	shot_accuracy NVARCHAR(5),
	shots_on_target_per90 NVARCHAR(10),
	shots_outside_the_box_per90 NVARCHAR(10),
	goals_per90 NVARCHAR(10),
	xGoals_per90 NVARCHAR(10),
	non_penalty_xGoals_per90 NVARCHAR(10),
	xGoals_overperformance NVARCHAR(10),
	conversion_rate NVARCHAR(5),
	assists NVARCHAR(10),
	assists_per90 NVARCHAR(10),
	passes_attempted_per90 NVARCHAR(10),
	pass_accuracy NVARCHAR(5),
	xAssits_per90 NVARCHAR(10),
	open_play_key_passes_per90 NVARCHAR(10)	,
	chances_created_per90 NVARCHAR(10),
	dribbles_made_per90 NVARCHAR(10),
	progressive_passes_per90 NVARCHAR(10),
	open_play_crosses_attempted_per90 NVARCHAR(10),
	open_play_cross_accuracy NVARCHAR(5),
	crosses_attempted_per90 NVARCHAR(10),
	cross_accuracy NVARCHAR(5),
	tackles_attempted_per90 NVARCHAR(10),
	tackle_accuracy NVARCHAR(5),
	pressures_attempted_per90 NVARCHAR(10),
	pressures_completed_per90 NVARCHAR(10),
	possession_won_per90 NVARCHAR(10),
	possession_lost_per90 NVARCHAR(10),
	key_tackles_per90 NVARCHAR(10),
	interceptions_per90 NVARCHAR(10),
	clearances_per90 NVARCHAR(10),
	blocks_per90 NVARCHAR(10),
	shots_blocked_per90 NVARCHAR(10),
	headers_attempted_per90 NVARCHAR(10),
	heading_accuracy NVARCHAR(5),
	key_headers_per90 NVARCHAR(10),
	sprints_per90 NVARCHAR(10),
	distance_covered_km_per90 NVARCHAR(10),
	mistakes_leading_to_goals_per90 NVARCHAR(10),
	fouls_made_per90 NVARCHAR(10),
	fouls_against_per90 NVARCHAR(10),
	yellow_cards_per90 NVARCHAR(10),
	red_cards_per90 NVARCHAR(10),
	wage_per_week_k NVARCHAR(20),
	transfer_value_m NVARCHAR(30),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

--
IF OBJECT_ID ('silver.fmdata_team_gks', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_team_gks;

CREATE TABLE silver.fmdata_team_gks(
	player_id NVARCHAR(10),
	player_name	NVARCHAR(50),
	club_name NVARCHAR(40),
	league NVARCHAR(50),
	position NVARCHAR(40),
	age NVARCHAR(10),
	minutes_played NVARCHAR(10),
	team_goals_scored_per90 NVARCHAR(10),
	team_goals_conceded_per90 NVARCHAR(10),	
	goals_conceded_per90 NVARCHAR(10),	
	saves_made_per90 NVARCHAR(10),
	xGoals_prevented_per90 NVARCHAR(10),
	xSave_rate NVARCHAR(5),
	saves_tipped_per90 NVARCHAR(10), 
	saves_parried_per90 NVARCHAR(10), 
	saves_held_per90 NVARCHAR(10),
	saves_percentage NVARCHAR(5),
	passes_attempted_per90 NVARCHAR(10),
	pass_accuracy NVARCHAR(5),
	possession_won_per90 NVARCHAR(10),
	possession_lost_per90 NVARCHAR(10),
	interceptions_per90 NVARCHAR(10),
	clearances_per90 NVARCHAR(10),
	penalties_faced_per90 NVARCHAR(10),
	penalties_save_percentage NVARCHAR(10),
	distance_covered_km_per90 NVARCHAR(10),
	mistakes_leading_to_goals_per90 NVARCHAR(10),
	fouls_against_per90 NVARCHAR(10),
	wage_per_week_k NVARCHAR(20),
	transfer_value_m NVARCHAR(30),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

--
IF OBJECT_ID ('silver.fmdata_manager_data', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_manager_data;

CREATE TABLE silver.fmdata_manager_data(
	staff_id NVARCHAR(10),
	staff_name NVARCHAR(30),
	club_name NVARCHAR(40),
	job_at_club NVARCHAR(30),
	previous_club_name NVARCHAR(45),
	tactical_style NVARCHAR(30),
	playing_mentality NVARCHAR(30),
	preferred_formation NVARCHAR(30),
	pressing_style NVARCHAR(30),
	marking_style NVARCHAR(30),
	contract_begins NVARCHAR(20),
	contract_expires NVARCHAR(20),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

--
IF OBJECT_ID ('silver.fmdata_interested_out_players', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_interested_out_players;

CREATE TABLE silver.fmdata_interested_out_players(
	player_id NVARCHAR(10),
	player_name	NVARCHAR(50),
	club_name NVARCHAR(40),
	league NVARCHAR(50),
	position NVARCHAR(40),
	age NVARCHAR(10),
	minutes_played NVARCHAR(10),
	team_goals_scored_per90 NVARCHAR(10),
	team_goals_conceded_per90 NVARCHAR(10),	
	goals NVARCHAR(10),	
	goals_outside_the_box NVARCHAR(10),
	shots_per90 NVARCHAR(10),	
	xGoals_per_shot NVARCHAR(10),	
	shot_accuracy NVARCHAR(5),
	shots_on_target_per90 NVARCHAR(10),
	shots_outside_the_box_per90 NVARCHAR(10),
	goals_per90 NVARCHAR(10),
	xGoals_per90 NVARCHAR(10),
	non_penalty_xGoals_per90 NVARCHAR(10),
	xGoals_overperformance NVARCHAR(10),
	conversion_rate NVARCHAR(5),
	assists NVARCHAR(10),
	assists_per90 NVARCHAR(10),
	passes_attempted_per90 NVARCHAR(10),
	pass_accuracy NVARCHAR(5),
	xAssits_per90 NVARCHAR(10),
	open_play_key_passes_per90 NVARCHAR(10)	,
	chances_created_per90 NVARCHAR(10),
	dribbles_made_per90 NVARCHAR(10),
	progressive_passes_per90 NVARCHAR(10),
	open_play_crosses_attempted_per90 NVARCHAR(10),
	open_play_cross_accuracy NVARCHAR(5),
	crosses_attempted_per90 NVARCHAR(10),
	cross_accuracy NVARCHAR(5),
	tackles_attempted_per90 NVARCHAR(10),
	tackle_accuracy NVARCHAR(5),
	pressures_attempted_per90 NVARCHAR(10),
	pressures_completed_per90 NVARCHAR(10),
	possession_won_per90 NVARCHAR(10),
	possession_lost_per90 NVARCHAR(10),
	key_tackles_per90 NVARCHAR(10),
	interceptions_per90 NVARCHAR(10),
	clearances_per90 NVARCHAR(10),
	blocks_per90 NVARCHAR(10),
	shots_blocked_per90 NVARCHAR(10),
	headers_attempted_per90 NVARCHAR(10),
	heading_accuracy NVARCHAR(5),
	key_headers_per90 NVARCHAR(10),
	sprints_per90 NVARCHAR(10),
	distance_covered_km_per90 NVARCHAR(10),
	mistakes_leading_to_goals_per90 NVARCHAR(10),
	fouls_made_per90 NVARCHAR(10),
	fouls_against_per90 NVARCHAR(10),
	yellow_cards_per90 NVARCHAR(10),
	red_cards_per90 NVARCHAR(10),
	wage_per_week_k NVARCHAR(20),
	transfer_value_m NVARCHAR(30),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

--
IF OBJECT_ID ('silver.fmdata_interested_gks', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_interested_gks;

CREATE TABLE silver.fmdata_interested_gks(
	player_id NVARCHAR(10),
	player_name	NVARCHAR(50),
	club_name NVARCHAR(40),
	league NVARCHAR(50),
	position NVARCHAR(40),
	age NVARCHAR(10),
	minutes_played NVARCHAR(10),
	team_goals_scored_per90 NVARCHAR(10),
	team_goals_conceded_per90 NVARCHAR(10),	
	goals_conceded_per90 NVARCHAR(10),	
	saves_made_per90 NVARCHAR(10),
	xGoals_prevented_per90 NVARCHAR(10),
	xSave_rate NVARCHAR(5),
	saves_tipped_per90 NVARCHAR(10), 
	saves_parried_per90 NVARCHAR(10), 
	saves_held_per90 NVARCHAR(10),
	saves_percentage NVARCHAR(5),
	passes_attempted_per90 NVARCHAR(10),
	pass_accuracy NVARCHAR(5),
	possession_won_per90 NVARCHAR(10),
	possession_lost_per90 NVARCHAR(10),
	interceptions_per90 NVARCHAR(10),
	clearances_per90 NVARCHAR(10),
	penalties_faced_per90 NVARCHAR(10),
	penalties_save_percentage NVARCHAR(10),
	distance_covered_km_per90 NVARCHAR(10),
	mistakes_leading_to_goals_per90 NVARCHAR(10),
	fouls_against_per90 NVARCHAR(10),
	wage_per_week_k NVARCHAR(20),
	transfer_value_m NVARCHAR(30),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

IF OBJECT_ID ('silver.fmdata_possession_data', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_possession_data

CREATE TABLE silver.fmdata_possession_data(
	club_name NVARCHAR(40),
	average_possession NVARCHAR(10),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);
