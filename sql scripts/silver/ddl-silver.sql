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
	player_id INT NOT NULL,
	player_name	NVARCHAR(50),
	club_name NVARCHAR(40),
	league NVARCHAR(50),
	position VARCHAR(28),
	age INT,
	minutes_played INT,
	team_goals_scored_per90 DECIMAL(5, 2),
	team_goals_conceded_per90 DECIMAL(5, 2),	
	goals DECIMAL(5, 2),	
	goals_outside_the_box DECIMAL(5, 2),
	shots_per90 DECIMAL(5, 2),	
	xGoals_per_shot DECIMAL(5, 2),	
	shot_accuracy DECIMAL(3, 2),
	shots_on_target_per90 DECIMAL(5, 2),
	shots_outside_the_box_per90 DECIMAL(5, 2),
	goals_per90 DECIMAL(5, 2),
	xGoals_per90 DECIMAL(5, 2),
	non_penalty_xGoals_per90 DECIMAL(5, 2),
	xGoals_overperformance DECIMAL(5, 2),
	conversion_rate DECIMAL(3, 2),
	assists DECIMAL(5, 2),
	assists_per90 DECIMAL(5, 2),
	passes_attempted_per90 DECIMAL(5, 2),
	pass_accuracy DECIMAL(3, 2),
	xAssits_per90 DECIMAL(5, 2),
	open_play_key_passes_per90 DECIMAL(5, 2),
	chances_created_per90 DECIMAL(5, 2),
	dribbles_made_per90 DECIMAL(5, 2),
	progressive_passes_per90 DECIMAL(5, 2),
	open_play_crosses_attempted_per90 DECIMAL(5, 2),
	open_play_cross_accuracy DECIMAL(3, 2),
	crosses_attempted_per90 DECIMAL(5, 2),
	cross_accuracy DECIMAL(3, 2),
	tackles_attempted_per90 DECIMAL(5, 2),
	tackle_accuracy DECIMAL(3, 2),
	pressures_attempted_per90 DECIMAL(5, 2),
	pressures_completed_per90 DECIMAL(5, 2),
	possession_won_per90 DECIMAL(5, 2),
	possession_lost_per90 DECIMAL(5, 2),
	key_tackles_per90 DECIMAL(5, 2),
	interceptions_per90 DECIMAL(5, 2),
	clearances_per90 DECIMAL(5, 2),
	blocks_per90 DECIMAL(5, 2),
	shots_blocked_per90 DECIMAL(5, 2),
	headers_attempted_per90 DECIMAL(5, 2),
	heading_accuracy DECIMAL(3, 2),
	key_headers_per90 DECIMAL(5, 2),
	sprints_per90 DECIMAL(5, 2),
	distance_covered_km_per90 DECIMAL(5, 2),
	mistakes_leading_to_goals_per90 DECIMAL(5, 2),
	fouls_made_per90 DECIMAL(5, 2),
	fouls_against_per90 DECIMAL(5, 2),
	yellow_cards_per90 DECIMAL(5, 2),
	red_cards_per90 DECIMAL(5, 2),
	wage_per_week_k DECIMAL(10, 2),
	transfer_value_m DECIMAL(10, 2),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

--
IF OBJECT_ID ('silver.fmdata_team_gks', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_team_gks;

CREATE TABLE silver.fmdata_team_gks(
	player_id INT NOT NULL,
	player_name	NVARCHAR(50),
	club_name NVARCHAR(40),
	league NVARCHAR(50),
	position VARCHAR(28),
	age INT,
	minutes_played INT,
	team_goals_scored_per90 DECIMAL(5, 2),
	team_goals_conceded_per90 DECIMAL(5, 2),	
	goals_conceded_per90 DECIMAL(5, 2),	
	saves_made_per90 DECIMAL(5, 2),
	xGoals_prevented_per90 DECIMAL(5, 2),
	xSave_rate DECIMAL(3, 2),
	saves_tipped_per90 DECIMAL(5, 2), 
	saves_parried_per90 DECIMAL(5, 2), 
	saves_held_per90 DECIMAL(5, 2),
	saves_percentage DECIMAL(3, 2),
	passes_attempted_per90 DECIMAL(5, 2),
	pass_accuracy DECIMAL(3, 2),
	possession_won_per90 DECIMAL(5, 2),
	possession_lost_per90 DECIMAL(5, 2),
	interceptions_per90 DECIMAL(5, 2),
	clearances_per90 DECIMAL(5, 2),
	penalties_faced_per90 DECIMAL(5, 2),
	penalties_save_percentage INT,
	distance_covered_km_per90 DECIMAL(5, 2),
	mistakes_leading_to_goals_per90 DECIMAL(5, 2),
	fouls_against_per90 DECIMAL(5, 2),
	wage_per_week_k DECIMAL(10, 2),
	transfer_value_m DECIMAL(10, 2),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

--
IF OBJECT_ID ('silver.fmdata_manager_data', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_manager_data;

CREATE TABLE silver.fmdata_manager_data(
	staff_id INT NOT NULL,
	staff_name NVARCHAR(30),
	club_name NVARCHAR(40),
	job_at_club VARCHAR(30),
	previous_club_name NVARCHAR(45),
	tactical_style VARCHAR(30),
	playing_mentality VARCHAR(30),
	preferred_formation VARCHAR(30),
	pressing_style VARCHAR(30),
	marking_style VARCHAR(30),
	contract_begins DATE,
	contract_expires DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

--
IF OBJECT_ID ('silver.fmdata_interested_out_players', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_interested_out_players;

CREATE TABLE silver.fmdata_interested_out_players(
	player_id INT NOT NULL,
	player_name	NVARCHAR(50),
	club_name NVARCHAR(40),
	league NVARCHAR(50),
	position VARCHAR(28),
	age INT,
	minutes_played INT,
	team_goals_scored_per90 DECIMAL(5, 2),
	team_goals_conceded_per90 DECIMAL(5, 2),	
	goals DECIMAL(5, 2),	
	goals_outside_the_box DECIMAL(5, 2),
	shots_per90 DECIMAL(5, 2),	
	xGoals_per_shot DECIMAL(5, 2),	
	shot_accuracy DECIMAL(3, 2),
	shots_on_target_per90 DECIMAL(5, 2),
	shots_outside_the_box_per90 DECIMAL(5, 2),
	goals_per90 DECIMAL(5, 2),
	xGoals_per90 DECIMAL(5, 2),
	non_penalty_xGoals_per90 DECIMAL(5, 2),
	xGoals_overperformance DECIMAL(5, 2),
	conversion_rate DECIMAL(3, 2),
	assists DECIMAL(5, 2),
	assists_per90 DECIMAL(5, 2),
	passes_attempted_per90 DECIMAL(5, 2),
	pass_accuracy DECIMAL(3, 2),
	xAssits_per90 DECIMAL(5, 2),
	open_play_key_passes_per90 DECIMAL(5, 2)	,
	chances_created_per90 DECIMAL(5, 2),
	dribbles_made_per90 DECIMAL(5, 2),
	progressive_passes_per90 DECIMAL(5, 2),
	open_play_crosses_attempted_per90 DECIMAL(5, 2),
	open_play_cross_accuracy DECIMAL(3, 2),
	crosses_attempted_per90 DECIMAL(5, 2),
	cross_accuracy DECIMAL(3, 2),
	tackles_attempted_per90 DECIMAL(5, 2),
	tackle_accuracy DECIMAL(3, 2),
	pressures_attempted_per90 DECIMAL(5, 2),
	pressures_completed_per90 DECIMAL(5, 2),
	possession_won_per90 DECIMAL(5, 2),
	possession_lost_per90 DECIMAL(5, 2),
	key_tackles_per90 DECIMAL(5, 2),
	interceptions_per90 DECIMAL(5, 2),
	clearances_per90 DECIMAL(5, 2),
	blocks_per90 DECIMAL(5, 2),
	shots_blocked_per90 DECIMAL(5, 2),
	headers_attempted_per90 DECIMAL(5, 2),
	heading_accuracy DECIMAL(3, 2),
	key_headers_per90 DECIMAL(5, 2),
	sprints_per90 DECIMAL(5, 2),
	distance_covered_km_per90 DECIMAL(5, 2),
	mistakes_leading_to_goals_per90 DECIMAL(5, 2),
	fouls_made_per90 DECIMAL(5, 2),
	fouls_against_per90 DECIMAL(5, 2),
	yellow_cards_per90 DECIMAL(5, 2),
	red_cards_per90 DECIMAL(5, 2),
	wage_per_week_k DECIMAL(10, 2),
	transfer_value_m DECIMAL(10, 2),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

--
IF OBJECT_ID ('silver.fmdata_interested_gks', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_interested_gks;

CREATE TABLE silver.fmdata_interested_gks(
	player_id INT NOT NULL,
	player_name	NVARCHAR(50),
	club_name NVARCHAR(40),
	league NVARCHAR(50),
	position VARCHAR(28),
	age INT,
	minutes_played INT,
	team_goals_scored_per90 DECIMAL(5, 2),
	team_goals_conceded_per90 DECIMAL(5, 2),	
	goals_conceded_per90 DECIMAL(5, 2),	
	saves_made_per90 DECIMAL(5, 2),
	xGoals_prevented_per90 DECIMAL(5, 2),
	xSave_rate DECIMAL(3, 2),
	saves_tipped_per90 DECIMAL(5, 2), 
	saves_parried_per90 DECIMAL(5, 2), 
	saves_held_per90 DECIMAL(5, 2),
	saves_percentage DECIMAL(3, 2),
	passes_attempted_per90 DECIMAL(5, 2),
	pass_accuracy DECIMAL(3, 2),
	possession_won_per90 DECIMAL(5, 2),
	possession_lost_per90 DECIMAL(5, 2),
	interceptions_per90 DECIMAL(5, 2),
	clearances_per90 DECIMAL(5, 2),
	penalties_faced_per90 DECIMAL(5, 2),
	penalties_save_percentage INT,
	distance_covered_km_per90 DECIMAL(5, 2),
	mistakes_leading_to_goals_per90 DECIMAL(5, 2),
	fouls_against_per90 DECIMAL(5, 2),
	wage_per_week_k DECIMAL(10, 2),
	transfer_value_m DECIMAL(10, 2),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

IF OBJECT_ID ('silver.fmdata_possession_data', 'U') IS NOT NULL
	DROP TABLE silver.fmdata_possession_data

CREATE TABLE silver.fmdata_possession_data(
	club_name NVARCHAR(40),
	average_possession INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);
