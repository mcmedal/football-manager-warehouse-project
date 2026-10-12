/*
==========================================
DDL Script: Create tables for bronze layer
==========================================
  This script creates the tables for ingestion in the bronze schema,
  dropping them if they exist beforehand.
  Run script to re-define the DDL structure of the bronze tables.
====================================================================
*/

--
IF OBJECT_ID ('bronze.fmdata_team_players', 'U') IS NOT NULL
	DROP TABLE bronze.fmdata_team_players;

CREATE TABLE bronze.fmdata_team_players(
	player_id VARCHAR(20),
	player_name	VARCHAR(100),
	club_name VARCHAR(60),
	league VARCHAR(60),
	position VARCHAR(30),
	age VARCHAR(20),
	minutes_played VARCHAR(20),
	team_goals_scored_per90 VARCHAR(20),
	team_goals_conceded_per90 VARCHAR(20),	
	goals VARCHAR(20),	
	goals_outside_the_box VARCHAR(20),
	shots_per90 VARCHAR(20),	
	xGoals_per_shot VARCHAR(20),	
	shot_accuracy VARCHAR(20),
	shots_on_target_per90 VARCHAR(20),
	shots_outside_the_box_per90 VARCHAR(20),
	goals_per90 VARCHAR(20),
	xGoals_per90 VARCHAR(20),
	non_penalty_xGoals_per90 VARCHAR(20),
	xGoals_overperformance VARCHAR(20),
	conversion_rate VARCHAR(20),
	assists VARCHAR(20),
	assists_per90 VARCHAR(20),
	passes_attempted_per90 VARCHAR(20),
	pass_accuracy VARCHAR(20),
	xAssits_per90 VARCHAR(20),
	open_play_key_passes_per90 VARCHAR(20),
	chances_created_per90 VARCHAR(20),
	dribbles_made_per90 VARCHAR(20),
	progressive_passes_per90 VARCHAR(20),
	open_play_crosses_attempted_per90 VARCHAR(20),
	open_play_cross_accuracy VARCHAR(20),
	crosses_attempted_per90 VARCHAR(20),
	cross_accuracy VARCHAR(20),
	tackles_attempted VARCHAR(20),
	tackle_accuracy VARCHAR(20),
	pressures_attempted_per90 VARCHAR(20),
	pressures_completed_per90 VARCHAR(20),
	possession_won_per90 VARCHAR(20),
	possession_lost_per90 VARCHAR(20),
	key_tackles_per90 VARCHAR(20),
	interceptions_per90 VARCHAR(20),
	clearances_per90 VARCHAR(20),
	blocks_per90 VARCHAR(20),
	shots_blocked_per90 VARCHAR(20),
	headers_attempted_per90 VARCHAR(20),
	heading_accuracy VARCHAR(20),
	key_headers_per90 VARCHAR(20),
	sprints_per90 VARCHAR(20),
	distance_covered_per90 VARCHAR(20),
	mistakes_leading_to_goals VARCHAR(20),
	fouls_made VARCHAR(20),
	fouls_against VARCHAR(20),
	yellow_cards VARCHAR(20),
	red_cards VARCHAR(20),
	wage_per_week VARCHAR(50),
	transfer_value VARCHAR(50)
);

--
IF OBJECT_ID ('bronze.fmdata_team_gks', 'U') IS NOT NULL
	DROP TABLE bronze.fmdata_team_gks;

CREATE TABLE bronze.fmdata_team_gks(
	player_id VARCHAR(20),
	player_name	VARCHAR(100),
	position VARCHAR(30),
	age VARCHAR(20),
	club_name VARCHAR(60),
	league VARCHAR(60),
	minutes_played VARCHAR(20),
	team_goals_scored_per90 VARCHAR(20),
	team_goals_conceded_per90 VARCHAR(20),	
	goals_conceded_per90 VARCHAR(20),	
	saves_made_per90 VARCHAR(20),
	xGoals_prevented_per90 VARCHAR(20),
	xSave_rate VARCHAR(20),
	saves_tipped VARCHAR(20),
	saves_parried VARCHAR(20),
	saves_held VARCHAR(20),
	saves_percentage VARCHAR(20),
	passes_attempted_per90 VARCHAR(20),
	pass_accuracy VARCHAR(20),
	possession_won_per90 VARCHAR(20),
	possession_lost_per90 VARCHAR(20),
	interceptions_per90 VARCHAR(20),
	clearances_per90 VARCHAR(20),
	penalties_faced VARCHAR(20),
	penalties_save_percentage VARCHAR(20),
	distance_covered_per90 VARCHAR(20),
	mistakes_leading_to_goals VARCHAR(20),
	fouls_against VARCHAR(20),
	wage_per_week VARCHAR(50),
	transfer_value VARCHAR(50)
);

--
-- Drop legacy partition tables if present
IF OBJECT_ID ('bronze.fmdata_interested_out_players1', 'U') IS NOT NULL DROP TABLE bronze.fmdata_interested_out_players1;
IF OBJECT_ID ('bronze.fmdata_interested_out_players2', 'U') IS NOT NULL DROP TABLE bronze.fmdata_interested_out_players2;
IF OBJECT_ID ('bronze.fmdata_interested_out_players3', 'U') IS NOT NULL DROP TABLE bronze.fmdata_interested_out_players3;

--
IF OBJECT_ID ('bronze.fmdata_interested_out_players', 'U') IS NOT NULL
	DROP TABLE bronze.fmdata_interested_out_players;

CREATE TABLE bronze.fmdata_interested_out_players(
	record VARCHAR(20),
	info VARCHAR(20),
	player_id VARCHAR(20),
	player_name	VARCHAR(100),
	position VARCHAR(30),
	age VARCHAR(20),
	club_name VARCHAR(60),
	league VARCHAR(60),
	minutes_played VARCHAR(20),
	team_goals_scored_per90 VARCHAR(20),
	team_goals_conceded_per90 VARCHAR(20),	
	goals VARCHAR(20),	
	goals_outside_the_box VARCHAR(20),
	shots_per90 VARCHAR(20),	
	xGoals_per_shot VARCHAR(20),	
	shot_accuracy VARCHAR(20),
	shots_on_target_per90 VARCHAR(20),
	shots_outside_the_box_per90 VARCHAR(20),
	goals_per90 VARCHAR(20),
	xGoals_per90 VARCHAR(20),
	non_penalty_xGoals_per90 VARCHAR(20),
	xGoals_overperformance VARCHAR(20),
	conversion_rate VARCHAR(20),
	assists VARCHAR(20),
	assists_per90 VARCHAR(20),
	passes_attempted_per90 VARCHAR(20),
	pass_accuracy VARCHAR(20),
	xAssits_per90 VARCHAR(20),
	open_play_key_passes_per90 VARCHAR(20),
	chances_created_per90 VARCHAR(20),
	dribbles_made_per90 VARCHAR(20),
	progressive_passes_per90 VARCHAR(20),
	open_play_crosses_attempted_per90 VARCHAR(20),
	open_play_cross_accuracy VARCHAR(20),
	crosses_attempted_per90 VARCHAR(20),
	cross_accuracy VARCHAR(20),
	tackles_attempted VARCHAR(20),
	tackle_accuracy VARCHAR(20),
	pressures_attempted_per90 VARCHAR(20),
	pressures_completed_per90 VARCHAR(20),
	possession_won_per90 VARCHAR(20),
	possession_lost_per90 VARCHAR(20),
	key_tackles_per90 VARCHAR(20),
	interceptions_per90 VARCHAR(20),
	clearances_per90 VARCHAR(20),
	blocks_per90 VARCHAR(20),
	shots_blocked_per90 VARCHAR(20),
	headers_attempted_per90 VARCHAR(20),
	heading_accuracy VARCHAR(20),
	key_headers_per90 VARCHAR(20),
	sprints_per90 VARCHAR(20),
	distance_covered_per90 VARCHAR(20),
	mistakes_leading_to_goals VARCHAR(20),
	fouls_made VARCHAR(20),
	fouls_against VARCHAR(20),
	yellow_cards VARCHAR(20),
	red_cards VARCHAR(20),
	wage_per_week VARCHAR(50),
	transfer_value VARCHAR(50)
);

-- Backward-compatibility views for legacy queries
IF OBJECT_ID ('bronze.fmdata_interested_out_players1', 'V') IS NOT NULL DROP VIEW bronze.fmdata_interested_out_players1;
GO
CREATE VIEW bronze.fmdata_interested_out_players1 AS SELECT * FROM bronze.fmdata_interested_out_players;
GO
IF OBJECT_ID ('bronze.fmdata_interested_out_players2', 'V') IS NOT NULL DROP VIEW bronze.fmdata_interested_out_players2;
GO
CREATE VIEW bronze.fmdata_interested_out_players2 AS SELECT * FROM bronze.fmdata_interested_out_players;
GO
IF OBJECT_ID ('bronze.fmdata_interested_out_players3', 'V') IS NOT NULL DROP VIEW bronze.fmdata_interested_out_players3;
GO
CREATE VIEW bronze.fmdata_interested_out_players3 AS SELECT * FROM bronze.fmdata_interested_out_players;
GO

--
IF OBJECT_ID ('bronze.fmdata_interested_gks', 'U') IS NOT NULL
	DROP TABLE bronze.fmdata_interested_gks;

CREATE TABLE bronze.fmdata_interested_gks(
	record VARCHAR(20),
	info VARCHAR(20),
	player_id VARCHAR(20),
	player_name	VARCHAR(100),
	position VARCHAR(30),
	age VARCHAR(20),
	club_name VARCHAR(60),
	league VARCHAR(60),
	minutes_played VARCHAR(20),
	team_goals_scored_per90 VARCHAR(20),
	team_goals_conceded_per90 VARCHAR(20),	
	goals_conceded_per90 VARCHAR(20),	
	saves_made_per90 VARCHAR(20),
	xGoals_prevented_per90 VARCHAR(20),
	xSave_rate VARCHAR(20),
	saves_tipped VARCHAR(20),
	saves_parried VARCHAR(20),
	saves_held VARCHAR(20),
	saves_percentage VARCHAR(20),
	passes_attempted_per90 VARCHAR(20),
	pass_accuracy VARCHAR(20),
	possession_won_per90 VARCHAR(20),
	possession_lost_per90 VARCHAR(20),
	interceptions_per90 VARCHAR(20),
	clearances_per90 VARCHAR(20),
	penalties_faced VARCHAR(20),
	penalties_save_percentage VARCHAR(20),
	distance_covered_per90 VARCHAR(20),
	mistakes_leading_to_goals VARCHAR(20),
	fouls_against VARCHAR(20),
	wage_per_week VARCHAR(50),
	transfer_value VARCHAR(50)
);
