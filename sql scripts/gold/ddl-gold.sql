/*
==========================================
DDL Script: Create tables for gold layer
==========================================
  This script creates the tables for ingestion in the gold schema,
  dropping them if they exist beforehand.
  Run script to re-define the DDL structure of the gold tables.
====================================================================
*/

--GOLD.DIM_LEAGUE
IF OBJECT_ID ('gold.dim_league', 'U') IS NOT NULL
	DROP TABLE gold.dim_league;

CREATE TABLE gold.dim_league (
	league_key INT IDENTITY(1, 1) NOT NULL,
	league NVARCHAR(35),
	league_difficulty_coef DECIMAL(3, 2) DEFAULT 1.00
)

--GOLD.DIM_CLUB
IF OBJECT_ID ('gold.dim_club', 'U') IS NOT NULL
	DROP TABLE gold.dim_club;

CREATE TABLE gold.dim_club (
	club_key INT IDENTITY(1, 1) NOT NULL,
	club NVARCHAR(35)
)

-- GOLD.DIM_TEAM_INFO
IF OBJECT_ID ('gold.dim_team_info', 'U') IS NOT NULL
	DROP TABLE gold.dim_team_info;

CREATE TABLE gold.dim_team_info(
	team_key VARCHAR(10) NOT NULL,
	league NVARCHAR(35),
	club_name NVARCHAR(35),
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
)

-- Clean up unused possession table if present
IF OBJECT_ID ('gold.fact_teams_possession', 'U') IS NOT NULL
	DROP TABLE gold.fact_teams_possession;

-- GOLD.DIM_TEAM_TACTICAL_ARCHETYPES
IF OBJECT_ID ('gold.dim_team_tactical_archetypes', 'U') IS NOT NULL
	DROP TABLE gold.dim_team_tactical_archetypes;

CREATE TABLE gold.dim_team_tactical_archetypes (
	team_key VARCHAR(10) NOT NULL,
	club_name NVARCHAR(35) NOT NULL,
	league NVARCHAR(35),
	active_players INT,
	total_minutes INT,
	tactical_archetype VARCHAR(35) NOT NULL,
	tactical_framework VARCHAR(50) NOT NULL,
	tactical_description NVARCHAR(500),

	-- Team Aggregated Metrics (Minutes-Weighted Per 90)
	pressures_attempted_per90 DECIMAL(5, 2),
	pressures_completed_per90 DECIMAL(5, 2),
	possession_won_per90 DECIMAL(5, 2),
	possession_lost_per90 DECIMAL(5, 2),
	passes_attempted_per90 DECIMAL(5, 2),
	pass_accuracy DECIMAL(5, 3),
	progressive_passes_per90 DECIMAL(5, 2),
	open_play_key_passes_per90 DECIMAL(5, 2),
	chances_created_per90 DECIMAL(5, 2),
	xGoals_per_shot DECIMAL(5, 2),
	crosses_attempted_per90 DECIMAL(5, 2),
	open_play_crosses_attempted_per90 DECIMAL(5, 2),
	headers_attempted_per90 DECIMAL(5, 2),
	heading_accuracy DECIMAL(5, 3),
	clearances_per90 DECIMAL(5, 2),
	blocks_per90 DECIMAL(5, 2),
	shots_blocked_per90 DECIMAL(5, 2),
	tackles_attempted_per90 DECIMAL(5, 2),
	interceptions_per90 DECIMAL(5, 2),
	dribbles_made_per90 DECIMAL(5, 2),
	sprints_per90 DECIMAL(5, 2),
	distance_covered_km_per90 DECIMAL(5, 2),
	fouls_made_per90 DECIMAL(5, 2),
	shots_per90 DECIMAL(5, 2),
	xGoals_per90 DECIMAL(5, 2),

	-- Tactical Archetype Affinity Scores (0 - 100)
	gegenpressing_score INT,
	tiki_taka_score INT,
	vertical_tiki_taka_score INT,
	fluid_counter_score INT,
	direct_counter_score INT,
	wing_play_score INT,
	route_one_score INT,
	catenaccio_score INT,

	season VARCHAR(9) DEFAULT '2023/24',
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

-- GOLD.DIM_PLAYER_INFO
IF OBJECT_ID ('gold.dim_player_info', 'U') IS NOT NULL
	DROP TABLE gold.dim_player_info;

CREATE TABLE gold.dim_player_info (
	player_key INT IDENTITY(1,1) NOT NULL,
	player_id INT NOT NULL,
	player_name VARCHAR(35),
	team_key VARCHAR(10),
	is_own_squad BIT DEFAULT ((0)),
	contracted BIT DEFAULT ((1)),
	age INT,
	position VARCHAR(28),
	season VARCHAR(9) DEFAULT '2023/24',
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1)),
)

-- GOLD.FACT_OUTFIELD_PLAYER_STATS
IF OBJECT_ID ('gold.fact_outfield_player_stats', 'U') IS NOT NULL
	DROP TABLE gold.fact_outfield_player_stats;

CREATE TABLE gold.fact_outfield_player_stats (
	player_key INT NOT NULL,
	team_key VARCHAR(10) NOT NULL,
	minutes_played INT,
	team_goals_scored_per90 DECIMAL(5, 2),
	team_goals_conceded_per90 DECIMAL(5, 2),	
	goals INT,	
	goals_outside_the_box INT,
	shots_per90 DECIMAL(5, 2),
	xGoals_per_shot DECIMAL(5, 2),	
	shot_accuracy DECIMAL(5, 2),
	shots_on_target_per90 DECIMAL(5, 2),
	shots_outside_the_box_per90 DECIMAL(5, 2),
	goals_per90 DECIMAL(5, 2),
	xGoals_per90 DECIMAL(5, 2),
	non_penalty_xGoals_per90 DECIMAL(5, 2),
	xGoals_overperformance DECIMAL(5, 2),
	conversion_rate DECIMAL(5, 2),
	assists INT,
	assists_per90 DECIMAL(5, 2),
	passes_attempted_per90 DECIMAL(5, 2),
	pass_accuracy DECIMAL(5, 2),
	xAssits_per90 DECIMAL(5, 2),
	open_play_key_passes_per90 DECIMAL(5, 2),
	chances_created_per90 DECIMAL(5, 2),
	dribbles_made_per90 DECIMAL(5, 2),
	progressive_passes_per90 DECIMAL(5, 2),
	open_play_crosses_attempted_per90 DECIMAL(5, 2),
	open_play_cross_accuracy DECIMAL(5, 2),
	crosses_attempted_per90 DECIMAL(5, 2),
	cross_accuracy DECIMAL(5, 2),
	tackles_attempted_per90 DECIMAL(5, 2),
	tackle_accuracy DECIMAL(5, 2),
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
	heading_accuracy DECIMAL(5, 2),
	key_headers_per90 DECIMAL(5, 2),
	sprints_per90 DECIMAL(5, 2),
	distance_covered_km_per90 DECIMAL(5, 2),
	mistakes_leading_to_goals_per90 DECIMAL(5, 2),
	fouls_made_per90 DECIMAL(5, 2),
	fouls_against_per90 DECIMAL(5, 2),
	yellow_cards_per90 DECIMAL(5, 2),
	red_cards_per90 DECIMAL(5, 2),
	season VARCHAR(9) DEFAULT '2023/24',
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
)

-- GOLD.FACT_GK_STATS
IF OBJECT_ID ('gold.fact_gk_stats', 'U') IS NOT NULL
	DROP TABLE gold.fact_gk_stats;

CREATE TABLE gold.fact_gk_stats (
	player_key INT NOT NULL,
	team_key VARCHAR(10) NOT NULL,
	minutes_played INT,
	team_goals_scored_per90 DECIMAL(5, 2),
	team_goals_conceded_per90 DECIMAL(5, 2),	
	goals_conceded_per90 DECIMAL(5, 2),	
	saves_made_per90 DECIMAL(5, 2),
	xGoals_prevented_per90 DECIMAL(5, 2),
	xSave_rate DECIMAL(5, 2),
	saves_tipped_per90 DECIMAL(5, 2), 
	saves_parried_per90 DECIMAL(5, 2), 
	saves_held_per90 DECIMAL(5, 2),
	saves_percentage DECIMAL(5, 2),
	passes_attempted_per90 DECIMAL(5, 2),
	pass_accuracy DECIMAL(5, 2),
	possession_won_per90 DECIMAL(5, 2),
	possession_lost_per90 DECIMAL(5, 2),
	interceptions_per90 DECIMAL(5, 2),
	clearances_per90 DECIMAL(5, 2),
	penalties_faced_per90 DECIMAL(5, 2),
	penalties_save_percentage DECIMAL(5, 2),
	distance_covered_km_per90 DECIMAL(5, 2),
	mistakes_leading_to_goals_per90 DECIMAL(5, 2),
	fouls_against_per90 DECIMAL(5, 2),
	season VARCHAR(9) DEFAULT '2023/24',
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
)

-- GOLD.FACT_PLAYERS_VALUE
IF OBJECT_ID ('gold.fact_players_value', 'U') IS NOT NULL
	DROP TABLE gold.fact_players_value;

CREATE TABLE gold.fact_players_value (
	player_key INT NOT NULL,
	team_key VARCHAR(10),
	wage_per_week_k DECIMAL(10, 2),
	transfer_value_m DECIMAL(10, 2),
	season VARCHAR(9) DEFAULT '2023/24',
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
	dwh_cd_valid_till DATETIME2 DEFAULT '9999-12-31 23:59:59.9999999',
	dwh_current_validity BIT DEFAULT ((1))
);

-- ======================================================================
-- AUDIT SCHEMA & ETL PIPELINE LOG
-- ======================================================================
IF SCHEMA_ID('audit') IS NULL
	EXEC('CREATE SCHEMA audit');
GO

IF OBJECT_ID('audit.etl_pipeline_log', 'U') IS NOT NULL
	DROP TABLE audit.etl_pipeline_log;

CREATE TABLE audit.etl_pipeline_log (
	log_id INT IDENTITY(1,1) PRIMARY KEY,
	batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
	pipeline_layer VARCHAR(20) NOT NULL, -- 'Bronze', 'Silver', 'Gold', 'Complete'
	step_name VARCHAR(100) NOT NULL,
	status VARCHAR(20) NOT NULL,        -- 'Started', 'Success', 'Failed'
	rows_affected INT DEFAULT 0,
	start_time DATETIME2 DEFAULT GETDATE(),
	end_time DATETIME2 NULL,
	duration_ms INT NULL,
	error_message NVARCHAR(4000) NULL
);