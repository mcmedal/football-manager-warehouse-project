/*
======================================================================
This script contains some of the tests made when checking the quality 
of loaded data into the silver schema.
======================================================================
*/

--- Tests for FMDATA_TEAM_PLAYERS
-- Checking for unnecessary text
SELECT 
player_id,
player_name
FROM silver.fmdata_team_players
WHERE player_name LIKE '% - Pick Player';

-- Checking for Consistency
SELECT 
DISTINCT(league)
FROM silver.fmdata_team_players
WHERE UPPER(league) != league;

-- Checking for Standardization
SELECT 
age
FROM silver.fmdata_team_players
WHERE age < 16 OR age > 45;

SELECT 
distance_covered_km_per90
FROM silver.fmdata_team_players
WHERE minutes_played = '-';

-- Check for Correct Calculation
SELECT 
	O.minutes_played,
	O.tackles_attempted,
	N.tackles_attempted_per90,
	O.mistakes_leading_to_goals,
	N.mistakes_leading_to_goals_per90,
	O.fouls_made,
	N.fouls_made_per90,
	O.fouls_against,
	N.fouls_against_per90,
	O.yellow_cards,
	N.yellow_cards_per90,
	O.red_cards,
	N.red_cards_per90
FROM bronze.fmdata_team_players AS O
INNER JOIN silver.fmdata_team_players AS N ON O.player_id = N.player_id;

-- Check for Clarity
SELECT 
	O.transfer_value,
	N.transfer_value_m
FROM bronze.fmdata_team_players AS O
INNER JOIN silver.fmdata_team_players AS N ON O.player_id = N.player_id;

-- Check for Clarity
SELECT 
	O.wage_per_week,
	N.wage_per_week_k
FROM bronze.fmdata_team_players AS O
INNER JOIN silver.fmdata_team_players AS N ON O.player_id = N.player_id;


--- Tests for FMDATA_TEAM_GKS
-- Checking for unnecessary text
SELECT 
player_id,
player_name
FROM silver.fmdata_team_gks
WHERE player_name LIKE '% - Pick Player';

-- Checking for Consistency
SELECT 
DISTINCT(league)
FROM silver.fmdata_team_gks
WHERE UPPER(league) != league;

-- Checking for Standardization
SELECT 
age
FROM silver.fmdata_team_gks
WHERE age < 16 OR age > 45;

SELECT 
distance_covered_km_per90
FROM silver.fmdata_team_gks
WHERE minutes_played = '-';

-- Check for Correct Calculation
SELECT 
	O.minutes_played,
	O.saves_parried,
	N.saves_parried_per90,
	O.saves_held,
	N.saves_held_per90,
	O.saves_tipped,
	N.saves_tipped_per90,
	O.fouls_against,
	N.fouls_against_per90
FROM bronze.fmdata_team_gks AS O
INNER JOIN silver.fmdata_team_gks AS N ON O.player_id = N.player_id;

-- Check for Clarity
SELECT 
	O.transfer_value,
	N.transfer_value_m
FROM bronze.fmdata_team_gks AS O
INNER JOIN silver.fmdata_team_gks AS N ON O.player_id = N.player_id;

-- Check for Clarity
SELECT 
	O.wage_per_week,
	N.wage_per_week_k
FROM bronze.fmdata_team_gks AS O
INNER JOIN silver.fmdata_team_gks AS N ON O.player_id = N.player_id;


--- Tests for FMDATA_MANAGER_DATA
SELECT
DISTINCT(tactical_style)
FROM silver.fmdata_manager_data;

SELECT
DISTINCT(playing_mentality)
FROM silver.fmdata_manager_data;

SELECT
DISTINCT(preferred_formation)
FROM silver.fmdata_manager_data;

SELECT
DISTINCT(pressing_style)
FROM silver.fmdata_manager_data;

SELECT
DISTINCT(marking_style)
FROM silver.fmdata_manager_data;

SELECT
DISTINCT(YEAR(contract_begins)) AS CB
FROM silver.fmdata_manager_data
ORDER BY CB;

SELECT
DISTINCT(YEAR(contract_expires)) AS CB
FROM silver.fmdata_manager_data
ORDER BY CB;

SELECT
	preferred_formation
FROM silver.fmdata_manager_data
WHERE preferred_formation = '4/4/2002';

--- Tests for FMDATA_INTERESTED_OUT_PLAYERS

-- Checking for Consistency
SELECT 
DISTINCT(club_name)
FROM silver.fmdata_interested_out_players;

-- Checking for Consistency
SELECT 
DISTINCT(league)
FROM silver.fmdata_interested_out_players
WHERE UPPER(league) != league;

-- Checking for Standardization
SELECT 
age
FROM silver.fmdata_interested_out_players
WHERE age < 16 OR age > 45;

SELECT 
distance_covered_km_per90
FROM silver.fmdata_interested_out_players
WHERE minutes_played = '-';

-- Check for Correct Calculation
SELECT 
	O.minutes_played,
	O.tackles_attempted,
	N.tackles_attempted_per90,
	O.mistakes_leading_to_goals,
	N.mistakes_leading_to_goals_per90,
	O.fouls_made,
	N.fouls_made_per90,
	O.fouls_against,
	N.fouls_against_per90,
	O.yellow_cards,
	N.yellow_cards_per90,
	O.red_cards,
	N.red_cards_per90
FROM bronze.fmdata_interested_out_players3 AS O
INNER JOIN silver.fmdata_interested_out_players AS N ON O.player_id = N.player_id;

-- Check for Clarity
SELECT 
	O.transfer_value,
	N.transfer_value_m
FROM bronze.fmdata_interested_out_players3 AS O
INNER JOIN silver.fmdata_interested_out_players AS N ON O.player_id = N.player_id;

-- Check for Clarity
SELECT 
	O.wage_per_week,
	N.wage_per_week_k
FROM bronze.fmdata_interested_out_players3 AS O
INNER JOIN silver.fmdata_interested_out_players AS N ON O.player_id = N.player_id;

-- Check for Accuracy
SELECT
(CHARINDEX('p', O.wage_per_week) - 3) AS OLD,
N.wage_per_week_k
FROM bronze.fmdata_interested_out_players3 AS O
INNER JOIN silver.fmdata_interested_out_players AS N ON O.player_id = N.player_id
WHERE (CHARINDEX('p', O.wage_per_week) - 3) < 3;

---TEST FOR FMDATA_POSSESSION_DATA
WITH no_match AS (
    SELECT 
        DISTINCT D.club_name
    FROM 
        silver.fmdata_possession_data AS D
    LEFT JOIN 
        silver.fmdata_interested_out_players AS C ON D.club_name = C.club_name
    WHERE 
        C.club_name IS NULL
)

SELECT 
    DISTINCT(D.club_name) AS no_matchC,
    C.club_name AS possible_match
FROM 
    no_match AS D
LEFT JOIN 
    silver.fmdata_interested_out_players AS C 
    ON LEN(REPLACE(REPLACE(D.club_name, '.', ''), '-', '')) = LEN(REPLACE(REPLACE(C.club_name, '.', ''), '-', ''))
    AND LEFT(D.club_name, 1) = LEFT(C.club_name, 1)
    AND RIGHT(D.club_name, 1) = RIGHT(C.club_name, 1)
WHERE C.club_name IS NOT NULL;
