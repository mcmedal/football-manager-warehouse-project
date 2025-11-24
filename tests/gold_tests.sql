/*
======================================================================
This script contains comprehensive tests for data quality validation
of all gold schema objects against their source silver schema tables.
Tests include: Referential Integrity, Completeness, and Accuracy checks.
======================================================================
*/

--- Tests for DIM_TEAM_INFO
-- Check for Referential Integrity
SELECT 'DIM_TEAM_INFO - League RI' AS test_name,
    COUNT(*) AS failed_records
FROM silver.fmdata_interested_out_players
WHERE league NOT IN (SELECT league FROM gold.dim_team_info);

-- Check for Referential Integrity
SELECT 'DIM_TEAM_INFO - Club RI' AS test_name,
    COUNT(*) AS failed_records
FROM silver.fmdata_interested_out_players
WHERE club_name NOT IN (SELECT club_name FROM gold.dim_team_info);

-- Check for Completeness
SELECT 'DIM_TEAM_INFO - Completeness' AS test_name,
    COUNT(DISTINCT A.club_name) - COUNT(DISTINCT B.club_name) AS missing_clubs,
    COUNT(DISTINCT A.league) - COUNT(DISTINCT B.league) AS missing_leagues
FROM gold.dim_team_info AS A
LEFT JOIN silver.fmdata_interested_out_players AS B
    ON A.club_name = B.club_name;

--- Tests for FACT_TEAM_POSSESSION
-- Check for Referential Integrity
SELECT 'FACT_TEAM_POSSESSION - Team Key RI' AS test_name,
    COUNT(*) AS failed_records
FROM gold.fact_teams_possession
WHERE team_key NOT IN (SELECT team_key FROM gold.dim_team_info);

-- Check for Completeness
SELECT 'FACT_TEAM_POSSESSION - Completeness' AS test_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT team_key) AS distinct_teams
FROM gold.fact_teams_possession;

--- Tests for FACT_PLAYER_STATS
-- Check for Referential Integrity
SELECT 'FACT_PLAYER_STATS - Player Key RI' AS test_name,
    COUNT(*) AS failed_records
FROM gold.fact_outfield_player_stats
WHERE player_key NOT IN (SELECT player_key FROM gold.dim_player_info);

-- Check for Referential Integrity
SELECT 'FACT_PLAYER_STATS - Team Key RI' AS test_name,
    COUNT(*) AS failed_records
FROM gold.fact_outfield_player_stats
WHERE team_key NOT IN (SELECT team_key FROM gold.dim_team_info);

-- Check for Completeness
SELECT 'FACT_PLAYER_STATS - Completeness' AS test_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT player_key) AS distinct_players,
    COUNT(DISTINCT team_key) AS distinct_teams
FROM gold.fact_outfield_player_stats;

--- Tests for DIM_PLAYER
-- Check for Null Values
SELECT 'DIM_PLAYER - Null Check' AS test_name,
    COUNT(*) AS null_records
FROM gold.dim_player_info
WHERE player_key IS NULL OR player_name IS NULL;

-- Check for Duplicates
SELECT 'DIM_PLAYER - Duplicates' AS test_name,
    player_key,
    player_name,
    COUNT(*) AS duplicate_count
FROM gold.dim_player_info
GROUP BY player_key, player_name
HAVING COUNT(*) > 1;