/*
======================================================================
Creates a View: Load Gold View (Silver Layer -> Gold Layer)
======================================================================
   This script is designed to create views from the silver schema that 
   loads tables from the silver layer  into the gold schema. These views
   are structured to facilitate easier access and analysis of the data
   for end-users.
 
======================================================================
*/

WITH league_ids AS (
    SELECT
        DISTINCT(league) AS league,
        DENSE_RANK () OVER (ORDER BY league) AS league_id,
        club_name
    FROM silver.fmdata_interested_out_players
    ),
    club_ids AS (
    SELECT
        DISTINCT(club_name) AS club_name,
        DENSE_RANK () OVER (ORDER BY club_name) AS club_id,
        league
    FROM silver.fmdata_interested_out_players
    )
SELECT
    CONCAT(A.league_id, '.', C.club_id) AS team_id,
    A.league,
    B.club_name,
    B.average_possession
FROM silver.fmdata_possession_data AS B
LEFT JOIN league_ids AS A
    ON B.club_name = A.club_name
INNER JOIN club_ids AS C
    ON B.club_name = C.club_name;

