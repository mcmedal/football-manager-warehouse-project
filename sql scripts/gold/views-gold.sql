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


-- View: gold.dim_team_info
CREATE VIEW gold.dim_team_info AS
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
        CONCAT(A.league_id, '.', C.club_id) AS team_key,
        A.league,
        B.club_name
    FROM silver.fmdata_possession_data AS B
    LEFT JOIN league_ids AS A
        ON B.club_name = A.club_name
    INNER JOIN club_ids AS C
        ON B.club_name = C.club_name;
GO

-- View: gold.fact_team_possession
CREATE VIEW gold.fact_teams_possession AS
    SELECT
        B.team_key,
        A.average_possession
    FROM silver.fmdata_possession_data AS A
    INNER JOIN gold.dim_team_info AS B
        ON A.club_name = B.club_name;
GO

-- View: gold.fmdata_manager_playstyle
CREATE VIEW gold.dim_playstyle AS
    WITH playstyle AS (
        SELECT
            *,
            ROW_NUMBER() OVER(ORDER BY tactical_style, playing_mentality, preferred_formation, pressing_style, marking_style) AS playstyle_key
        FROM(
            SELECT
                DISTINCT tactical_style, playing_mentality, preferred_formation, pressing_style, marking_style
            FROM silver.fmdata_manager_data) AS styles_of_play
        )
    SELECT
        playstyle_key,
        tactical_style,
        playing_mentality,
        preferred_formation,
        pressing_style,
        marking_style
    FROM playstyle;
GO

-- View: gold.dim_manager_info
CREATE VIEW gold.dim_manager_info AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY A.staff_name) AS manager_key,
        A.staff_id,
        A.staff_name,
        C.team_key AS current_club_key,
        D.team_key AS previous_club_key,
        B.playstyle_key,
        A.contract_begins,
        A.contract_expires
    FROM silver.fmdata_manager_data AS A
    INNER JOIN gold.dim_playstyle AS B
        ON A.tactical_style = B.tactical_style
    INNER JOIN gold.dim_team_info AS C
        ON A.club_name = C.club_name
    LEFT JOIN gold.dim_team_info AS D
        ON A.previous_club_name = D.club_name
    WHERE A.tactical_style = B.tactical_style
        AND A.playing_mentality = B.playing_mentality
        AND A.preferred_formation = B.preferred_formation
        AND A.pressing_style = B.pressing_style
        AND A.marking_style = B.marking_style;
GO

-- View: gold.dim_player_info
CREATE VIEW gold.dim_player_info AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY player_name) AS player_key,
        *
    FROM (
        SELECT
            0 AS contracted,
            A.player_id,
            A.player_name,
            A.age,
            A.position,
            B.team_key AS team_key
        FROM silver.fmdata_interested_out_players AS A
        INNER JOIN gold.dim_team_info AS B
            ON A.club_name = B.club_name
        WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_players) -- If present, ignore our players
    UNION
        SELECT
            1 AS contracted,
            A.player_id,
            A.player_name,
            A.age,
            A.position,
            B.team_key AS team_key
        FROM silver.fmdata_team_players AS A
        INNER JOIN gold.dim_team_info AS B
            ON A.club_name = B.club_name
    UNION
        SELECT
            0 AS contracted,
            A.player_id,
            A.player_name,
            A.age,
            A.position,
            B.team_key AS team_key
        FROM silver.fmdata_interested_gks AS A
        INNER JOIN gold.dim_team_info AS B
            ON A.club_name = B.club_name
        WHERE A.player_id NOT IN (SELECT player_id FROM silver.fmdata_team_gks) -- If present, ignore our players
    UNION
        SELECT
            1 AS contracted,
            A.player_id,
            A.player_name,
            A.age,
            A.position,
            B.team_key AS team_key
        FROM silver.fmdata_team_gks AS A
        INNER JOIN gold.dim_team_info AS B
            ON A.club_name = B.club_name
            ) AS player_info;
GO

-- View: gold.fact_outfield_player_stats
CREATE VIEW gold.fact_outfield_player_stats AS
        SELECT
            B.player_key,
            C.team_key,
            B.contracted,
            A.position,
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
            A.red_cards_per90
        FROM silver.fmdata_interested_out_players AS A
        INNER JOIN gold.dim_player_info AS B
            ON A.player_id = B.player_id
        INNER JOIN gold.dim_team_info AS C
            ON A.club_name = C.club_name
    UNION
        SELECT
            B.player_key,
            C.team_key,
            B.contracted,
            A.position,
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
            A.red_cards_per90
        FROM silver.fmdata_team_players AS A
        INNER JOIN gold.dim_player_info AS B
            ON A.player_id = B.player_id
        INNER JOIN gold.dim_team_info AS C
            ON A.club_name = C.club_name;
GO

-- View: gold.fact_gk_stats
CREATE VIEW gold.fact_gk_stats AS
        SELECT
            B.player_key,
            C.team_key,
            B.contracted,
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
			A.fouls_against_per90
        FROM silver.fmdata_interested_gks AS A
        INNER JOIN gold.dim_player_info AS B
            ON A.player_id = B.player_id
        INNER JOIN gold.dim_team_info AS C
            ON A.club_name = C.club_name
    UNION
        SELECT
            B.player_key,
            C.team_key,
            B.contracted,
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
			A.fouls_against_per90
        FROM silver.fmdata_team_gks AS A
        INNER JOIN gold.dim_player_info AS B
            ON A.player_id = B.player_id
        INNER JOIN gold.dim_team_info AS C
            ON A.club_name = C.club_name;
GO

-- View: gold.fact_players_value
CREATE VIEW gold.fact_players_value AS
        SELECT
            A.player_key,
            A.team_key,
            B.wage_per_week_k,
            B.transfer_value_m
        FROM gold.dim_player_info AS A
        INNER JOIN silver.fmdata_interested_out_players AS B
            ON A.player_id = B.player_id
    UNION
        SELECT
            A.player_key,
            A.team_key,
            B.wage_per_week_k,
            B.transfer_value_m
        FROM gold.dim_player_info AS A
        INNER JOIN silver.fmdata_interested_gks AS B
            ON A.player_id = B.player_id
    UNION
        SELECT
            A.player_key,
            A.team_key,
            B.wage_per_week_k,
            B.transfer_value_m
        FROM gold.dim_player_info AS A
        INNER JOIN silver.fmdata_team_players AS B
            ON A.player_id = B.player_id
    UNION
        SELECT
            A.player_key,
            A.team_key,
            B.wage_per_week_k,
            B.transfer_value_m
        FROM gold.dim_player_info AS A
        INNER JOIN silver.fmdata_team_gks AS B
            ON A.player_id = B.player_id;
GO