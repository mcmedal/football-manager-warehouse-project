/*
======================================================================
Automated Unit Test Suite: Midseason & Multi-Snapshot Warehouse Ingestion
======================================================================
Purpose:
    Validates Option B native support for midseason data ingestion:
    1. Initial midseason snapshot loads cleanly into Gold.
    2. End-of-season snapshot retires midseason records without creating
       duplicate active records in dim_player_info, fact_outfield_player_stats,
       fact_gk_stats, or fact_players_value.
    3. January transfer club resolution picks the active end-of-season club.
    4. Season-over-season growth tracking (vw_player_growth_tracking)
       properly compares definitive season snapshots without treating
       midseason interim data as a whole standalone season.

Execution:
    docker exec sql_server /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'McMedal@14' -C -d DW -i "/scripts/tests/test_midseason_ingestion.sql"
======================================================================
*/

USE DW;
GO

SET NOCOUNT ON;

PRINT '==========================================================';
PRINT 'Starting Test Suite: Midseason Ingestion & SCD2 Isolation';
PRINT '==========================================================';

DECLARE @TotalTests INT = 0;
DECLARE @PassedTests INT = 0;
DECLARE @FailedTests INT = 0;

BEGIN TRANSACTION;
BEGIN TRY
    -- ---------------------------------------------------------------
    -- SETUP: Insert mock league and clubs for testing
    -- ---------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM gold.dim_league WHERE league = 'Test League Alpha')
        INSERT INTO gold.dim_league (league, league_difficulty_coef) VALUES ('Test League Alpha', 0.90);

    IF NOT EXISTS (SELECT 1 FROM gold.dim_club WHERE club = 'Midseason FC')
        INSERT INTO gold.dim_club (club) VALUES ('Midseason FC');

    IF NOT EXISTS (SELECT 1 FROM gold.dim_club WHERE club = 'Winter Transfer United')
        INSERT INTO gold.dim_club (club) VALUES ('Winter Transfer United');

    EXEC gold.load_gold @Season = '2023/24';

    DECLARE @TestPlayerId INT = 899901;

    -- Clean up any residual test records from silver if present
    DELETE FROM silver.fmdata_team_players WHERE player_id = @TestPlayerId;

    -- ===============================================================
    -- STAGE 1: Midseason Ingestion (January - 1,200 mins, 8 goals)
    -- ===============================================================
    PRINT '>>> Stage 1: Simulating Midseason Ingestion...';

    INSERT INTO silver.fmdata_team_players (
        player_id, player_name, club_name, league, position, age, minutes_played,
        team_goals_scored_per90, team_goals_conceded_per90, goals, goals_outside_the_box,
        shots_per90, xGoals_per_shot, shot_accuracy, shots_on_target_per90, shots_outside_the_box_per90,
        goals_per90, xGoals_per90, non_penalty_xGoals_per90, xGoals_overperformance, conversion_rate,
        assists, assists_per90, passes_attempted_per90, pass_accuracy, xAssits_per90, open_play_key_passes_per90,
        chances_created_per90, dribbles_made_per90, progressive_passes_per90, open_play_crosses_attempted_per90,
        open_play_cross_accuracy, crosses_attempted_per90, cross_accuracy, tackles_attempted_per90, tackle_accuracy,
        pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,
        key_tackles_per90, interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90,
        headers_attempted_per90, heading_accuracy, key_headers_per90, sprints_per90, distance_covered_km_per90,
        mistakes_leading_to_goals_per90, fouls_made_per90, fouls_against_per90, yellow_cards_per90, red_cards_per90,
        wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
    ) VALUES (
        @TestPlayerId, 'Test Target Alpha', 'Midseason FC', 'Test League Alpha', 'Striker', 24, 1200,
        1.80, 0.90, 8, 2, 3.20, 0.16, 0.55, 1.76, 1.00,
        0.60, 0.52, 0.48, 0.12, 0.18,
        3, 0.22, 28.0, 0.82, 0.20, 1.40,
        1.80, 2.10, 3.50, 0.80, 0.25,
        1.10, 0.24, 1.20, 0.70, 15.0, 8.5,
        4.20, 8.10, 0.40, 0.60, 0.50, 0.30, 0.10,
        2.50, 0.60, 0.80, 14.0, 10.5,
        0.05, 1.10, 1.40, 2, 0,
        45.0, 25.0, '2024-01-15 10:00:00', '9999-12-31 23:59:59.9999999', 1
    );

    EXEC gold.load_gold @Season = '2023/24';

    -- Test 1: Active Midseason Record Created
    SET @TotalTests = @TotalTests + 1;
    DECLARE @MidActiveCount INT = 0;
    SELECT @MidActiveCount = COUNT(*)
    FROM gold.dim_player_info
    WHERE player_id = @TestPlayerId AND dwh_current_validity = 1;

    IF @MidActiveCount = 1
    BEGIN
        PRINT '[PASS] Test 1: Midseason Load - Active player dimension row created.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 1: Midseason Load - Expected 1 active player record, found ', @MidActiveCount);
        SET @FailedTests = @FailedTests + 1;
    END;

    -- ===============================================================
    -- STAGE 2: End-of-Season Ingestion (May - Transferred, 2,800 mins, 20 goals)
    -- ===============================================================
    PRINT '>>> Stage 2: Simulating End-of-Season Ingestion with Winter Transfer...';

    -- In silver, the midseason record is retired and the end-of-season record is inserted
    UPDATE silver.fmdata_team_players
    SET dwh_current_validity = 0,
        dwh_cd_valid_till = '2024-05-30 18:00:00'
    WHERE player_id = @TestPlayerId AND dwh_current_validity = 1;

    INSERT INTO silver.fmdata_team_players (
        player_id, player_name, club_name, league, position, age, minutes_played,
        team_goals_scored_per90, team_goals_conceded_per90, goals, goals_outside_the_box,
        shots_per90, xGoals_per_shot, shot_accuracy, shots_on_target_per90, shots_outside_the_box_per90,
        goals_per90, xGoals_per90, non_penalty_xGoals_per90, xGoals_overperformance, conversion_rate,
        assists, assists_per90, passes_attempted_per90, pass_accuracy, xAssits_per90, open_play_key_passes_per90,
        chances_created_per90, dribbles_made_per90, progressive_passes_per90, open_play_crosses_attempted_per90,
        open_play_cross_accuracy, crosses_attempted_per90, cross_accuracy, tackles_attempted_per90, tackle_accuracy,
        pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,
        key_tackles_per90, interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90,
        headers_attempted_per90, heading_accuracy, key_headers_per90, sprints_per90, distance_covered_km_per90,
        mistakes_leading_to_goals_per90, fouls_made_per90, fouls_against_per90, yellow_cards_per90, red_cards_per90,
        wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
    ) VALUES (
        @TestPlayerId, 'Test Target Alpha', 'Winter Transfer United', 'Test League Alpha', 'Striker', 24, 2800,
        2.10, 0.85, 20, 4, 3.60, 0.17, 0.58, 2.08, 1.10,
        0.64, 0.55, 0.50, 0.14, 0.20,
        8, 0.26, 30.0, 0.84, 0.24, 1.60,
        2.10, 2.40, 3.80, 0.90, 0.28,
        1.20, 0.26, 1.30, 0.72, 16.0, 9.2,
        4.50, 7.80, 0.45, 0.65, 0.55, 0.35, 0.12,
        2.70, 0.62, 0.85, 15.0, 10.8,
        0.04, 1.05, 1.50, 3, 0,
        65.0, 45.0, '2024-05-30 18:00:01', '9999-12-31 23:59:59.9999999', 1
    );

    EXEC gold.load_gold @Season = '2023/24';

    -- Test 2: Exactly 1 Active Record in dim_player_info
    SET @TotalTests = @TotalTests + 1;
    DECLARE @EosDimActiveCount INT = 0;
    SELECT @EosDimActiveCount = COUNT(*)
    FROM gold.dim_player_info
    WHERE player_id = @TestPlayerId AND dwh_current_validity = 1;

    IF @EosDimActiveCount = 1
    BEGIN
        PRINT '[PASS] Test 2: End-of-Season Load - Exactly 1 active player dimension record.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 2: End-of-Season Load - Expected 1 active dim record, found ', @EosDimActiveCount);
        SET @FailedTests = @FailedTests + 1;
    END;

    -- Test 3: Active dim_player_info resolved winter transfer club
    SET @TotalTests = @TotalTests + 1;
    DECLARE @ActiveClub NVARCHAR(50) = '';
    SELECT @ActiveClub = t.club_name
    FROM gold.dim_player_info p
    INNER JOIN gold.dim_team_info t ON p.team_key = t.team_key
    WHERE p.player_id = @TestPlayerId AND p.dwh_current_validity = 1;

    IF @ActiveClub = 'Winter Transfer United'
    BEGIN
        PRINT '[PASS] Test 3: Winter Transfer Resolution - Correctly resolved latest club (Winter Transfer United).';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 3: Winter Transfer Resolution - Expected Winter Transfer United, found: ', @ActiveClub);
        SET @FailedTests = @FailedTests + 1;
    END;

    -- Test 4: Exactly 1 Active Fact Record with Updated End-of-Season Stats
    SET @TotalTests = @TotalTests + 1;
    DECLARE @ActiveFactCount INT = 0;
    DECLARE @ActiveGoals INT = 0;
    DECLARE @ActiveMinutes INT = 0;

    SELECT 
        @ActiveFactCount = COUNT(*),
        @ActiveGoals = MAX(s.goals),
        @ActiveMinutes = MAX(s.minutes_played)
    FROM gold.fact_outfield_player_stats s
    INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
    WHERE p.player_id = @TestPlayerId AND s.dwh_current_validity = 1;

    IF (@ActiveFactCount = 1 AND @ActiveGoals = 20 AND @ActiveMinutes = 2800)
    BEGIN
        PRINT '[PASS] Test 4: Active Fact Stats - Exactly 1 active fact record with 20 goals and 2,800 mins.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 4: Active Fact Stats - Count: ', @ActiveFactCount, ', Goals: ', @ActiveGoals, ', Minutes: ', @ActiveMinutes);
        SET @FailedTests = @FailedTests + 1;
    END;

    -- Test 5: Historical Midseason Fact Record Preserved
    SET @TotalTests = @TotalTests + 1;
    DECLARE @HistoricalFactCount INT = 0;
    DECLARE @HistoricalGoals INT = 0;

    SELECT 
        @HistoricalFactCount = COUNT(*),
        @HistoricalGoals = MAX(s.goals)
    FROM gold.fact_outfield_player_stats s
    INNER JOIN gold.dim_player_info p ON s.player_key = p.player_key
    WHERE p.player_id = @TestPlayerId AND s.dwh_current_validity = 0;

    IF (@HistoricalFactCount >= 1 AND @HistoricalGoals = 8)
    BEGIN
        PRINT '[PASS] Test 5: Historical Fact Preservation - Midseason record preserved with validity=0 and 8 goals.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 5: Historical Fact Preservation - Historical count: ', @HistoricalFactCount, ', Goals: ', @HistoricalGoals);
        SET @FailedTests = @FailedTests + 1;
    END;

    -- Test 6: Zero Duplicate Active Value Records
    SET @TotalTests = @TotalTests + 1;
    DECLARE @ActiveValueCount INT = 0;

    SELECT @ActiveValueCount = COUNT(*)
    FROM gold.fact_players_value v
    INNER JOIN gold.dim_player_info p ON v.player_key = p.player_key
    WHERE p.player_id = @TestPlayerId AND v.dwh_current_validity = 1;

    IF @ActiveValueCount = 1
    BEGIN
        PRINT '[PASS] Test 6: Fact Players Value - Exactly 1 active valuation record (no duplicate active values).';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 6: Fact Players Value - Expected 1 active valuation row, found: ', @ActiveValueCount);
        SET @FailedTests = @FailedTests + 1;
    END;

    -- Test 7: Growth Tracking Does Not Treat Intra-Season Midseason As Whole Season
    SET @TotalTests = @TotalTests + 1;
    DECLARE @GrowthRowsSeason1 INT = 0;

    -- Within a single season (2023/24), there should be 0 season transitions in vw_player_growth_tracking
    SELECT @GrowthRowsSeason1 = COUNT(*)
    FROM gold.vw_player_growth_tracking
    WHERE player_id = @TestPlayerId;

    IF @GrowthRowsSeason1 = 0
    BEGIN
        PRINT '[PASS] Test 7: Growth Tracking Isolation - Intra-season midseason snapshot not confused for a new season.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 7: Growth Tracking Isolation - Expected 0 season transitions within 2023/24, found: ', @GrowthRowsSeason1);
        SET @FailedTests = @FailedTests + 1;
    END;

    -- ===============================================================
    -- STAGE 3: Subsequent Season Load (2024/25 - 3,100 mins, 25 goals)
    -- ===============================================================
    PRINT '>>> Stage 3: Simulating Subsequent Season 2024/25 Ingestion...';

    UPDATE silver.fmdata_team_players
    SET dwh_current_validity = 0,
        dwh_cd_valid_till = '2025-05-30 18:00:00'
    WHERE player_id = @TestPlayerId AND dwh_current_validity = 1;

    INSERT INTO silver.fmdata_team_players (
        player_id, player_name, club_name, league, position, age, minutes_played,
        team_goals_scored_per90, team_goals_conceded_per90, goals, goals_outside_the_box,
        shots_per90, xGoals_per_shot, shot_accuracy, shots_on_target_per90, shots_outside_the_box_per90,
        goals_per90, xGoals_per90, non_penalty_xGoals_per90, xGoals_overperformance, conversion_rate,
        assists, assists_per90, passes_attempted_per90, pass_accuracy, xAssits_per90, open_play_key_passes_per90,
        chances_created_per90, dribbles_made_per90, progressive_passes_per90, open_play_crosses_attempted_per90,
        open_play_cross_accuracy, crosses_attempted_per90, cross_accuracy, tackles_attempted_per90, tackle_accuracy,
        pressures_attempted_per90, pressures_completed_per90, possession_won_per90, possession_lost_per90,
        key_tackles_per90, interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90,
        headers_attempted_per90, heading_accuracy, key_headers_per90, sprints_per90, distance_covered_km_per90,
        mistakes_leading_to_goals_per90, fouls_made_per90, fouls_against_per90, yellow_cards_per90, red_cards_per90,
        wage_per_week_k, transfer_value_m, dwh_create_date, dwh_cd_valid_till, dwh_current_validity
    ) VALUES (
        @TestPlayerId, 'Test Target Alpha', 'Winter Transfer United', 'Test League Alpha', 'Striker', 25, 3100,
        2.20, 0.80, 25, 5, 3.80, 0.18, 0.60, 2.28, 1.20,
        0.72, 0.62, 0.58, 0.14, 0.22,
        10, 0.29, 32.0, 0.85, 0.28, 1.80,
        2.30, 2.60, 4.10, 1.00, 0.30,
        1.30, 0.28, 1.40, 0.74, 16.5, 9.8,
        4.80, 7.50, 0.50, 0.70, 0.60, 0.40, 0.15,
        2.80, 0.64, 0.90, 15.5, 11.0,
        0.03, 1.00, 1.60, 2, 0,
        85.0, 65.0, '2025-05-30 18:00:01', '9999-12-31 23:59:59.9999999', 1
    );

    EXEC gold.load_gold @Season = '2024/25';

    -- Test 8: Multi-Season Growth Comparison Uses Definitive End-of-Season Snapshot
    SET @TotalTests = @TotalTests + 1;
    DECLARE @DeltaValue DECIMAL(6,2) = 0.0;
    DECLARE @CurSeason VARCHAR(10) = '';
    DECLARE @PrevSeason VARCHAR(10) = '';

    SELECT 
        @DeltaValue = delta_transfer_value_m,
        @CurSeason = current_season,
        @PrevSeason = previous_season
    FROM gold.vw_player_growth_tracking
    WHERE player_id = @TestPlayerId;

    -- In 2024/25 value = 65.0M. In 2023/24 end-of-season value = 45.0M (delta = +20.0M).
    -- If midseason was used (25.0M), delta would incorrectly be +40.0M.
    IF (@CurSeason = '2024/25' AND @PrevSeason = '2023/24' AND @DeltaValue = 20.00)
    BEGIN
        PRINT CONCAT('[PASS] Test 8: True Season-over-Season Growth - Correct transition ', @PrevSeason, ' -> ', @CurSeason, ' with delta_transfer_value_m = +£', @DeltaValue, 'M.');
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 8: True Season-over-Season Growth - Expected +20.00M from 2023/24 to 2024/25, found: delta = ', @DeltaValue, ', cur = ', @CurSeason, ', prev = ', @PrevSeason);
        SET @FailedTests = @FailedTests + 1;
    END;

END TRY
BEGIN CATCH
    PRINT CONCAT('[ERROR] Test Execution Failed: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;

-- Always rollback test transactions to keep DW clean
ROLLBACK TRANSACTION;
PRINT '>>> Transaction rolled back: Zero persistent test data remains in DW.';

PRINT '==========================================================';
PRINT CONCAT('Midseason Ingestion Test Summary: Total: ', @TotalTests, ' | Passed: ', @PassedTests, ' | Failed: ', @FailedTests);
PRINT '==========================================================';

IF @FailedTests > 0
    THROW 53000, 'Midseason Ingestion test suite failed.', 1;
GO
