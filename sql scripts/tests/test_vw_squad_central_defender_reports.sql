/*
======================================================================
Unit Test Suite: gold.vw_squad_central_defender_detailed_reports
======================================================================
Purpose:
    Test-Driven Development (TDD) automated unit tests verifying the
    calculations, boundary conditions, and filtering logic of
    gold.vw_squad_central_defender_detailed_reports.

Execution:
    Execute in SQL Server Management Studio (SSMS), Azure Data Studio,
    or via sqlcmd:
    sqlcmd -S <server> -d DW -i test_vw_squad_central_defender_reports.sql

Safety:
    All tests execute within transactions that ROLLBACK upon completion,
    guaranteeing no test data persists in tables.
======================================================================
*/

USE DW;
GO

SET NOCOUNT ON;

PRINT '==========================================================';
PRINT 'Starting Unit Tests: gold.vw_squad_central_defender_detailed_reports';
PRINT '==========================================================';

DECLARE @TotalTests INT = 0;
DECLARE @PassedTests INT = 0;
DECLARE @FailedTests INT = 0;

-- -------------------------------------------------------------------
-- Test 1: Happy Path - Correct Calculation of Business Metrics
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Insert valid central defender
    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, contracted, age, position, dwh_current_validity
    ) VALUES (
        -101, 999101, 'Test Defender Alpha', 'OWN', 1, 26, 'Central Defender', 1
    );
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played,
        team_goals_scored_per90, team_goals_conceded_per90,
        open_play_key_passes_per90, progressive_passes_per90, passes_attempted_per90,
        tackles_attempted_per90, tackle_accuracy, key_tackles_per90,
        pressures_attempted_per90, pressures_completed_per90,
        possession_won_per90, possession_lost_per90,
        interceptions_per90, clearances_per90, blocks_per90, shots_blocked_per90,
        headers_attempted_per90, heading_accuracy,
        dwh_current_validity
    ) VALUES (
        -101, 'OWN', 1200,
        2.50, 1.00,       -- tG_diff = 1.50
        0.50, 2.00, 50.00,-- kP_per_ProgP = 0.25; ProgRate_per_Pass = 4.00%
        3.00, 0.75, 1.20,
        10.00, 8.00,      -- pressure_accuracy = 80.00%
        6.00, 2.00,       -- possession_differential = 4.00
        2.00, 4.00, 1.00, 1.00, -- Wdefensive = (2*0.5)+(4*0.25)+((1+1)*0.25) = 1.0 + 1.0 + 0.5 = 2.50
        4.00, 0.80,
        1
    );

    -- Act
    DECLARE @tG_diff DECIMAL(5,2), @kP_prog DECIMAL(5,2), @prog_rate DECIMAL(5,2);
    DECLARE @press_acc DECIMAL(5,2), @poss_diff DECIMAL(5,2), @w_def DECIMAL(5,2);

    SELECT 
        @tG_diff = tG_differential_per90,
        @kP_prog = kP_per_ProgP,
        @prog_rate = ProgRate_per_Pass,
        @press_acc = pressure_accuracy,
        @poss_diff = possession_differential,
        @w_def = Wdefensive_actions
    FROM gold.vw_squad_central_defender_detailed_reports
    WHERE player_key = -101;

    -- Assert
    IF (@tG_diff = 1.50 AND @kP_prog = 0.25 AND @prog_rate = 4.00 
        AND @press_acc = 80.00 AND @poss_diff = 4.00 AND @w_def = 2.50)
    BEGIN
        PRINT '[PASS] Test 1: Happy Path - All calculated metrics are accurate.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 1: Happy Path metric mismatch.';
        PRINT '       Expected: tG_diff=1.50, kP_prog=0.25, prog_rate=4.00, press_acc=80.00, poss_diff=4.00, w_def=2.50';
        PRINT CONCAT('       Actual:   tG_diff=', @tG_diff, ', kP_prog=', @kP_prog, ', prog_rate=', @prog_rate, 
                     ', press_acc=', @press_acc, ', poss_diff=', @poss_diff, ', w_def=', @w_def);
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 1 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;


-- -------------------------------------------------------------------
-- Test 2: Division-by-Zero Resilience (Edge Cases with 0 values)
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Player with zero passes and zero pressures attempted
    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, contracted, age, position, dwh_current_validity
    ) VALUES (
        -102, 999102, 'Test Defender Zeroes', 'OWN', 1, 24, 'Central Defender', 1
    );
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played,
        open_play_key_passes_per90, progressive_passes_per90, passes_attempted_per90,
        pressures_attempted_per90, pressures_completed_per90,
        dwh_current_validity
    ) VALUES (
        -102, 'OWN', 1050,
        0.00, 0.00, 0.00, -- Zero progressive passes and zero passes attempted
        0.00, 0.00,       -- Zero pressures attempted
        1
    );

    -- Act
    DECLARE @zero_kP_prog DECIMAL(5,2), @zero_prog_rate DECIMAL(5,2), @zero_press_acc DECIMAL(5,2);

    SELECT 
        @zero_kP_prog = kP_per_ProgP,
        @zero_prog_rate = ProgRate_per_Pass,
        @zero_press_acc = pressure_accuracy
    FROM gold.vw_squad_central_defender_detailed_reports
    WHERE player_key = -102;

    -- Assert: Should return NULL without crashing
    IF (@zero_kP_prog IS NULL AND @zero_prog_rate IS NULL AND @zero_press_acc IS NULL)
    BEGIN
        PRINT '[PASS] Test 2: Division-by-Zero resilience - Handled safely without runtime error.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 2: Expected NULLs for divide-by-zero cases.';
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 2 threw exception (division by zero bug): ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;


-- -------------------------------------------------------------------
-- Test 3: Boundary Filter - Minutes Played Threshold (>= 1000)
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Player with 999 minutes vs Player with 1000 minutes
    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, contracted, age, position, dwh_current_validity
    ) VALUES 
        (-103, 999103, 'Under Threshold', 'OWN', 1, 22, 'Central Defender', 1),
        (-104, 999104, 'At Threshold',    'OWN', 1, 23, 'Central Defender', 1);
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played, dwh_current_validity
    ) VALUES 
        (-103, 'OWN', 999,  1),
        (-104, 'OWN', 1000, 1);

    -- Act
    DECLARE @found_under BIT = 0, @found_at BIT = 0;
    IF EXISTS (SELECT 1 FROM gold.vw_squad_central_defender_detailed_reports WHERE player_key = -103)
        SET @found_under = 1;
    IF EXISTS (SELECT 1 FROM gold.vw_squad_central_defender_detailed_reports WHERE player_key = -104)
        SET @found_at = 1;

    -- Assert
    IF (@found_under = 0 AND @found_at = 1)
    BEGIN
        PRINT '[PASS] Test 3: Boundary Filter - Correctly filters < 1000 minutes and includes >= 1000.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT CONCAT('[FAIL] Test 3: Filter boundary failed. Under(999)=', @found_under, ', At(1000)=', @found_at);
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 3 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;


-- -------------------------------------------------------------------
-- Test 4: Position Filter - Only 'Central Defender'
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Non-central defender
    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, contracted, age, position, dwh_current_validity
    ) VALUES (
        -105, 999105, 'Test Striker', 'OWN', 1, 25, 'Striker', 1
    );
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played, dwh_current_validity
    ) VALUES (
        -105, 'OWN', 1500, 1
    );

    -- Act & Assert
    IF NOT EXISTS (SELECT 1 FROM gold.vw_squad_central_defender_detailed_reports WHERE player_key = -105)
    BEGIN
        PRINT '[PASS] Test 4: Position Filter - Non-central defenders are excluded.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 4: Position Filter - Non-central defender was unexpectedly included.';
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 4 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;


-- -------------------------------------------------------------------
-- Test 5: Contract Filter - Exclude uncontracted players
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Uncontracted player
    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, contracted, age, position, dwh_current_validity
    ) VALUES (
        -106, 999106, 'Test Free Agent', 'OWN', 0, 29, 'Central Defender', 1
    );
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played, dwh_current_validity
    ) VALUES (
        -106, 'OWN', 1500, 1
    );

    -- Act & Assert
    IF NOT EXISTS (SELECT 1 FROM gold.vw_squad_central_defender_detailed_reports WHERE player_key = -106)
    BEGIN
        PRINT '[PASS] Test 5: Contract Filter - Uncontracted players are excluded.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 5: Contract Filter - Uncontracted player was included.';
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 5 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;


-- -------------------------------------------------------------------
-- Test 6: SCD Type 2 Inactive Records - Exclude dwh_current_validity = 0
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Historic SCD record
    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, contracted, age, position, dwh_current_validity
    ) VALUES (
        -107, 999107, 'Test Historical Defender', 'OWN', 1, 28, 'Central Defender', 0
    );
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played, dwh_current_validity
    ) VALUES (
        -107, 'OWN', 1500, 0
    );

    -- Act & Assert
    IF NOT EXISTS (SELECT 1 FROM gold.vw_squad_central_defender_detailed_reports WHERE player_key = -107)
    BEGIN
        PRINT '[PASS] Test 6: SCD Type 2 Filter - Inactive historical records are excluded.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 6: SCD Type 2 Filter - Inactive historical record was included.';
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 6 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;

-- -------------------------------------------------------------------
-- Test Summary Report
-- -------------------------------------------------------------------
PRINT '==========================================================';
PRINT CONCAT('Test Summary: Total: ', @TotalTests, ' | Passed: ', @PassedTests, ' | Failed: ', @FailedTests);
PRINT '==========================================================';

IF @FailedTests > 0
    THROW 51000, 'One or more unit tests failed.', 1;
GO
