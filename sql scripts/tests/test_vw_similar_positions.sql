/*
======================================================================
Unit Test Suite: Positional Replacement Similarity Engines
======================================================================
Purpose:
    Test-Driven Development (TDD) automated unit tests verifying the
    calculations, boundary conditions, null safety, and ranking logic
    of the positional similarity views:
    - gold.vw_similar_strikers
    - gold.vw_similar_central_defenders
    - gold.vw_similar_flank_defenders
    - gold.vw_similar_defensive_midfielders
    - gold.vw_similar_central_midfielders
    - gold.vw_similar_wingers
    - gold.vw_similar_attacking_midfielders
    - gold.vw_similar_goalkeepers

Execution:
    Execute in SQL Server Management Studio (SSMS), Azure Data Studio,
    or via sqlcmd:
    sqlcmd -S <server> -d DW -i test_vw_similar_positions.sql

Safety:
    All tests execute within transactions that ROLLBACK upon completion,
    guaranteeing zero test data persistence.
======================================================================
*/

USE DW;
GO

SET NOCOUNT ON;

PRINT '==========================================================';
PRINT 'Starting Unit Tests: Positional Similarity Engines';
PRINT '==========================================================';

DECLARE @TotalTests INT = 0;
DECLARE @PassedTests INT = 0;
DECLARE @FailedTests INT = 0;

-- -------------------------------------------------------------------
-- Test 1: Central Defender Similarity Ranking & Distance Calculation
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Insert team keys and starter benchmark + 2 candidate targets
    INSERT INTO gold.dim_team_info (team_key, league, club_name, dwh_current_validity)
    VALUES ('OWN', 'Serie A', 'Own Club', 1),
           ('EXT1', 'LaLiga', 'External Club 1', 1),
           ('EXT2', 'Ligue 1', 'External Club 2', 1);

    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, is_own_squad, contracted, age, position, dwh_current_validity
    ) VALUES 
        (-201, 888201, 'Starter CD Alpha', 'OWN', 1, 1, 27, 'Central Defender', 1),
        (-202, 888202, 'Target CD Close',  'EXT1', 0, 0, 24, 'Central Defender', 1),
        (-203, 888203, 'Target CD Far',    'EXT2', 0, 0, 29, 'Central Defender', 1);
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_players_value (
        player_key, team_key, wage_per_week_k, transfer_value_m, dwh_current_validity
    ) VALUES 
        (-201, 'OWN',  50.0, 20.0, 1),
        (-202, 'EXT1', 25.0, 10.0, 1),
        (-203, 'EXT2', 40.0, 15.0, 1);

    -- Starter stats
    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played,
        progressive_passes_per90, passes_attempted_per90,
        tackles_attempted_per90, tackle_accuracy,
        interceptions_per90, clearances_per90, blocks_per90,
        headers_attempted_per90, heading_accuracy,
        fouls_made_per90, yellow_cards_per90, red_cards_per90,
        dwh_current_validity
    ) VALUES 
        (-201, 'OWN', 2000, 5.0, 60.0, 2.0, 0.80, 2.0, 5.0, 1.0, 6.0, 0.70, 0.5, 0.1, 0.0, 1),
        -- Close match stats
        (-202, 'EXT1', 1800, 5.1, 58.0, 2.1, 0.79, 1.9, 4.9, 1.1, 5.9, 0.71, 0.6, 0.1, 0.0, 1),
        -- Far match stats (low aerial, low passing, high fouls)
        (-203, 'EXT2', 1500, 1.0, 30.0, 4.0, 0.50, 0.5, 2.0, 0.2, 2.0, 0.40, 2.5, 0.5, 0.1, 1);

    -- Act
    DECLARE @RankClose INT, @RankFar INT;
    DECLARE @DistClose DECIMAL(5,2), @DistFar DECIMAL(5,2);

    SELECT @RankClose = similarity_rank, @DistClose = similarity_distance
    FROM gold.vw_similar_central_defenders
    WHERE player_key = -202;

    SELECT @RankFar = similarity_rank, @DistFar = similarity_distance
    FROM gold.vw_similar_central_defenders
    WHERE player_key = -203;

    -- Assert
    IF (@RankClose IS NOT NULL AND @RankFar IS NOT NULL AND @RankClose < @RankFar AND @DistClose < @DistFar)
    BEGIN
        PRINT '[PASS] Test 1: Central Defender Similarity Engine correctly ranks closest target ahead of distant target.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 1: Central Defender Similarity Engine ranking mismatch.';
        PRINT CONCAT('       Actual: RankClose=', @RankClose, ' (Dist=', @DistClose, '), RankFar=', @RankFar, ' (Dist=', @DistFar, ')');
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 1 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;

-- -------------------------------------------------------------------
-- Test 2: Own Squad Exclusion - Starter Never Appears as Target
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Insert starting striker
    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, is_own_squad, contracted, age, position, dwh_current_validity
    ) VALUES 
        (-204, 888204, 'Own Striker Starter', 'OWN', 1, 1, 26, 'Striker', 1);
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_players_value (
        player_key, team_key, wage_per_week_k, transfer_value_m, dwh_current_validity
    ) VALUES 
        (-204, 'OWN', 60.0, 30.0, 1);

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played,
        non_penalty_xGoals_per90, shots_per90, shots_on_target_per90,
        headers_attempted_per90, heading_accuracy,
        pressures_attempted_per90, possession_won_per90,
        dwh_current_validity
    ) VALUES 
        (-204, 'OWN', 2200, 0.45, 2.5, 1.3, 4.0, 0.60, 10.0, 2.5, 1);

    -- Act & Assert
    IF NOT EXISTS (SELECT 1 FROM gold.vw_similar_strikers WHERE player_key = -204)
    BEGIN
        PRINT '[PASS] Test 2: Own squad starter is strictly excluded from target search pool.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 2: Own squad starter appeared in transfer targets view.';
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 2 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;

-- -------------------------------------------------------------------
-- Test 3: Minimum Sample Filter - Players with < 900 Minutes Excluded
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Starter + Target with only 500 minutes
    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, is_own_squad, contracted, age, position, dwh_current_validity
    ) VALUES 
        (-205, 888205, 'Starter Winger', 'OWN', 1, 1, 25, 'Left Attacking Midfielder', 1),
        (-206, 888206, 'Low Min Winger', 'EXT1', 0, 0, 19, 'Left Attacking Midfielder', 1);
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_players_value (
        player_key, team_key, wage_per_week_k, transfer_value_m, dwh_current_validity
    ) VALUES 
        (-205, 'OWN',  30.0, 12.0, 1),
        (-206, 'EXT1',  5.0,  2.0, 1);

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played, dwh_current_validity
    ) VALUES 
        (-205, 'OWN', 1800, 1),
        (-206, 'EXT1', 500, 1); -- Under 900 minutes threshold

    -- Act & Assert
    IF NOT EXISTS (SELECT 1 FROM gold.vw_similar_wingers WHERE player_key = -206)
    BEGIN
        PRINT '[PASS] Test 3: Low-minute sample targets (< 900 mins) are excluded from similarity results.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 3: Target under 900 minutes was erroneously returned.';
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 3 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;

-- -------------------------------------------------------------------
-- Test 4: Null Safety - Null Ratios Do Not Cause NULL Distance
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Starter + Target with zero shots, passes, or tackles (ratios are NULL)
    INSERT INTO gold.dim_team_info (team_key, league, club_name, dwh_current_validity)
    VALUES ('OWN', 'Serie A', 'Own Club', 1),
           ('EXT1', 'LaLiga', 'External Club 1', 1);

    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, is_own_squad, contracted, age, position, dwh_current_validity
    ) VALUES 
        (-207, 888207, 'Starter Striker', 'OWN', 1, 1, 28, 'Striker', 1),
        (-208, 888208, 'Zero Stats Striker', 'EXT1', 0, 0, 22, 'Striker', 1);
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_players_value (
        player_key, team_key, wage_per_week_k, transfer_value_m, dwh_current_validity
    ) VALUES 
        (-207, 'OWN',  40.0, 15.0, 1),
        (-208, 'EXT1', 10.0,  3.0, 1);

    INSERT INTO gold.fact_outfield_player_stats (
        player_key, team_key, minutes_played,
        shots_per90, shots_on_target_per90, non_penalty_xGoals_per90,
        headers_attempted_per90, heading_accuracy,
        pressures_attempted_per90, possession_won_per90,
        dwh_current_validity
    ) VALUES 
        (-207, 'OWN', 1500, 2.0, 1.0, 0.40, 4.0, 0.50, 8.0, 2.0, 1),
        (-208, 'EXT1', 1000, 0.0, 0.0, 0.00, 0.0, 0.00, 0.0, 0.0, 1); -- All ratios will be NULL

    -- Act
    DECLARE @NullSafeDist DECIMAL(5,2);
    SELECT @NullSafeDist = similarity_distance
    FROM gold.vw_similar_strikers
    WHERE player_key = -208;

    -- Assert
    IF (@NullSafeDist IS NOT NULL AND @NullSafeDist > 0)
    BEGIN
        PRINT '[PASS] Test 4: Null Safety - Target with 0-value/NULL ratios computes a valid distance.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 4: Null Safety failed - Distance computed as NULL.';
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 4 encountered error: ', ERROR_MESSAGE());
    SET @FailedTests = @FailedTests + 1;
END CATCH;
ROLLBACK TRANSACTION;

-- -------------------------------------------------------------------
-- Test 5: Goalkeeper Replacement Engine Verification
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
BEGIN TRANSACTION;
BEGIN TRY
    -- Arrange: Starting GK + 2 target GKs
    INSERT INTO gold.dim_team_info (team_key, league, club_name, dwh_current_validity)
    VALUES ('OWN', 'Serie A', 'Own Club', 1),
           ('EXT1', 'LaLiga', 'External Club 1', 1),
           ('EXT2', 'Ligue 1', 'External Club 2', 1);

    SET IDENTITY_INSERT gold.dim_player_info ON;
    INSERT INTO gold.dim_player_info (
        player_key, player_id, player_name, team_key, is_own_squad, contracted, age, position, dwh_current_validity
    ) VALUES 
        (-209, 888209, 'Starter GK Alpha', 'OWN', 1, 1, 30, 'Goalkeeper', 1),
        (-210, 888210, 'Target GK Twin',   'EXT1', 0, 0, 23, 'Goalkeeper', 1),
        (-211, 888211, 'Target GK Opp',    'EXT2', 0, 0, 28, 'Goalkeeper', 1);
    SET IDENTITY_INSERT gold.dim_player_info OFF;

    INSERT INTO gold.fact_players_value (
        player_key, team_key, wage_per_week_k, transfer_value_m, dwh_current_validity
    ) VALUES 
        (-209, 'OWN',  35.0, 15.0, 1),
        (-210, 'EXT1', 15.0,  5.0, 1),
        (-211, 'EXT2', 25.0,  8.0, 1);

    INSERT INTO gold.fact_gk_stats (
        player_key, team_key, minutes_played,
        saves_percentage, xSave_rate, xGoals_prevented_per90,
        saves_made_per90, saves_held_per90,
        clearances_per90, interceptions_per90,
        passes_attempted_per90, pass_accuracy,
        dwh_current_validity
    ) VALUES 
        (-209, 'OWN',  2500, 0.78, 0.72, 0.35, 3.5, 1.8, 0.8, 0.4, 25.0, 0.82, 1),
        -- Twin profile
        (-210, 'EXT1', 2000, 0.77, 0.73, 0.33, 3.4, 1.7, 0.7, 0.5, 24.0, 0.81, 1),
        -- Opposite profile (negative shot stopping, no sweeper actions)
        (-211, 'EXT2', 1800, 0.65, 0.74, -0.40, 2.8, 0.5, 0.1, 0.0, 12.0, 0.55, 1);

    -- Act
    DECLARE @GkRankClose INT, @GkRankFar INT;
    SELECT @GkRankClose = similarity_rank FROM gold.vw_similar_goalkeepers WHERE player_key = -210;
    SELECT @GkRankFar   = similarity_rank FROM gold.vw_similar_goalkeepers WHERE player_key = -211;

    -- Assert
    IF (@GkRankClose IS NOT NULL AND @GkRankFar IS NOT NULL AND @GkRankClose < @GkRankFar)
    BEGIN
        PRINT '[PASS] Test 5: Goalkeeper Replacement Engine correctly ranks profile twin ahead of divergent target.';
        SET @PassedTests = @PassedTests + 1;
    END
    ELSE
    BEGIN
        PRINT '[FAIL] Test 5: Goalkeeper Replacement Engine ranking mismatch.';
        PRINT CONCAT('       Actual: GkRankClose=', @GkRankClose, ', GkRankFar=', @GkRankFar);
        SET @FailedTests = @FailedTests + 1;
    END
END TRY
BEGIN CATCH
    PRINT CONCAT('[FAIL] Test 5 encountered error: ', ERROR_MESSAGE());
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
