/*
======================================================================
Automated Data Quality & Integrity Test Suite
======================================================================
Purpose:
    Enterprise dbt-style data warehouse integrity testing suite
    verifying table constraints, referential integrity, domain boundaries,
    and SCD Type 2 historization rules across FiorentinaDW.

Execution:
    sqlcmd -S <server> -d FiorentinaDW -i test_data_integrity.sql
======================================================================
*/

USE FiorentinaDW;
GO

SET NOCOUNT ON;

PRINT '==========================================================';
PRINT 'Starting Data Warehouse Integrity & Sanity Test Suite';
PRINT '==========================================================';

DECLARE @TotalTests INT = 0;
DECLARE @PassedTests INT = 0;
DECLARE @FailedTests INT = 0;

-- -------------------------------------------------------------------
-- Test 1: Uniqueness of Active Records in dim_player_info
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
DECLARE @DuplicateActivePlayers INT = 0;

SELECT @DuplicateActivePlayers = COUNT(*)
FROM (
    SELECT player_id
    FROM gold.dim_player_info
    WHERE dwh_current_validity = 1
    GROUP BY player_id
    HAVING COUNT(*) > 1
) dup;

IF @DuplicateActivePlayers = 0
BEGIN
    PRINT '[PASS] Test 1: Active Player Uniqueness - No duplicate active player records.';
    SET @PassedTests = @PassedTests + 1;
END
ELSE
BEGIN
    PRINT CONCAT('[FAIL] Test 1: Active Player Uniqueness - Found ', @DuplicateActivePlayers, ' duplicate active players.');
    SET @FailedTests = @FailedTests + 1;
END;

-- -------------------------------------------------------------------
-- Test 2: Referential Integrity (No Orphan Fact Rows)
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
DECLARE @OrphanOutfieldStats INT = 0;
DECLARE @OrphanGkStats INT = 0;

SELECT @OrphanOutfieldStats = COUNT(*)
FROM gold.fact_outfield_player_stats s
LEFT JOIN gold.dim_player_info p ON s.player_key = p.player_key
WHERE p.player_key IS NULL;

SELECT @OrphanGkStats = COUNT(*)
FROM gold.fact_gk_stats s
LEFT JOIN gold.dim_player_info p ON s.player_key = p.player_key
WHERE p.player_key IS NULL;

IF (@OrphanOutfieldStats = 0 AND @OrphanGkStats = 0)
BEGIN
    PRINT '[PASS] Test 2: Referential Integrity - Zero orphan fact records found.';
    SET @PassedTests = @PassedTests + 1;
END
ELSE
BEGIN
    PRINT CONCAT('[FAIL] Test 2: Referential Integrity - Orphan facts found (Outfield: ', @OrphanOutfieldStats, ', GK: ', @OrphanGkStats, ').');
    SET @FailedTests = @FailedTests + 1;
END;

-- -------------------------------------------------------------------
-- Test 3: Domain Sanity (Accuracy Metrics in Range [0, 1])
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
DECLARE @InvalidAccuracies INT = 0;

SELECT @InvalidAccuracies = COUNT(*)
FROM gold.fact_outfield_player_stats
WHERE (pass_accuracy < 0.0 OR pass_accuracy > 1.0)
   OR (shot_accuracy < 0.0 OR shot_accuracy > 1.0)
   OR (tackle_accuracy < 0.0 OR tackle_accuracy > 1.0)
   OR (heading_accuracy < 0.0 OR heading_accuracy > 1.0);

IF @InvalidAccuracies = 0
BEGIN
    PRINT '[PASS] Test 3: Domain Boundaries - All accuracy ratios are within [0.00, 1.00].';
    SET @PassedTests = @PassedTests + 1;
END
ELSE
BEGIN
    PRINT CONCAT('[FAIL] Test 3: Domain Boundaries - Found ', @InvalidAccuracies, ' records with invalid accuracies.');
    SET @FailedTests = @FailedTests + 1;
END;

-- -------------------------------------------------------------------
-- Test 4: SCD Type 2 Temporal Validity
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
DECLARE @InvalidScd2Dates INT = 0;

SELECT @InvalidScd2Dates = COUNT(*)
FROM gold.dim_player_info
WHERE (dwh_current_validity = 1 AND dwh_cd_valid_till < '9999-01-01')
   OR (dwh_current_validity = 0 AND dwh_create_date > dwh_cd_valid_till);

IF @InvalidScd2Dates = 0
BEGIN
    PRINT '[PASS] Test 4: SCD Type 2 Validity - All temporal dates and validity flags are consistent.';
    SET @PassedTests = @PassedTests + 1;
END
ELSE
BEGIN
    PRINT CONCAT('[FAIL] Test 4: SCD Type 2 Validity - Found ', @InvalidScd2Dates, ' records with inconsistent temporal validity.');
    SET @FailedTests = @FailedTests + 1;
END;

-- -------------------------------------------------------------------
-- Test 5: Non-Negative Quantities (Minutes, Valuations)
-- -------------------------------------------------------------------
SET @TotalTests = @TotalTests + 1;
DECLARE @NegativeValues INT = 0;

SELECT @NegativeValues = COUNT(*)
FROM gold.fact_outfield_player_stats s
LEFT JOIN gold.fact_players_value v ON s.player_key = v.player_key
WHERE s.minutes_played < 0
   OR (v.transfer_value_m < 0)
   OR (v.wage_per_week_k < 0);

IF @NegativeValues = 0
BEGIN
    PRINT '[PASS] Test 5: Non-Negative Sanity - Zero negative minutes, wages, or valuations.';
    SET @PassedTests = @PassedTests + 1;
END
ELSE
BEGIN
    PRINT CONCAT('[FAIL] Test 5: Non-Negative Sanity - Found ', @NegativeValues, ' records with negative quantities.');
    SET @FailedTests = @FailedTests + 1;
END;

-- -------------------------------------------------------------------
-- Test Summary Report
-- -------------------------------------------------------------------
PRINT '==========================================================';
PRINT CONCAT('Data Integrity Summary: Total: ', @TotalTests, ' | Passed: ', @PassedTests, ' | Failed: ', @FailedTests);
PRINT '==========================================================';

IF @FailedTests > 0
    THROW 52000, 'Data Integrity verification failed.', 1;
GO
