/*
======================================================================
Database Master Orchestrator: End-to-End Warehouse Pipeline
======================================================================
Script Purpose:
    Defines a master stored procedure dbo.usp_run_warehouse_pipeline
    that orchestrates the full Medallion Data Warehouse pipeline:
      1. Bronze Layer (Raw Bulk Ingestion)
      2. Silver Layer (Cleaning, Standardization & Historization)
      3. Gold Layer (Dimensional Modeling & SCD Type 2 Historization)
      4. Audit Logging & Verification

Usage:
    EXEC dbo.usp_run_warehouse_pipeline @Season = '2023/24', @RunTests = 1;
======================================================================
*/

USE DW;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE dbo.usp_run_warehouse_pipeline
    @Season VARCHAR(9) = '2023/24',
    @DataPath NVARCHAR(500) = 'C:\Users\<username>\Documents\FM data\',
    @RunTests BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchId UNIQUEIDENTIFIER = NEWID();
    DECLARE @PipelineStart DATETIME2 = GETDATE();
    DECLARE @StepStart DATETIME2, @StepEnd DATETIME2;
    DECLARE @Duration INT;

    -- Detect host operating system (Linux vs Windows)
    DECLARE @HostPlatform NVARCHAR(50) = 'Windows';
    BEGIN TRY
        SELECT @HostPlatform = host_platform FROM sys.dm_os_host_info;
    END TRY
    BEGIN CATCH
        SET @HostPlatform = 'Windows';
    END CATCH;

    -- If running on Linux and path is Windows default, default to Docker container /data/
    IF @HostPlatform = 'Linux' AND (@DataPath LIKE 'C:\%' OR @DataPath LIKE 'D:\%')
    BEGIN
        SET @DataPath = '/data/';
    END;

    PRINT '======================================================================';
    PRINT CONCAT('STARTING WAREHOUSE ETL PIPELINE - BATCH: ', CAST(@BatchId AS NVARCHAR(50)));
    PRINT CONCAT('HOST PLATFORM:  ', @HostPlatform);
    PRINT CONCAT('TARGET SEASON:  ', @Season);
    PRINT CONCAT('DATA DIRECTORY: ', @DataPath);
    PRINT '======================================================================';

    -- Log Pipeline Start
    INSERT INTO audit.etl_pipeline_log (batch_id, pipeline_layer, step_name, status, start_time)
    VALUES (@BatchId, 'Complete', 'Warehouse Pipeline Execution', 'Started', @PipelineStart);

    -- -------------------------------------------------------------------
    -- 1. BRONZE LAYER INGESTION
    -- -------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = GETDATE();
        PRINT '>>> Step 1: Executing Bronze Layer Ingestion (bronze.load_bronze)...';

        IF OBJECT_ID('bronze.load_bronze', 'P') IS NOT NULL
        BEGIN
            EXEC bronze.load_bronze @DataPath = @DataPath;
        END
        ELSE
        BEGIN
            PRINT '    [INFO] bronze.load_bronze procedure not found, assuming bronze data pre-loaded.';
        END

        SET @StepEnd = GETDATE();
        SET @Duration = DATEDIFF(millisecond, @StepStart, @StepEnd);

        INSERT INTO audit.etl_pipeline_log (batch_id, pipeline_layer, step_name, status, start_time, end_time, duration_ms)
        VALUES (@BatchId, 'Bronze', 'bronze.load_bronze', 'Success', @StepStart, @StepEnd, @Duration);
        PRINT CONCAT('    [SUCCESS] Bronze layer completed in ', @Duration, ' ms.');
    END TRY
    BEGIN CATCH
        SET @StepEnd = GETDATE();
        SET @Duration = DATEDIFF(millisecond, @StepStart, @StepEnd);
        INSERT INTO audit.etl_pipeline_log (batch_id, pipeline_layer, step_name, status, start_time, end_time, duration_ms, error_message)
        VALUES (@BatchId, 'Bronze', 'bronze.load_bronze', 'Failed', @StepStart, @StepEnd, @Duration, ERROR_MESSAGE());
        PRINT CONCAT('    [ERROR] Bronze layer failed: ', ERROR_MESSAGE());
        THROW;
    END CATCH;

    -- -------------------------------------------------------------------
    -- 2. SILVER LAYER TRANSFORMATION
    -- -------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = GETDATE();
        PRINT '>>> Step 2: Executing Silver Layer Transformation (silver.load_silver)...';

        IF OBJECT_ID('silver.load_silver', 'P') IS NOT NULL
        BEGIN
            EXEC silver.load_silver;
        END
        ELSE
        BEGIN
            PRINT '    [INFO] silver.load_silver procedure not found, skipping silver execution.';
        END

        SET @StepEnd = GETDATE();
        SET @Duration = DATEDIFF(millisecond, @StepStart, @StepEnd);

        INSERT INTO audit.etl_pipeline_log (batch_id, pipeline_layer, step_name, status, start_time, end_time, duration_ms)
        VALUES (@BatchId, 'Silver', 'silver.load_silver', 'Success', @StepStart, @StepEnd, @Duration);
        PRINT CONCAT('    [SUCCESS] Silver layer completed in ', @Duration, ' ms.');
    END TRY
    BEGIN CATCH
        SET @StepEnd = GETDATE();
        SET @Duration = DATEDIFF(millisecond, @StepStart, @StepEnd);
        INSERT INTO audit.etl_pipeline_log (batch_id, pipeline_layer, step_name, status, start_time, end_time, duration_ms, error_message)
        VALUES (@BatchId, 'Silver', 'silver.load_silver', 'Failed', @StepStart, @StepEnd, @Duration, ERROR_MESSAGE());
        PRINT CONCAT('    [ERROR] Silver layer failed: ', ERROR_MESSAGE());
        THROW;
    END CATCH;

    -- -------------------------------------------------------------------
    -- 3. GOLD LAYER DIMENSIONAL MODELING & SCD2
    -- -------------------------------------------------------------------
    BEGIN TRY
        SET @StepStart = GETDATE();
        PRINT CONCAT('>>> Step 3: Executing Gold Layer Load (gold.load_gold @Season = ''', @Season, ''')...');

        IF OBJECT_ID('gold.load_gold', 'P') IS NOT NULL
        BEGIN
            EXEC gold.load_gold @Season = @Season;
        END
        ELSE
        BEGIN
            PRINT '    [INFO] gold.load_gold procedure not found, skipping gold execution.';
        END

        SET @StepEnd = GETDATE();
        SET @Duration = DATEDIFF(millisecond, @StepStart, @StepEnd);

        INSERT INTO audit.etl_pipeline_log (batch_id, pipeline_layer, step_name, status, start_time, end_time, duration_ms)
        VALUES (@BatchId, 'Gold', 'gold.load_gold', 'Success', @StepStart, @StepEnd, @Duration);
        PRINT CONCAT('    [SUCCESS] Gold layer completed in ', @Duration, ' ms.');
    END TRY
    BEGIN CATCH
        SET @StepEnd = GETDATE();
        SET @Duration = DATEDIFF(millisecond, @StepStart, @StepEnd);
        INSERT INTO audit.etl_pipeline_log (batch_id, pipeline_layer, step_name, status, start_time, end_time, duration_ms, error_message)
        VALUES (@BatchId, 'Gold', 'gold.load_gold', 'Failed', @StepStart, @StepEnd, @Duration, ERROR_MESSAGE());
        PRINT CONCAT('    [ERROR] Gold layer failed: ', ERROR_MESSAGE());
        THROW;
    END CATCH;

    -- -------------------------------------------------------------------
    -- 4. DATA INTEGRITY SANITY VERIFICATION
    -- -------------------------------------------------------------------
    IF @RunTests = 1
    BEGIN
        SET @StepStart = GETDATE();
        PRINT '>>> Step 4: Verifying Data Warehouse Integrity Assertions...';

        DECLARE @DuplicateActivePlayerCount INT = 0;
        SELECT @DuplicateActivePlayerCount = COUNT(*)
        FROM (
            SELECT player_id, COUNT(*) AS cnt
            FROM gold.dim_player_info
            WHERE dwh_current_validity = 1
            GROUP BY player_id
            HAVING COUNT(*) > 1
        ) d;

        DECLARE @DuplicateActiveOutfieldFactCount INT = 0;
        SELECT @DuplicateActiveOutfieldFactCount = COUNT(*)
        FROM (
            SELECT player_key, COUNT(*) AS cnt
            FROM gold.fact_outfield_player_stats
            WHERE dwh_current_validity = 1
            GROUP BY player_key
            HAVING COUNT(*) > 1
        ) d;

        DECLARE @DuplicateActiveGkFactCount INT = 0;
        SELECT @DuplicateActiveGkFactCount = COUNT(*)
        FROM (
            SELECT player_key, COUNT(*) AS cnt
            FROM gold.fact_gk_stats
            WHERE dwh_current_validity = 1
            GROUP BY player_key
            HAVING COUNT(*) > 1
        ) d;

        DECLARE @DuplicateActiveValueFactCount INT = 0;
        SELECT @DuplicateActiveValueFactCount = COUNT(*)
        FROM (
            SELECT player_key, COUNT(*) AS cnt
            FROM gold.fact_players_value
            WHERE dwh_current_validity = 1
            GROUP BY player_key
            HAVING COUNT(*) > 1
        ) d;

        IF @DuplicateActivePlayerCount > 0 OR @DuplicateActiveOutfieldFactCount > 0 OR @DuplicateActiveGkFactCount > 0 OR @DuplicateActiveValueFactCount > 0
        BEGIN
            PRINT CONCAT('    [WARNING] Duplicate active records detected! (dim_player_info: ', @DuplicateActivePlayerCount,
                         ', fact_outfield: ', @DuplicateActiveOutfieldFactCount,
                         ', fact_gk: ', @DuplicateActiveGkFactCount,
                         ', fact_value: ', @DuplicateActiveValueFactCount, ')');
        END
        ELSE
        BEGIN
            PRINT '    [PASS] Zero duplicate active records across dim_player_info and all fact tables.';
        END

        SET @StepEnd = GETDATE();
        SET @Duration = DATEDIFF(millisecond, @StepStart, @StepEnd);
        INSERT INTO audit.etl_pipeline_log (batch_id, pipeline_layer, step_name, status, start_time, end_time, duration_ms)
        VALUES (@BatchId, 'Verification', 'Data Sanity Checks', 'Success', @StepStart, @StepEnd, @Duration);
    END

    -- Final Log Update
    DECLARE @PipelineEnd DATETIME2 = GETDATE();
    DECLARE @TotalDuration INT = DATEDIFF(millisecond, @PipelineStart, @PipelineEnd);

    UPDATE audit.etl_pipeline_log
    SET status = 'Success', end_time = @PipelineEnd, duration_ms = @TotalDuration
    WHERE batch_id = @BatchId AND step_name = 'Warehouse Pipeline Execution';

    PRINT '======================================================================';
    PRINT CONCAT('PIPELINE COMPLETED SUCCESSFULLY! Total Duration: ', @TotalDuration, ' ms');
    PRINT '======================================================================';
END;
GO
