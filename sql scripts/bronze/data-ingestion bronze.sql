/*
======================================================================
Creates a Stored Procedure: Load Bronze Layer (Source -> Bronze Layer)
======================================================================
   This script creates a stored procedure that loads external CSV
   files into the bronze schema. This procedure:
     - Truncates bronze tables before loading data.
     - Uses 'BULK INSERT' for high-throughput data loading.
     - Decouples file paths via configurable parameter @DataPath.
     - DYNAMICALLY discovers and ingests ANY number of interested outfield
       player partition files (e.g. interested_out_players1.csv through
       interested_out_players8.csv or higher) into the unified table
       bronze.fmdata_interested_out_players without hardcoding table numbers.

   Parameters:
     @DataPath     - Directory path containing FM24 CSV exports 
                     (Default: 'C:\Users\<username>\Documents\FM data\')
     @MaxFileParts - Maximum file partition index to probe in fallback mode
                     (Default: 50)

   Examples:
     EXEC bronze.load_bronze;
     EXEC bronze.load_bronze @DataPath = 'C:\Users\<username>\Documents\FM data\';
     EXEC bronze.load_bronze @DataPath = 'D:\FM24\Exports\', @MaxFileParts = 20;
======================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
    @DataPath NVARCHAR(500) = 'C:\Users\<username>\Documents\FM data\',
    @MaxFileParts INT = 50
AS 
BEGIN
	SET NOCOUNT ON;

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	DECLARE @sql NVARCHAR(MAX);

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

	-- Normalize data directory path with trailing separator based on platform/path format
	DECLARE @Sep CHAR(1) = CASE WHEN @DataPath LIKE '/%' OR @DataPath LIKE '%/%' THEN '/' ELSE '\' END;
	IF RIGHT(@DataPath, 1) != '/' AND RIGHT(@DataPath, 1) != '\'
	BEGIN
		SET @DataPath = @DataPath + @Sep;
	END;
	IF @Sep = '/'
	BEGIN
		SET @DataPath = REPLACE(@DataPath, '\', '/');
	END;

	-- Build cross-platform BULK INSERT options
	-- (SQL Server on Linux supports CODEPAGE = 'RAW' with UTF-8 collation; Windows supports CODEPAGE = '65001')
	DECLARE @BulkOptions NVARCHAR(500) = N'FIRSTROW = 2, FORMAT = ''CSV'', TABLOCK';
	IF @HostPlatform = 'Linux'
		SET @BulkOptions = @BulkOptions + N', CODEPAGE = ''RAW''';
	ELSE
		SET @BulkOptions = @BulkOptions + N', CODEPAGE = ''65001''';

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'LOADING BRONZE LAYER (DYNAMIC INGESTION)';
		PRINT 'HOST PLATFORM:    ' + @HostPlatform;
		PRINT 'SOURCE DIRECTORY: ' + @DataPath;
		PRINT 'BULK OPTIONS:     ' + @BulkOptions;
		PRINT '=======================================================';

		-- ------------------------------------------------------------
		-- 1. LOAD TEAM SQUAD PLAYERS (OUTFIELD)
		-- ------------------------------------------------------------
		PRINT '-------------------------------------------------------';
		PRINT 'LOADING: bronze.fmdata_team_players';
		PRINT '-------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_team_players';
		TRUNCATE TABLE bronze.fmdata_team_players;

		PRINT '>>INSERTING DATA FROM: ' + @DataPath + 'team_players.csv';
		SET @sql = N'BULK INSERT bronze.fmdata_team_players
		             FROM ''' + @DataPath + N'team_players.csv''
		             WITH (' + @BulkOptions + N');';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';

		-- ------------------------------------------------------------
		-- 2. LOAD TEAM SQUAD GOALKEEPERS
		-- ------------------------------------------------------------
		PRINT '-------------------------------------------------------';
		PRINT 'LOADING: bronze.fmdata_team_gks';
		PRINT '-------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_team_gks';
		TRUNCATE TABLE bronze.fmdata_team_gks;

		PRINT '>>INSERTING DATA FROM: ' + @DataPath + 'team_gks.csv';
		SET @sql = N'BULK INSERT bronze.fmdata_team_gks
		             FROM ''' + @DataPath + N'team_gks.csv''
		             WITH (' + @BulkOptions + N');';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';

		-- ------------------------------------------------------------
		-- 3. LOAD DYNAMIC INTERESTED OUTFIELD PLAYERS (PARTITIONS 1..N)
		-- ------------------------------------------------------------
		PRINT '-------------------------------------------------------';
		PRINT 'LOADING: bronze.fmdata_interested_out_players (DYNAMIC)';
		PRINT '-------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_interested_out_players';
		TRUNCATE TABLE bronze.fmdata_interested_out_players;

		-- Temp table to collect discovered partition CSV files
		IF OBJECT_ID('tempdb..#OutfieldFiles') IS NOT NULL DROP TABLE #OutfieldFiles;
		CREATE TABLE #OutfieldFiles (
			file_name NVARCHAR(255),
			file_path NVARCHAR(500),
			file_order INT
		);

		-- Discovery Strategy A: Query directory using xp_dirtree
		DECLARE @DiscoveredViaDirTree BIT = 0;
		BEGIN TRY
			IF OBJECT_ID('tempdb..#DirTree') IS NOT NULL DROP TABLE #DirTree;
			CREATE TABLE #DirTree (
				subdirectory NVARCHAR(255),
				depth INT,
				isfile BIT
			);

			INSERT INTO #DirTree (subdirectory, depth, isfile)
			EXEC master.sys.xp_dirtree @DataPath, 1, 1;

			INSERT INTO #OutfieldFiles (file_name, file_path, file_order)
			SELECT 
				subdirectory,
				@DataPath + subdirectory,
				CASE 
					WHEN subdirectory = 'interested_out_players.csv' THEN 0
					ELSE TRY_CAST(REPLACE(REPLACE(subdirectory, 'interested_out_players', ''), '.csv', '') AS INT)
				END
			FROM #DirTree
			WHERE isfile = 1
			  AND (
			      subdirectory = 'interested_out_players.csv'
			      OR subdirectory LIKE 'interested_out_players[0-9]%.csv'
			  );

			IF EXISTS (SELECT 1 FROM #OutfieldFiles)
				SET @DiscoveredViaDirTree = 1;

			DROP TABLE #DirTree;
		END TRY
		BEGIN CATCH
			-- Fall back to file probe strategy if xp_dirtree is restricted
			SET @DiscoveredViaDirTree = 0;
		END CATCH;

		-- Discovery Strategy B: Sequential file probe using xp_fileexist (Fallback)
		IF @DiscoveredViaDirTree = 0
		BEGIN
			DECLARE @probe_idx INT = 0;
			DECLARE @candidate_name NVARCHAR(255);
			DECLARE @candidate_path NVARCHAR(500);
			DECLARE @file_exists INT;

			IF OBJECT_ID('tempdb..#FileCheck') IS NOT NULL DROP TABLE #FileCheck;
			CREATE TABLE #FileCheck (FileExists INT, FileIsDirectory INT, ParentDirExists INT);

			WHILE @probe_idx <= @MaxFileParts
			BEGIN
				IF @probe_idx = 0
					SET @candidate_name = 'interested_out_players.csv';
				ELSE
					SET @candidate_name = 'interested_out_players' + CAST(@probe_idx AS NVARCHAR(10)) + '.csv';

				SET @candidate_path = @DataPath + @candidate_name;
				TRUNCATE TABLE #FileCheck;

				BEGIN TRY
					INSERT INTO #FileCheck EXEC master.dbo.xp_fileexist @candidate_path;
					SELECT @file_exists = FileExists FROM #FileCheck;
				END TRY
				BEGIN CATCH
					SET @file_exists = 0;
				END CATCH;

				IF @file_exists = 1
				BEGIN
					INSERT INTO #OutfieldFiles (file_name, file_path, file_order)
					VALUES (@candidate_name, @candidate_path, @probe_idx);
				END;

				SET @probe_idx = @probe_idx + 1;
			END;

			DROP TABLE #FileCheck;
		END;

		-- Iteratively bulk insert each discovered partition file
		DECLARE @total_partitions INT = (SELECT COUNT(*) FROM #OutfieldFiles);
		DECLARE @cur_partition_name NVARCHAR(255), @cur_partition_path NVARCHAR(500);
		DECLARE @partitions_loaded INT = 0;

		IF @total_partitions = 0
		BEGIN
			PRINT '>>[WARNING] No interested outfield player CSV files found in ' + @DataPath;
		END
		ELSE
		BEGIN
			PRINT '>>Found ' + CAST(@total_partitions AS NVARCHAR(10)) + ' partition file(s) to ingest.';

			DECLARE cur_files CURSOR LOCAL FAST_FORWARD FOR
			SELECT file_name, file_path 
			FROM #OutfieldFiles 
			ORDER BY ISNULL(file_order, 9999), file_name;

			OPEN cur_files;
			FETCH NEXT FROM cur_files INTO @cur_partition_name, @cur_partition_path;

			WHILE @@FETCH_STATUS = 0
			BEGIN
				PRINT '  >> Bulk inserting [' + CAST(@partitions_loaded + 1 AS NVARCHAR(10)) + '/' + CAST(@total_partitions AS NVARCHAR(10)) + ']: ' + @cur_partition_name;
				
				SET @sql = N'BULK INSERT bronze.fmdata_interested_out_players
				             FROM ''' + @cur_partition_path + N'''
				             WITH (' + @BulkOptions + N');';
				EXEC sp_executesql @sql;

				SET @partitions_loaded = @partitions_loaded + 1;
				FETCH NEXT FROM cur_files INTO @cur_partition_name, @cur_partition_path;
			END;

			CLOSE cur_files;
			DEALLOCATE cur_files;
		END;

		DROP TABLE #OutfieldFiles;

		SET @end_time = GETDATE();
		PRINT '>>SUCCESSFULLY INGESTED ' + CAST(@partitions_loaded AS NVARCHAR(10)) + ' OUTFIELD FILE PARTITION(S)';
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';

		-- ------------------------------------------------------------
		-- 4. LOAD SCOUTED TARGET GOALKEEPERS
		-- ------------------------------------------------------------
		PRINT '-------------------------------------------------------';
		PRINT 'LOADING: bronze.fmdata_interested_gks';
		PRINT '-------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_interested_gks';
		TRUNCATE TABLE bronze.fmdata_interested_gks;

		PRINT '>>INSERTING DATA FROM: ' + @DataPath + 'interested_gks.csv';
		SET @sql = N'BULK INSERT bronze.fmdata_interested_gks
		             FROM ''' + @DataPath + N'interested_gks.csv''
		             WITH (' + @BulkOptions + N');';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';

		-- ------------------------------------------------------------
		-- COMPLETED BRONZE BATCH
		-- ------------------------------------------------------------
		SET @batch_end_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'LOADING OF BRONZE LAYER COMPLETED';
		PRINT 'TOTAL DURATION: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '=======================================================';
	END TRY
	BEGIN CATCH
		PRINT '=======================================================';
		PRINT 'ERROR DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================';
		THROW;
	END CATCH
END;
