/*
======================================================================
Creates a Stored Procedure: Load Bronze Layer (Source -> Bronze Layer)
======================================================================
   This script is designed to create a stored procedure that loads external 'csv'
  files into the bronze schema. This procedure:
    - truncates the bronze tables before loading the data.
    - uses the 'BULK INSERT' to load data into the tables.

  This Stored Procedure does not require any parameters.
  Example: EXECUTE bronze.load_bronze;
======================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '====================';
		--
	
		PRINT '--------------------------';
		PRINT 'LOADING TABLES FROM FMDATA';
		PRINT '--------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_team_players';
		TRUNCATE TABLE bronze.fmdata_team_players;

		PRINT '>>INSERTING DATA INTO: bronze.fmdata_team_players';
		BULK INSERT bronze.fmdata_team_players
		FROM 'C:\Users\olufe\Documents\FM data\team_players.csv'
		WITH (
			FIRSTROW = 2,
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------';

		--
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_team_gks';
		TRUNCATE TABLE bronze.fmdata_team_gks;

		PRINT '>>INSERTING DATA INTO: bronze.fmdata_team_gks';
		BULK INSERT bronze.fmdata_team_gks
		FROM 'C:\Users\olufe\Documents\FM data\team_gks.csv'
		WITH (
			FIRSTROW = 2,
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';;
		PRINT '---------------------------------------';

		--
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_manager_data';
		TRUNCATE TABLE bronze.fmdata_manager_data;

		PRINT '>>INSERTING DATA INTO: bronze.fmdata_manager_data';
		BULK INSERT bronze.fmdata_manager_data
		FROM 'C:\Users\olufe\Documents\FM data\mngrs_data.csv'
		WITH (
			FIRSTROW = 2,
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';;
		PRINT '---------------------------------------';

		--
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_interested_out_players1';
		TRUNCATE TABLE bronze.fmdata_interested_out_players1;

		PRINT '>>INSERTING DATA INTO: bronze.fmdata_interested_out_players1';
		BULK INSERT bronze.fmdata_interested_out_players1
		FROM 'C:\Users\olufe\Documents\FM data\interested_out_players1.csv'
		WITH (
			FIRSTROW = 2,
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';;
		PRINT '---------------------------------------';

		--
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_interested_out_players2';
		TRUNCATE TABLE bronze.fmdata_interested_out_players2;

		PRINT '>>INSERTING DATA INTO: bronze.fmdata_interested_out_players2';
		BULK INSERT bronze.fmdata_interested_out_players2
		FROM 'C:\Users\olufe\Documents\FM data\interested_out_players2.csv'
		WITH (
			FIRSTROW = 2,
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';;
		PRINT '---------------------------------------';

		--
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_interested_out_players3';
		TRUNCATE TABLE bronze.fmdata_interested_out_players3;

		PRINT '>>INSERTING DATA INTO: bronze.fmdata_interested_out_players3';
		BULK INSERT bronze.fmdata_interested_out_players3
		FROM 'C:\Users\olufe\Documents\FM data\interested_out_players3.csv'
		WITH (
			FIRSTROW = 2,
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';;
		PRINT '---------------------------------------';

		--
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_interested_gks';
		TRUNCATE TABLE bronze.fmdata_interested_gks;

		PRINT '>>INSERTING DATA INTO: bronze.fmdata_interested_gks';
		BULK INSERT bronze.fmdata_interested_gks
		FROM 'C:\Users\olufe\Documents\FM data\interested_gks.csv'
		WITH (
			FIRSTROW = 2,
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------';
		--
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING: bronze.fmdata_possession_data';
		TRUNCATE TABLE bronze.fmdata_possession_data;

		PRINT '>>INSERTING DATA INTO: bronze.fmdata_possession_data';
		BULK INSERT bronze.fmdata_possession_data
		FROM 'C:\Users\olufe\Documents\FM data\possession_data.csv'
		WITH (
			FIRSTROW = 2,
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>DURATION: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=================================';
		PRINT 'LOADING OF BRONZE LAYER COMPLETED';
		PRINT '	TOTAL DURATION: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '=================================';
	END TRY
	BEGIN CATCH
		PRINT '=================================';
		PRINT 'ERROR DURING LOADING BRONZE LAYER';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================';
	END CATCH
END
