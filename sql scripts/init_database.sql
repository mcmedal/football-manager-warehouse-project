/*
============================================
Creating the Database & necessary Schemas
============================================
Script Purpose:
	This script is design to create a new database "DW" after checking if it exists.
	If it does exist, it is dropped and recreated. The script also creates three schemas within the database:
	"bronze", "silver" and "gold".

WARNING:
	Running this script will drop the "DW" db if it exists.
	All data in the db will be permanently lost. Proceed with caustion 
	and confirm the exitence of proper backups before running this script.

*/

-- Create Database "DW"
USE master;
GO

-- Drop and recreate the "DW" db
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DW')
BEGIN
	ALTER DATABASE DW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DW;
END;
GO

-- Create the "DW" db with UTF-8 collation for cross-platform Linux & Windows support
CREATE DATABASE DW COLLATE Latin1_General_100_CI_AS_SC_UTF8;
GO

USE DW;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
CREATE SCHEMA audit;
GO
