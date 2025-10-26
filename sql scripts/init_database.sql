/*
============================================
Creating the Database & necessary Schemas
============================================
Script Purpose:
	This script is design to create a new database "FiorentinaDW" after checking if it exists.
	If it does exist, it is dropped and recreated. The script also creates three schemas within the database:
	"bronze", "silver" and "gold".

WARNING:
	Running this script will drop the "FiorentinaDW" db if it exists.
	All data in the db will be permanently lost. Proceed with caustion 
	and confirm the exitence of proper backups before running this script.

*/

-- Create Database "FiorentinaDW"
USE master;
GO

-- Drop and recreate the "FiorentinaDW" db
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'FiorentinaDW')
BEGIN
	ALTER DATABASE FiorentinaDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE FiorentinaDW;
END;
GO

-- Create the "FiorentinaDW" db
CREATE DATABASE FiorentinaDW;

USE FiorentinaDW;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
