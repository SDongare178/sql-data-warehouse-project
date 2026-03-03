
/*
CREATE DB and Schemas
SCript pupose:
  This script creates a new database 'DataWarehouse and after checks if it exists,drop. then screate 3 schemas
 IMP WARNING - Running this script will permanently delete the database if already it exists so proceed with caution.

*/

USE master;

--Drop and recreate the datawarehouse if exist--
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

  --Create the DataWarehouse Object--
CREATE DATABASE DataWarehouse;
USE DataWarehouse;
GO

 --CREATE SCHEMA-- 
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
