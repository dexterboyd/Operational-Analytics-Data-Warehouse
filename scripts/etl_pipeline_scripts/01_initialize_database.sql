/*
=============================================================
Database Initialization Script
Project: Fedex Operations Data Warehouse
Description: Drops and recreates the Fedex_Ops_Database to 
ensure a clean environment for rebuilding the data warehouse.

WARNING: DESTRUCTIVE SCRIPT

This script will DROP and RECREATE the database:
    Fedex_Ops_Database

Running this script will permanently delete:
    • All tables
    • All views
    • All stored procedures
    • All data within the database

This script should ONLY be used in:
    • Development environments
    • Local testing environments
    • Initial project setup

DO NOT run this script in a production environment.
Author: Dexter M. Boyd
=============================================================
*/

USE master;
GO

-------------------------------------------------------------
-- Check if the database already exists
-------------------------------------------------------------
IF EXISTS (SELECT name 
           FROM sys.databases 
           WHERE name = 'Fedex_Ops_Database')
BEGIN
    PRINT 'Existing database found. Preparing to drop...';

    -- Force disconnect all active connections
    ALTER DATABASE Fedex_Ops_Database
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE;

    -- Drop the existing database
    DROP DATABASE Fedex_Ops_Database;
    PRINT 'Old database dropped successfully.';

END
GO

-- Create a fresh database
CREATE DATABASE Fedex_Ops_Database;
GO

-- Switch context to the new database
USE Fedex_Ops_Database;
GO

-- 7. Confirmation
PRINT 'Fedex_Ops_Database created successfully.';
GO
