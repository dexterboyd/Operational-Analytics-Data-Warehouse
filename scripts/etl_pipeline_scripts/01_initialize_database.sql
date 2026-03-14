/*
=============================================================
Database Initialization
Project:     FedEx Operations Data Warehouse
Version:     2.0

Description:
    Drops and recreates Fedex_Ops_Database to ensure a clean
    environment for rebuilding the data warehouse.

WARNING: DESTRUCTIVE SCRIPT

    This script will DROP and RECREATE the database:
        Fedex_Ops_Database

    Running this script will permanently delete:
        - All tables
        - All views
        - All stored procedures
        - All data within the database

    This script should ONLY be used in:
        - Development environments
        - Local testing environments
        - Initial project setup

    DO NOT run this script in a production environment.

Change Log:
    v2.0 - Fixed corrupted bullet characters in header comment.
         - Added explicit collation (Latin1_General_CI_AS) to
           CREATE DATABASE so string comparisons are consistent
           regardless of the server's default collation.
         - Added ALTER DATABASE ... SET COMPATIBILITY_LEVEL
           after creation to pin optimizer behavior to SQL
           Server 2019 (150). Adjust if targeting a different
           version.
         - Added ALTER DATABASE ... SET RECOVERY SIMPLE for
           dev/test environments to prevent runaway log growth
           during bulk loads. Remove or change to FULL in
           production.
=============================================================
*/

USE master;
GO

/*=============================================================
  STEP 1: DROP EXISTING DATABASE (IF EXISTS)
  Forces all active connections to disconnect before dropping
  so the DROP does not hang on open sessions.
=============================================================*/
IF EXISTS (
    SELECT name
    FROM   sys.databases
    WHERE  name = 'Fedex_Ops_Database'
)
BEGIN
    PRINT 'Existing database found. Preparing to drop...';

    -- Force-disconnect all active sessions before dropping
    ALTER DATABASE Fedex_Ops_Database
        SET SINGLE_USER
        WITH ROLLBACK IMMEDIATE;

    DROP DATABASE Fedex_Ops_Database;

    PRINT 'Old database dropped successfully.';
END
GO

/*=============================================================
  STEP 2: CREATE FRESH DATABASE
  Explicit collation prevents silent string-comparison
  mismatches when the server default collation differs from
  what the pipeline expects.
=============================================================*/
CREATE DATABASE Fedex_Ops_Database
    COLLATE Latin1_General_CI_AS;
GO

/*=============================================================
  STEP 3: SET COMPATIBILITY LEVEL
  Pins the query optimizer to SQL Server 2019 behavior (150).
  Change to 160 for SQL Server 2022, 140 for 2017, etc.
=============================================================*/
ALTER DATABASE Fedex_Ops_Database
    SET COMPATIBILITY_LEVEL = 150;
GO

/*=============================================================
  STEP 4: SET RECOVERY MODEL
  SIMPLE recovery prevents transaction log growth during bulk
  loads in dev/test. Switch to FULL for production databases
  that require point-in-time restore capability.
=============================================================*/
ALTER DATABASE Fedex_Ops_Database
    SET RECOVERY SIMPLE;
GO

/*=============================================================
  STEP 5: SWITCH CONTEXT
=============================================================*/
USE Fedex_Ops_Database;
GO

PRINT 'Fedex_Ops_Database created successfully.';
PRINT 'Collation : Latin1_General_CI_AS';
PRINT 'Compat    : 150 (SQL Server 2019)';
PRINT 'Recovery  : SIMPLE (dev/test only)';
GO
