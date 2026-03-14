/*=============================================================
  ETL STAGING SETUP
  Database: Fedex_Ops_Database
  Version:  2.0

  Purpose:
      1. Create the four pipeline schemas if they do not exist.
      2. Create staging tables if they do not exist.
      3. Bulk-insert raw CSV data into staging tables.
      4. Print an object summary confirming setup is complete.

  Pipeline Layers:
      staging   -> raw imported source data
      clean     -> standardized transformation views
      dw        -> star schema (fact & dimension tables)
      reporting -> aggregated BI views

  Change Log:
      v2.0 - Replaced dynamic SQL schema loop with four
             individual IF NOT EXISTS / EXEC blocks.
             CREATE SCHEMA must be the first statement in a
             batch; the loop pattern works but is brittle and
             harder to read than four explicit blocks.
           - Added composite primary key (RouteID, DriverID)
             to staging_routes. The original had no PK, which
             allowed duplicate rows to load silently and
             corrupt downstream aggregations.
           - Fixed copy/paste comment on staging_routes:
             was "Raw delivery exceptions data", corrected to
             "Raw route performance data".
           - Changed ResolutionTimeHours from INT to
             DECIMAL(6,2) so fractional hours (e.g. 1.5) are
             not silently rounded.
           - Added NOT NULL constraints on all required
             identifier and date columns to document intent
             and catch upstream data problems early.
           - Added MAXERRORS = 0 and ERRORFILE to every BULK
             INSERT so a single bad row produces a diagnostic
             file instead of silently aborting the load.
           - Added CODEPAGE = '65001' (UTF-8) to every BULK
             INSERT to prevent silent encoding corruption for
             non-ASCII characters (e.g. driver names).
           - Wrapped all four BULK INSERTs + TRUNCATEs in a
             single transaction so a partial load failure
             rolls back all tables, leaving staging in a
             consistent all-or-nothing state.
           - Updated BULK INSERT paths to use a configurable
             variable (@DataPath) so the script does not need
             to be edited when run from a different machine.
=============================================================*/

USE Fedex_Ops_Database;
GO

/*=============================================================
  STEP 1: CREATE PIPELINE SCHEMAS
  Each schema is created in its own batch via EXEC so that
  CREATE SCHEMA is always the first statement in its batch,
  as required by SQL Server.
=============================================================*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
ELSE
    PRINT 'Schema already exists: staging';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'clean')
    EXEC('CREATE SCHEMA clean');
ELSE
    PRINT 'Schema already exists: clean';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');
ELSE
    PRINT 'Schema already exists: dw';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'reporting')
    EXEC('CREATE SCHEMA reporting');
ELSE
    PRINT 'Schema already exists: reporting';

GO

/*=============================================================
  STEP 2: CREATE STAGING TABLES
  Tables are created only if they do not already exist so
  this script is safe to re-run without data loss.

  NOT NULL is declared explicitly on all required columns.
  Columns that are legitimately nullable (e.g. ResolvedDate
  for open exceptions) are marked NULL explicitly for clarity.
=============================================================*/

-- -----------------------------------------------------------
-- STAGING TABLE: DELIVERIES
-- Raw delivery records: route, driver, timing, status.
-- -----------------------------------------------------------
IF OBJECT_ID('staging.staging_deliveries', 'U') IS NULL
BEGIN
    CREATE TABLE staging.staging_deliveries (
        DeliveryID           INT          NOT NULL PRIMARY KEY,
        RouteID              NVARCHAR(10) NOT NULL,
        DriverID             NVARCHAR(50) NOT NULL,
        Region               NVARCHAR(10) NOT NULL,
        ShipmentType         NVARCHAR(20) NOT NULL,
        DeliveryDate         DATE         NOT NULL,
        ExpectedDeliveryDate DATE         NULL,      -- Planned date; NULL if not scheduled
        DeliveryStatus       NVARCHAR(20) NOT NULL,  -- Delivered, Delayed, Failed
        PriorityFlag         BIT          NOT NULL
    );
    PRINT 'Table created: staging.staging_deliveries';
END
ELSE
    PRINT 'Table already exists: staging.staging_deliveries';
GO

-- -----------------------------------------------------------
-- STAGING TABLE: DELIVERY EXCEPTIONS
-- Operational issues affecting deliveries (delays, damage,
-- weather events, etc.).
-- -----------------------------------------------------------
IF OBJECT_ID('staging.staging_exceptions', 'U') IS NULL
BEGIN
    CREATE TABLE staging.staging_exceptions (
        ExceptionID          INT          NOT NULL PRIMARY KEY,
        DeliveryID           INT          NOT NULL,
        ExceptionType        NVARCHAR(50) NOT NULL,  -- Delay, Damage, Weather, etc.
        DateReported         DATE         NOT NULL,
        ResolvedDate         DATE         NULL,       -- NULL = exception still open
        ResolutionTimeHours  DECIMAL(6,2) NULL,       -- DECIMAL allows fractional hours
        PriorityFlag         BIT          NOT NULL,
        Region               NVARCHAR(10) NOT NULL
    );
    PRINT 'Table created: staging.staging_exceptions';
END
ELSE
    PRINT 'Table already exists: staging.staging_exceptions';
GO

-- -----------------------------------------------------------
-- STAGING TABLE: ROUTES
-- Raw route performance data: planned vs actual stops and
-- hours for each driver-route combination.
-- -----------------------------------------------------------
IF OBJECT_ID('staging.staging_routes', 'U') IS NULL
BEGIN
    CREATE TABLE staging.staging_routes (
        RouteID       NVARCHAR(10)   NOT NULL,
        DriverID      NVARCHAR(50)   NOT NULL,
        PlannedStops  INT            NOT NULL,
        ActualStops   INT            NOT NULL,
        PlannedHours  DECIMAL(5,2)   NOT NULL,
        ActualHours   DECIMAL(5,2)   NOT NULL,
        Region        NVARCHAR(10)   NOT NULL,
        -- Composite PK prevents duplicate route+driver rows from
        -- loading silently and causing double-counting downstream.
        CONSTRAINT PK_staging_routes PRIMARY KEY (RouteID, DriverID)
    );
    PRINT 'Table created: staging.staging_routes';
END
ELSE
    PRINT 'Table already exists: staging.staging_routes';
GO

-- -----------------------------------------------------------
-- STAGING TABLE: SALES
-- Raw sales transactions tied to deliveries.
-- -----------------------------------------------------------
IF OBJECT_ID('staging.staging_sales', 'U') IS NULL
BEGIN
    CREATE TABLE staging.staging_sales (
        SalesID      INT             NOT NULL PRIMARY KEY,
        DeliveryID   INT             NOT NULL,
        DateKey      DATE            NOT NULL,
        ProductType  NVARCHAR(50)    NOT NULL,
        Region       NVARCHAR(10)    NOT NULL,
        UnitsSold    INT             NOT NULL,
        SalesAmount  DECIMAL(10,2)   NOT NULL
    );
    PRINT 'Table created: staging.staging_sales';
END
ELSE
    PRINT 'Table already exists: staging.staging_sales';
GO


/*=============================================================
  STEP 3: BULK INSERT CSV DATA INTO STAGING TABLES

  CONFIGURATION
  -------------
  Set @DataPath to the folder containing the four CSV files.
  Use a trailing backslash. The script appends the filename.

  Example (local dev):
      SET @DataPath = 'C:\Operational-Analytics-Data-Warehouse\datasets\raw\';

  Error files are written to the same folder with an _errors
  suffix. MAXERRORS = 0 means any bad row aborts the load and
  writes a diagnostic file — do not change this to a higher
  value without understanding the data quality implications.

  TRANSACTION SAFETY
  ------------------
  All four TRUNCATE + BULK INSERT operations run inside a
  single transaction. If any load fails, all four tables are
  rolled back so staging is never left in a partial state.
=============================================================*/

DECLARE @DataPath NVARCHAR(500);

-- *** UPDATE THIS PATH BEFORE RUNNING ***
SET @DataPath = 'C:\Operational-Analytics-Data-Warehouse\datasets\raw\';

-- ---------------------
-- Build full file paths
-- ---------------------
DECLARE @SalesFile      NVARCHAR(500) = @DataPath + 'sales.csv';
DECLARE @DeliveriesFile NVARCHAR(500) = @DataPath + 'deliveries.csv';
DECLARE @RoutesFile     NVARCHAR(500) = @DataPath + 'routes.csv';
DECLARE @ExceptionsFile NVARCHAR(500) = @DataPath + 'exceptions.csv';

DECLARE @SalesErr       NVARCHAR(500) = @DataPath + 'sales_errors.txt';
DECLARE @DeliveriesErr  NVARCHAR(500) = @DataPath + 'deliveries_errors.txt';
DECLARE @RoutesErr      NVARCHAR(500) = @DataPath + 'routes_errors.txt';
DECLARE @ExceptionsErr  NVARCHAR(500) = @DataPath + 'exceptions_errors.txt';

DECLARE @SQL NVARCHAR(MAX);

BEGIN TRANSACTION;
BEGIN TRY

    -- -------------------------------------------------------
    -- SALES
    -- -------------------------------------------------------
    TRUNCATE TABLE staging.staging_sales;

    SET @SQL = '
    BULK INSERT staging.staging_sales
    FROM ''' + @SalesFile + '''
    WITH (
        FIRSTROW        = 2,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR   = ''\n'',
        CODEPAGE        = ''65001'',
        MAXERRORS       = 0,
        ERRORFILE       = ''' + @SalesErr + ''',
        TABLOCK
    );';
    EXEC(@SQL);
    PRINT 'Loaded: staging.staging_sales';

    -- -------------------------------------------------------
    -- DELIVERIES
    -- -------------------------------------------------------
    TRUNCATE TABLE staging.staging_deliveries;

    SET @SQL = '
    BULK INSERT staging.staging_deliveries
    FROM ''' + @DeliveriesFile + '''
    WITH (
        FIRSTROW        = 2,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR   = ''\n'',
        CODEPAGE        = ''65001'',
        MAXERRORS       = 0,
        ERRORFILE       = ''' + @DeliveriesErr + ''',
        TABLOCK
    );';
    EXEC(@SQL);
    PRINT 'Loaded: staging.staging_deliveries';

    -- -------------------------------------------------------
    -- ROUTES
    -- -------------------------------------------------------
    TRUNCATE TABLE staging.staging_routes;

    SET @SQL = '
    BULK INSERT staging.staging_routes
    FROM ''' + @RoutesFile + '''
    WITH (
        FIRSTROW        = 2,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR   = ''\n'',
        CODEPAGE        = ''65001'',
        MAXERRORS       = 0,
        ERRORFILE       = ''' + @RoutesErr + ''',
        TABLOCK
    );';
    EXEC(@SQL);
    PRINT 'Loaded: staging.staging_routes';

    -- -------------------------------------------------------
    -- EXCEPTIONS
    -- -------------------------------------------------------
    TRUNCATE TABLE staging.staging_exceptions;

    SET @SQL = '
    BULK INSERT staging.staging_exceptions
    FROM ''' + @ExceptionsFile + '''
    WITH (
        FIRSTROW        = 2,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR   = ''\n'',
        CODEPAGE        = ''65001'',
        MAXERRORS       = 0,
        ERRORFILE       = ''' + @ExceptionsErr + ''',
        TABLOCK
    );';
    EXEC(@SQL);
    PRINT 'Loaded: staging.staging_exceptions';

    COMMIT TRANSACTION;
    PRINT 'All CSV data loaded into staging tables successfully.';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT 'BULK INSERT failed. All staging tables have been rolled back.';
    PRINT 'Error: ' + ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  OPTION 2: TRANSFER DBO TABLES INTO STAGING SCHEMA
  Use this block instead of BULK INSERT if raw data was
  imported directly into dbo (common with SSMS import wizard).
  Uncomment and run after verifying table names.
=============================================================*/
/*
DECLARE @Tables TABLE (TableName NVARCHAR(100));

INSERT INTO @Tables VALUES
    ('staging_sales'),
    ('staging_deliveries'),
    ('staging_routes'),
    ('staging_exceptions');

DECLARE @TransferSQL NVARCHAR(MAX) = '';

SELECT @TransferSQL = @TransferSQL + '
IF OBJECT_ID(''dbo.' + TableName + ''', ''U'') IS NOT NULL
BEGIN
    ALTER SCHEMA staging TRANSFER dbo.' + TableName + ';
    PRINT ''Transferred: ' + TableName + ''';
END'
FROM @Tables;

EXEC(@TransferSQL);
GO
*/


/*=============================================================
  STEP 4: ETL SCHEMA OBJECT SUMMARY
  Quick post-setup report confirming tables and views exist
  in each pipeline layer.
=============================================================*/

PRINT '--- ETL SCHEMA OBJECT SUMMARY ---';

SELECT
    s.name AS SchemaName,
    SUM(CASE WHEN o.type = 'U' THEN 1 ELSE 0 END) AS TableCount,
    SUM(CASE WHEN o.type = 'V' THEN 1 ELSE 0 END) AS ViewCount
FROM sys.schemas s
LEFT JOIN sys.objects o
    ON o.schema_id = s.schema_id
WHERE s.name IN ('staging', 'clean', 'dw', 'reporting')
GROUP BY s.name
ORDER BY
    CASE s.name
        WHEN 'staging'   THEN 1
        WHEN 'clean'     THEN 2
        WHEN 'dw'        THEN 3
        WHEN 'reporting' THEN 4
    END;

PRINT '--- END OF SUMMARY ---';
GO
