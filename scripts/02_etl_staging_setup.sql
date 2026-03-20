/*=============================================================
  ETL STAGING SETUP
  Database: Fedex_Ops_Database
  Version:  3.0

  Purpose:
      1. Create the four pipeline schemas if they do not exist.
      2. Create staging tables if they do not exist.
      3. Create load_log metadata table if it does not exist.
      4. Print an object summary confirming setup is complete.

  Pipeline Layers:
      staging   -> raw imported source data
      clean     -> standardized transformation views
      dw        -> star schema (fact & dimension tables)
      reporting -> aggregated BI views

  Data Loading:
      CSV data is loaded into staging tables via load_staging.py.
      Run that script after this one to populate the tables.
      load_staging.py location: C:\ETL_DATA\load_staging.py

  Validation:
      Run staging_layer_validation_v2.sql after each load to
      confirm data landed correctly before proceeding to the
      clean layer.

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
  Columns that are legitimately nullable are marked NULL
  explicitly for clarity.
=============================================================*/

-- -----------------------------------------------------------
-- STAGING TABLE: DELIVERIES
-- Raw delivery records: route, driver, timing, status.
-- DriverID is NULL: ~1,010 empty values exist in source CSV.
-- DeliveryDate / ExpectedDeliveryDate are DATETIME2: source
-- CSV includes full timestamps e.g. 2024-09-08 02:05:35.
-- -----------------------------------------------------------
IF OBJECT_ID('staging.staging_deliveries', 'U') IS NULL
BEGIN
    CREATE TABLE staging.staging_deliveries (
        DeliveryID           INT          NOT NULL PRIMARY KEY,
        RouteID              NVARCHAR(10) NOT NULL,
        DriverID             NVARCHAR(50) NULL,           -- NULL: empty values in source
        Region               NVARCHAR(10) NOT NULL,
        ShipmentType         NVARCHAR(20) NOT NULL,
        DeliveryDate         DATETIME2    NOT NULL,       -- DATETIME2: source has timestamps
        ExpectedDeliveryDate DATETIME2    NULL,           -- NULL if not scheduled
        DeliveryStatus       NVARCHAR(20) NOT NULL,       -- On-Time, Late, Exception
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
-- DateReported / ResolvedDate are DATETIME2: source CSV
-- includes full timestamps e.g. 2024-02-09 04:17:37.
-- -----------------------------------------------------------
IF OBJECT_ID('staging.staging_exceptions', 'U') IS NULL
BEGIN
    CREATE TABLE staging.staging_exceptions (
        ExceptionID         INT          NOT NULL PRIMARY KEY,
        DeliveryID          INT          NOT NULL,
        ExceptionType       NVARCHAR(50) NOT NULL,        -- Delay, Damage, Weather, etc.
        DateReported        DATETIME2    NOT NULL,        -- DATETIME2: source has timestamps
        ResolvedDate        DATETIME2    NULL,            -- NULL = exception still open
        ResolutionTimeHours DECIMAL(6,2) NULL,            -- NULL if unresolved
        PriorityFlag        BIT          NOT NULL,
        Region              NVARCHAR(10) NOT NULL
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
        RouteID      NVARCHAR(10) NOT NULL,
        DriverID     NVARCHAR(50) NULL,
        PlannedStops INT          NOT NULL,
        ActualStops  INT          NOT NULL,
        PlannedHours DECIMAL(5,2) NOT NULL,
        ActualHours  DECIMAL(5,2) NOT NULL,
        Region       NVARCHAR(10) NOT NULL,
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
-- DateKey is DATETIME2: source CSV includes full timestamps
-- e.g. 2025-03-11 09:37:19.
-- -----------------------------------------------------------
IF OBJECT_ID('staging.staging_sales', 'U') IS NULL
BEGIN
    CREATE TABLE staging.staging_sales (
        SalesID     INT           NOT NULL PRIMARY KEY,
        DeliveryID  INT           NOT NULL,
        DateKey     DATETIME2     NOT NULL,               -- DATETIME2: source has timestamps
        ProductType NVARCHAR(50)  NOT NULL,
        Region      NVARCHAR(10)  NOT NULL,
        UnitsSold   INT           NOT NULL,
        SalesAmount DECIMAL(10,2) NOT NULL
    );
    PRINT 'Table created: staging.staging_sales';
END
ELSE
    PRINT 'Table already exists: staging.staging_sales';
GO

-- -----------------------------------------------------------
-- METADATA TABLE: LOAD LOG
-- Records row counts and timestamps from each
-- load_staging.py run. Used by the validation script for
-- dynamic row count checks instead of hardcoded values.
-- Rows are never deleted -- full load history is retained.
-- -----------------------------------------------------------
IF OBJECT_ID('staging.load_log', 'U') IS NULL
BEGIN
    CREATE TABLE staging.load_log (
        LoadID      INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        TableName   NVARCHAR(100)     NOT NULL,
        RowsLoaded  INT               NOT NULL,
        LoadedAt    DATETIME2         NOT NULL DEFAULT GETDATE()
    );
    PRINT 'Table created: staging.load_log';
END
ELSE
    PRINT 'Table already exists: staging.load_log';
GO

/*=============================================================
  STEP 3: ETL SCHEMA OBJECT SUMMARY
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
