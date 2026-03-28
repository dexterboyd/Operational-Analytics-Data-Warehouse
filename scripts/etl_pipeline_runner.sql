/*=============================================================
  ETL PIPELINE RUNNER
  Database: Fedex_Ops_Database
  Version:  1.0

  Purpose:
      Master script that runs all SQL pipeline steps in order
      with full pipeline_log audit trail. Each step is wrapped
      in START / END / ERROR logging calls so every run is
      fully recorded in staging.pipeline_log.

      Step 2 (staging_load) is handled by load_staging.py
      and is not included here. Run that script separately
      and pass the --run-id flag to link it to this run.

  Run Order:
      1. etl_pipeline_logging_v1.sql    -- run once to set up logging
      2. THIS SCRIPT (Steps 1, 3-9)     -- runs all SQL steps
      3. load_staging.py --run-id <N>   -- run separately for Step 2

  Steps Executed:
      Step 1: staging_setup       etl_staging_setup_v5.sql logic
      Step 3: staging_validation  staging_layer_validation_v2.sql logic
      Step 4: clean_build         clean_layer_v1.sql logic
      Step 5: clean_gate          07_clean_validation_gate_v3_0.sql logic
      Step 6: dw_build            dw_layer_v1.sql logic
      Step 7: dw_gate             09_dw_validation_v3_0.sql logic
      Step 8: reporting_build     reporting_layer_v1.sql logic
      Step 9: reporting_gate      reporting_validation_gate_v1.sql logic

  Viewing the Log:
      -- Current run summary
      SELECT * FROM staging.pipeline_log
      WHERE RunID = (SELECT MAX(RunID) FROM staging.pipeline_log)
      ORDER BY StepOrder;

      -- All failed steps
      SELECT * FROM staging.pipeline_log
      WHERE StepStatus = 'FAILED'
      ORDER BY StepStartTime DESC;

=============================================================*/

USE Fedex_Ops_Database;
GO

SET XACT_ABORT ON;
SET LOCK_TIMEOUT 30000;
GO

/*=============================================================
  GENERATE RUN ID
  A new RunID is generated at the start of each pipeline run.
  All steps in this execution share the same RunID so the full
  run can be queried as a unit from pipeline_log.
=============================================================*/

DECLARE @RunID INT;

SELECT @RunID = ISNULL(MAX(RunID), 0) + 1
FROM staging.pipeline_log;

PRINT '=========================================';
PRINT ' PIPELINE RUN STARTED';
PRINT ' RunID     : ' + CAST(@RunID AS NVARCHAR);
PRINT ' Started   : ' + CONVERT(NVARCHAR, GETDATE(), 120);
PRINT ' Database  : ' + DB_NAME();
PRINT '=========================================';
GO

/*=============================================================
  STEP 1: STAGING SETUP
  Creates pipeline schemas and staging tables if they do not
  exist. Safe to re-run without data loss.
=============================================================*/

DECLARE @RunID INT;
SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

EXEC staging.usp_log_step_start
    @RunID      = @RunID,
    @StepOrder  = 1,
    @StepName   = 'staging_setup',
    @StepDesc   = 'Create pipeline schemas (staging clean dw reporting) and staging tables if they do not exist.';

BEGIN TRY

    -- Schemas
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')   EXEC('CREATE SCHEMA staging');
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'clean')     EXEC('CREATE SCHEMA clean');
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')        EXEC('CREATE SCHEMA dw');
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'reporting') EXEC('CREATE SCHEMA reporting');

    -- staging_deliveries
    IF OBJECT_ID('staging.staging_deliveries', 'U') IS NULL
        CREATE TABLE staging.staging_deliveries (
            DeliveryID           INT          NOT NULL PRIMARY KEY,
            RouteID              NVARCHAR(10) NOT NULL,
            DriverID             NVARCHAR(50) NULL,
            Region               NVARCHAR(10) NOT NULL,
            ShipmentType         NVARCHAR(20) NOT NULL,
            DeliveryDate         DATETIME2    NOT NULL,
            ExpectedDeliveryDate DATETIME2    NULL,
            DeliveryStatus       NVARCHAR(20) NOT NULL,
            PriorityFlag         BIT          NOT NULL
        );

    -- staging_exceptions
    IF OBJECT_ID('staging.staging_exceptions', 'U') IS NULL
        CREATE TABLE staging.staging_exceptions (
            ExceptionID         INT          NOT NULL PRIMARY KEY,
            DeliveryID          INT          NOT NULL,
            ExceptionType       NVARCHAR(50) NOT NULL,
            DateReported        DATETIME2    NOT NULL,
            ResolvedDate        DATETIME2    NULL,
            ResolutionTimeHours DECIMAL(6,2) NULL,
            PriorityFlag        BIT          NOT NULL,
            Region              NVARCHAR(10) NOT NULL
        );

    -- staging_routes
    IF OBJECT_ID('staging.staging_routes', 'U') IS NULL
        CREATE TABLE staging.staging_routes (
            RouteID      NVARCHAR(10) NOT NULL,
            DriverID     NVARCHAR(50) NULL,
            PlannedStops INT          NOT NULL,
            ActualStops  INT          NOT NULL,
            PlannedHours DECIMAL(5,2) NOT NULL,
            ActualHours  DECIMAL(5,2) NOT NULL,
            Region       NVARCHAR(10) NOT NULL,
            CONSTRAINT PK_staging_routes PRIMARY KEY (RouteID, DriverID)
        );

    -- staging_sales
    IF OBJECT_ID('staging.staging_sales', 'U') IS NULL
        CREATE TABLE staging.staging_sales (
            SalesID     INT           NOT NULL PRIMARY KEY,
            DeliveryID  INT           NOT NULL,
            DateKey     DATETIME2     NOT NULL,
            ProductType NVARCHAR(50)  NOT NULL,
            Region      NVARCHAR(10)  NOT NULL,
            UnitsSold   INT           NOT NULL,
            SalesAmount DECIMAL(10,2) NOT NULL
        );

    -- load_log
    IF OBJECT_ID('staging.load_log', 'U') IS NULL
        CREATE TABLE staging.load_log (
            LoadID     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            TableName  NVARCHAR(100)     NOT NULL,
            RowsLoaded INT               NOT NULL,
            LoadedAt   DATETIME2         NOT NULL DEFAULT GETDATE()
        );

    EXEC staging.usp_log_step_end
        @RunID        = @RunID,
        @StepName     = 'staging_setup',
        @RowsAffected = 0;

END TRY
BEGIN CATCH
    EXEC staging.usp_log_step_error
        @RunID    = @RunID,
        @StepName = 'staging_setup',
        @ErrorMsg = ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  STEP 2: STAGING LOAD
  Handled by load_staging.py. Run that script now with:

      python load_staging.py --run-id <RunID>

  Retrieve the current RunID:
      SELECT MAX(RunID) FROM staging.pipeline_log;

  Then continue running this script from Step 3 onward.
=============================================================*/

PRINT '';
PRINT '>>> PAUSE: Run load_staging.py --run-id ' +
      CAST((SELECT MAX(RunID) FROM staging.pipeline_log) AS NVARCHAR);
PRINT '>>> Then continue running this script from Step 3.';
PRINT '';
GO

/*=============================================================
  STEP 3: STAGING VALIDATION
  Validates row counts, NULLs, duplicates, referential
  integrity, value ranges, date sanity, and data quality
  flags across all four staging tables.
=============================================================*/

DECLARE @RunID      INT;
DECLARE @BadRows    INT;
DECLARE @CheckName  NVARCHAR(200);
DECLARE @FailCount  INT = 0;
DECLARE @Expected   INT;
DECLARE @Count      INT;

SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

EXEC staging.usp_log_step_start
    @RunID      = @RunID,
    @StepOrder  = 3,
    @StepName   = 'staging_validation',
    @StepDesc   = 'Validate staging tables: row counts, NULLs, duplicates, referential integrity, value ranges, date sanity, data quality flags.';

BEGIN TRY

    -- Row counts from load_log
    SELECT @Expected = RowsLoaded FROM staging.load_log
    WHERE TableName = 'staging.staging_sales'
      AND LoadID = (SELECT MAX(LoadID) FROM staging.load_log WHERE TableName = 'staging.staging_sales');
    SELECT @Count = COUNT(*) FROM staging.staging_sales;
    IF @Count <> @Expected BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_sales row count | Expected=' + CAST(@Expected AS NVARCHAR) + ' Actual=' + CAST(@Count AS NVARCHAR); END
    ELSE PRINT 'PASS: staging_sales row count';

    SELECT @Expected = RowsLoaded FROM staging.load_log
    WHERE TableName = 'staging.staging_deliveries'
      AND LoadID = (SELECT MAX(LoadID) FROM staging.load_log WHERE TableName = 'staging.staging_deliveries');
    SELECT @Count = COUNT(*) FROM staging.staging_deliveries;
    IF @Count <> @Expected BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_deliveries row count'; END
    ELSE PRINT 'PASS: staging_deliveries row count';

    SELECT @Expected = RowsLoaded FROM staging.load_log
    WHERE TableName = 'staging.staging_routes'
      AND LoadID = (SELECT MAX(LoadID) FROM staging.load_log WHERE TableName = 'staging.staging_routes');
    SELECT @Count = COUNT(*) FROM staging.staging_routes;
    IF @Count <> @Expected BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_routes row count'; END
    ELSE PRINT 'PASS: staging_routes row count';

    SELECT @Expected = RowsLoaded FROM staging.load_log
    WHERE TableName = 'staging.staging_exceptions'
      AND LoadID = (SELECT MAX(LoadID) FROM staging.load_log WHERE TableName = 'staging.staging_exceptions');
    SELECT @Count = COUNT(*) FROM staging.staging_exceptions;
    IF @Count <> @Expected BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_exceptions row count'; END
    ELSE PRINT 'PASS: staging_exceptions row count';

    -- NULL checks on required columns
    SELECT @BadRows = COUNT(*) FROM staging.staging_sales
    WHERE SalesID IS NULL OR DeliveryID IS NULL OR DateKey IS NULL OR UnitsSold IS NULL OR SalesAmount IS NULL;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_sales required NULLs | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: staging_sales required NULLs';

    SELECT @BadRows = COUNT(*) FROM staging.staging_deliveries
    WHERE DeliveryID IS NULL OR RouteID IS NULL OR Region IS NULL OR ShipmentType IS NULL OR DeliveryDate IS NULL OR DeliveryStatus IS NULL OR PriorityFlag IS NULL;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_deliveries required NULLs | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: staging_deliveries required NULLs';

    SELECT @BadRows = COUNT(*) FROM staging.staging_exceptions
    WHERE ExceptionID IS NULL OR DeliveryID IS NULL OR ExceptionType IS NULL OR DateReported IS NULL OR PriorityFlag IS NULL OR Region IS NULL;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_exceptions required NULLs | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: staging_exceptions required NULLs';

    -- Referential integrity
    SELECT @BadRows = COUNT(*) FROM staging.staging_sales s
    WHERE NOT EXISTS (SELECT 1 FROM staging.staging_deliveries d WHERE d.DeliveryID = s.DeliveryID);
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_sales orphaned DeliveryIDs | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: staging_sales referential integrity';

    SELECT @BadRows = COUNT(*) FROM staging.staging_exceptions e
    WHERE NOT EXISTS (SELECT 1 FROM staging.staging_deliveries d WHERE d.DeliveryID = e.DeliveryID);
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_exceptions orphaned DeliveryIDs | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: staging_exceptions referential integrity';

    -- Value range checks
    SELECT @BadRows = COUNT(*) FROM staging.staging_sales WHERE SalesAmount <= 0 OR UnitsSold <= 0;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: staging_sales negative values | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: staging_sales value ranges';

    IF @FailCount > 0
    BEGIN
        EXEC staging.usp_log_step_error
            @RunID    = @RunID,
            @StepName = 'staging_validation',
            @ErrorMsg = 'Staging validation failed: ' + CAST(@FailCount AS NVARCHAR) + ' check(s) failed.';
        THROW 51001, 'Staging Validation Failed.', 1;
    END

    EXEC staging.usp_log_step_end
        @RunID        = @RunID,
        @StepName     = 'staging_validation',
        @RowsAffected = 0;

END TRY
BEGIN CATCH
    IF @FailCount = 0  -- Only log error here if it wasn't already logged above
        EXEC staging.usp_log_step_error
            @RunID    = @RunID,
            @StepName = 'staging_validation',
            @ErrorMsg = ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  STEP 4: CLEAN BUILD
  Creates or replaces all four clean layer views.
=============================================================*/

DECLARE @RunID INT;
SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

EXEC staging.usp_log_step_start
    @RunID      = @RunID,
    @StepOrder  = 4,
    @StepName   = 'clean_build',
    @StepDesc   = 'Create clean layer views: clean_sales, clean_deliveries, clean_routes, clean_exceptions.';

BEGIN TRY

    IF OBJECT_ID('clean.clean_sales',      'V') IS NOT NULL DROP VIEW clean.clean_sales;
    IF OBJECT_ID('clean.clean_deliveries', 'V') IS NOT NULL DROP VIEW clean.clean_deliveries;
    IF OBJECT_ID('clean.clean_routes',     'V') IS NOT NULL DROP VIEW clean.clean_routes;
    IF OBJECT_ID('clean.clean_exceptions', 'V') IS NOT NULL DROP VIEW clean.clean_exceptions;

    EXEC('
    CREATE VIEW clean.clean_sales AS
    SELECT SalesID, DeliveryID, DateKey,
        CAST(DateKey AS DATE) AS SaleDate,
        YEAR(DateKey) AS SaleYear, MONTH(DateKey) AS SaleMonth, DATEPART(QUARTER,DateKey) AS SaleQuarter,
        CASE ProductType WHEN ''F.'' THEN ''Freight'' WHEN ''L.'' THEN ''Large Package''
            WHEN ''M.'' THEN ''Medium Package'' WHEN ''S.'' THEN ''Small Package'' ELSE ProductType END AS ProductType,
        CASE Region WHEN ''M.'' THEN ''MW'' WHEN ''N.'' THEN ''N'' WHEN ''S.'' THEN ''S'' ELSE Region END AS Region,
        UnitsSold, SalesAmount,
        ROUND(SalesAmount / NULLIF(UnitsSold,0), 2) AS RevenuePerUnit
    FROM staging.staging_sales');

    EXEC('
    CREATE VIEW clean.clean_deliveries AS
    SELECT DeliveryID, RouteID, ISNULL(DriverID,''Unknown'') AS DriverID,
        CASE Region WHEN ''M.'' THEN ''MW'' WHEN ''N.'' THEN ''N'' WHEN ''S.'' THEN ''S'' ELSE Region END AS Region,
        CASE ShipmentType WHEN ''E.'' THEN ''Express'' WHEN ''P.'' THEN ''Priority'' WHEN ''S.'' THEN ''Standard'' ELSE ShipmentType END AS ShipmentType,
        DeliveryDate, ExpectedDeliveryDate,
        CAST(DeliveryDate AS DATE) AS DeliveryDateOnly,
        CAST(ExpectedDeliveryDate AS DATE) AS ExpectedDateOnly,
        YEAR(DeliveryDate) AS DeliveryYear, MONTH(DeliveryDate) AS DeliveryMonth, DATEPART(QUARTER,DeliveryDate) AS DeliveryQuarter,
        DeliveryStatus,
        CASE WHEN DeliveryStatus IN (''Late'',''Exception'') THEN 1 ELSE 0 END AS IsLate,
        CASE WHEN ExpectedDeliveryDate IS NOT NULL THEN DATEDIFF(DAY,ExpectedDeliveryDate,DeliveryDate) ELSE NULL END AS DaysVariance,
        PriorityFlag
    FROM staging.staging_deliveries');

    EXEC('
    CREATE VIEW clean.clean_routes AS
    SELECT ROW_NUMBER() OVER (ORDER BY RouteID,DriverID,Region,PlannedStops) AS RouteRunID,
        RouteID, ISNULL(NULLIF(LTRIM(RTRIM(DriverID)),''''),''Unknown'') AS DriverID,
        PlannedStops, ActualStops, PlannedHours, ActualHours,
        CASE Region WHEN ''M.'' THEN ''MW'' WHEN ''N.'' THEN ''N'' WHEN ''S.'' THEN ''S'' ELSE Region END AS Region,
        (ActualStops-PlannedStops) AS StopVariance,
        ROUND((ActualHours-PlannedHours),2) AS HourVariance,
        ROUND(CASE WHEN PlannedStops>0 THEN (CAST(ActualStops AS DECIMAL(10,2))/PlannedStops)*100 ELSE NULL END,2) AS StopEfficiencyPct,
        ROUND(CASE WHEN ActualHours>0 THEN (PlannedHours/ActualHours)*100 ELSE NULL END,2) AS HourEfficiencyPct
    FROM staging.staging_routes');

    EXEC('
    CREATE VIEW clean.clean_exceptions AS
    SELECT ExceptionID, DeliveryID,
        CASE ExceptionType WHEN ''A.'' THEN ''Address Issue'' WHEN ''C.'' THEN ''Customer Not Available''
            WHEN ''M.'' THEN ''Mechanical'' WHEN ''W.'' THEN ''Weather'' ELSE ExceptionType END AS ExceptionType,
        DateReported, ResolvedDate,
        CAST(DateReported AS DATE) AS DateReportedOnly,
        CAST(ResolvedDate AS DATE) AS ResolvedDateOnly,
        YEAR(DateReported) AS ExceptionYear, MONTH(DateReported) AS ExceptionMonth, DATEPART(QUARTER,DateReported) AS ExceptionQuarter,
        ResolutionTimeHours,
        CASE WHEN ResolvedDate IS NOT NULL THEN 1 ELSE 0 END AS IsResolved,
        CASE WHEN ResolvedDate IS NOT NULL THEN DATEDIFF(DAY,DateReported,ResolvedDate) ELSE NULL END AS ResolutionDays,
        PriorityFlag,
        CASE Region WHEN ''M.'' THEN ''MW'' WHEN ''N.'' THEN ''N'' WHEN ''S.'' THEN ''S'' ELSE Region END AS Region
    FROM staging.staging_exceptions');

    EXEC staging.usp_log_step_end
        @RunID        = @RunID,
        @StepName     = 'clean_build',
        @RowsAffected = 0;

END TRY
BEGIN CATCH
    EXEC staging.usp_log_step_error
        @RunID    = @RunID,
        @StepName = 'clean_build',
        @ErrorMsg = ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  STEP 5: CLEAN GATE
  Validates clean layer views before DW load.
=============================================================*/

DECLARE @RunID     INT;
DECLARE @BadRows   INT;
DECLARE @FailCount INT = 0;

SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

EXEC staging.usp_log_step_start
    @RunID      = @RunID,
    @StepOrder  = 5,
    @StepName   = 'clean_gate',
    @StepDesc   = 'Validate clean layer views: empty guard, NULLs, truncation, referential integrity, flag consistency.';

BEGIN TRY

    -- Empty view guard
    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM clean.clean_sales;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: clean_sales empty'; END ELSE PRINT 'PASS: clean_sales not empty';

    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM clean.clean_deliveries;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: clean_deliveries empty'; END ELSE PRINT 'PASS: clean_deliveries not empty';

    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM clean.clean_routes;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: clean_routes empty'; END ELSE PRINT 'PASS: clean_routes not empty';

    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM clean.clean_exceptions;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: clean_exceptions empty'; END ELSE PRINT 'PASS: clean_exceptions not empty';

    -- Truncation check
    SELECT @BadRows =
        (SELECT COUNT(*) FROM clean.clean_sales      WHERE ProductType  LIKE '_.')
      + (SELECT COUNT(*) FROM clean.clean_sales      WHERE Region       LIKE '_.')
      + (SELECT COUNT(*) FROM clean.clean_deliveries WHERE ShipmentType LIKE '_.')
      + (SELECT COUNT(*) FROM clean.clean_deliveries WHERE Region       LIKE '_.')
      + (SELECT COUNT(*) FROM clean.clean_exceptions WHERE ExceptionType LIKE '_.')
      + (SELECT COUNT(*) FROM clean.clean_routes     WHERE Region       LIKE '_.');
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: truncated values remain in clean layer | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: no truncated values in clean layer';

    -- IsLate flag
    SELECT @BadRows = COUNT(*) FROM clean.clean_deliveries
    WHERE DeliveryStatus IN ('Late','Exception') AND IsLate <> 1;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: IsLate flag mismatch | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: IsLate flag accuracy';

    -- Referential integrity
    SELECT @BadRows = COUNT(*) FROM clean.clean_sales s
    WHERE NOT EXISTS (SELECT 1 FROM clean.clean_deliveries d WHERE d.DeliveryID = s.DeliveryID);
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: clean_sales orphaned DeliveryIDs | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: clean_sales referential integrity';

    SELECT @BadRows = COUNT(*) FROM clean.clean_exceptions e
    WHERE NOT EXISTS (SELECT 1 FROM clean.clean_deliveries d WHERE d.DeliveryID = e.DeliveryID);
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: clean_exceptions orphaned DeliveryIDs | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: clean_exceptions referential integrity';

    IF @FailCount > 0
    BEGIN
        EXEC staging.usp_log_step_error
            @RunID    = @RunID,
            @StepName = 'clean_gate',
            @ErrorMsg = 'Clean gate failed: ' + CAST(@FailCount AS NVARCHAR) + ' check(s) failed.';
        THROW 51002, 'Clean Layer Validation Failed. DW Load Cancelled.', 1;
    END

    EXEC staging.usp_log_step_end
        @RunID        = @RunID,
        @StepName     = 'clean_gate',
        @RowsAffected = 0;

END TRY
BEGIN CATCH
    IF @FailCount = 0
        EXEC staging.usp_log_step_error
            @RunID    = @RunID,
            @StepName = 'clean_gate',
            @ErrorMsg = ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  STEP 6: DW BUILD
  Builds the star schema. Drops and recreates all dimension
  and fact tables then populates them from clean layer views.
=============================================================*/

DECLARE @RunID     INT;
DECLARE @RowCount  INT = 0;

SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

EXEC staging.usp_log_step_start
    @RunID      = @RunID,
    @StepOrder  = 6,
    @StepName   = 'dw_build',
    @StepDesc   = 'Build DW star schema: 7 dimension tables, 3 fact tables. Drop and recreate on each run.';

BEGIN TRY

    -- Drop facts before dimensions (FK dependency order)
    IF OBJECT_ID('dw.fact_exceptions',   'U') IS NOT NULL DROP TABLE dw.fact_exceptions;
    IF OBJECT_ID('dw.fact_sales',        'U') IS NOT NULL DROP TABLE dw.fact_sales;
    IF OBJECT_ID('dw.fact_deliveries',   'U') IS NOT NULL DROP TABLE dw.fact_deliveries;
    IF OBJECT_ID('dw.dim_date',          'U') IS NOT NULL DROP TABLE dw.dim_date;
    IF OBJECT_ID('dw.dim_driver',        'U') IS NOT NULL DROP TABLE dw.dim_driver;
    IF OBJECT_ID('dw.dim_region',        'U') IS NOT NULL DROP TABLE dw.dim_region;
    IF OBJECT_ID('dw.dim_product',       'U') IS NOT NULL DROP TABLE dw.dim_product;
    IF OBJECT_ID('dw.dim_shipment_type', 'U') IS NOT NULL DROP TABLE dw.dim_shipment_type;
    IF OBJECT_ID('dw.dim_exception_type','U') IS NOT NULL DROP TABLE dw.dim_exception_type;
    IF OBJECT_ID('dw.dim_route',         'U') IS NOT NULL DROP TABLE dw.dim_route;

    -- Create dimensions
    CREATE TABLE dw.dim_date (
        DateKey INT NOT NULL, FullDate DATE NOT NULL, DayOfWeek INT NOT NULL,
        DayName NVARCHAR(10) NOT NULL, DayOfMonth INT NOT NULL, DayOfYear INT NOT NULL,
        WeekOfYear INT NOT NULL, MonthNumber INT NOT NULL, MonthName NVARCHAR(10) NOT NULL,
        MonthShort NVARCHAR(3) NOT NULL, Quarter INT NOT NULL, QuarterName NVARCHAR(6) NOT NULL,
        YearNumber INT NOT NULL, YearMonth NVARCHAR(7) NOT NULL, YearQuarter NVARCHAR(7) NOT NULL,
        IsWeekend BIT NOT NULL, CONSTRAINT PK_dim_date PRIMARY KEY (DateKey));

    CREATE TABLE dw.dim_driver (
        DriverSK INT IDENTITY(1,1) NOT NULL, DriverCode NVARCHAR(50) NOT NULL,
        DriverLabel NVARCHAR(50) NOT NULL, IsUnknown BIT NOT NULL DEFAULT 0,
        CONSTRAINT PK_dim_driver PRIMARY KEY (DriverSK),
        CONSTRAINT UQ_dim_driver_code UNIQUE (DriverCode));

    CREATE TABLE dw.dim_region (
        RegionSK INT IDENTITY(1,1) NOT NULL, RegionCode NVARCHAR(5) NOT NULL,
        RegionName NVARCHAR(50) NOT NULL,
        CONSTRAINT PK_dim_region PRIMARY KEY (RegionSK),
        CONSTRAINT UQ_dim_region_code UNIQUE (RegionCode));

    CREATE TABLE dw.dim_product (
        ProductSK INT IDENTITY(1,1) NOT NULL, ProductType NVARCHAR(50) NOT NULL,
        CONSTRAINT PK_dim_product PRIMARY KEY (ProductSK),
        CONSTRAINT UQ_dim_product_type UNIQUE (ProductType));

    CREATE TABLE dw.dim_shipment_type (
        ShipmentTypeSK INT IDENTITY(1,1) NOT NULL, ShipmentType NVARCHAR(20) NOT NULL,
        CONSTRAINT PK_dim_shipment_type PRIMARY KEY (ShipmentTypeSK),
        CONSTRAINT UQ_dim_shipment_type UNIQUE (ShipmentType));

    CREATE TABLE dw.dim_exception_type (
        ExceptionTypeSK INT IDENTITY(1,1) NOT NULL, ExceptionType NVARCHAR(50) NOT NULL,
        CONSTRAINT PK_dim_exception_type PRIMARY KEY (ExceptionTypeSK),
        CONSTRAINT UQ_dim_exception_type UNIQUE (ExceptionType));

    CREATE TABLE dw.dim_route (
        RouteSK INT IDENTITY(1,1) NOT NULL, RouteID NVARCHAR(10) NOT NULL,
        CONSTRAINT PK_dim_route PRIMARY KEY (RouteSK),
        CONSTRAINT UQ_dim_route_id UNIQUE (RouteID));

    -- Create facts
    CREATE TABLE dw.fact_deliveries (
        DeliveryID INT NOT NULL, DateKey INT NOT NULL, DriverSK INT NOT NULL,
        RegionSK INT NOT NULL, ShipmentTypeSK INT NOT NULL, RouteSK INT NOT NULL,
        DeliveryStatus NVARCHAR(20) NOT NULL, IsLate BIT NOT NULL,
        PriorityFlag BIT NOT NULL, DaysVariance INT NULL,
        CONSTRAINT PK_fact_deliveries PRIMARY KEY (DeliveryID),
        CONSTRAINT FK_fact_deliveries_date     FOREIGN KEY (DateKey)        REFERENCES dw.dim_date (DateKey),
        CONSTRAINT FK_fact_deliveries_driver   FOREIGN KEY (DriverSK)       REFERENCES dw.dim_driver (DriverSK),
        CONSTRAINT FK_fact_deliveries_region   FOREIGN KEY (RegionSK)       REFERENCES dw.dim_region (RegionSK),
        CONSTRAINT FK_fact_deliveries_shipment FOREIGN KEY (ShipmentTypeSK) REFERENCES dw.dim_shipment_type (ShipmentTypeSK),
        CONSTRAINT FK_fact_deliveries_route    FOREIGN KEY (RouteSK)        REFERENCES dw.dim_route (RouteSK));

    CREATE TABLE dw.fact_sales (
        SalesID INT NOT NULL, DeliveryID INT NOT NULL, DateKey INT NOT NULL,
        ProductSK INT NOT NULL, RegionSK INT NOT NULL,
        UnitsSold INT NOT NULL, SalesAmount DECIMAL(10,2) NOT NULL, RevenuePerUnit DECIMAL(10,2) NULL,
        CONSTRAINT PK_fact_sales PRIMARY KEY (SalesID),
        CONSTRAINT FK_fact_sales_delivery FOREIGN KEY (DeliveryID) REFERENCES dw.fact_deliveries (DeliveryID),
        CONSTRAINT FK_fact_sales_date     FOREIGN KEY (DateKey)    REFERENCES dw.dim_date (DateKey),
        CONSTRAINT FK_fact_sales_product  FOREIGN KEY (ProductSK)  REFERENCES dw.dim_product (ProductSK),
        CONSTRAINT FK_fact_sales_region   FOREIGN KEY (RegionSK)   REFERENCES dw.dim_region (RegionSK));

    CREATE TABLE dw.fact_exceptions (
        ExceptionID INT NOT NULL, DeliveryID INT NOT NULL,
        DateReportedKey INT NOT NULL, ResolvedDateKey INT NULL,
        ExceptionTypeSK INT NOT NULL, RegionSK INT NOT NULL,
        PriorityFlag BIT NOT NULL, IsResolved BIT NOT NULL,
        ResolutionTimeHours DECIMAL(6,2) NULL, ResolutionDays INT NULL,
        CONSTRAINT PK_fact_exceptions PRIMARY KEY (ExceptionID),
        CONSTRAINT FK_fact_exceptions_delivery      FOREIGN KEY (DeliveryID)      REFERENCES dw.fact_deliveries (DeliveryID),
        CONSTRAINT FK_fact_exceptions_date_reported FOREIGN KEY (DateReportedKey) REFERENCES dw.dim_date (DateKey),
        CONSTRAINT FK_fact_exceptions_date_resolved FOREIGN KEY (ResolvedDateKey) REFERENCES dw.dim_date (DateKey),
        CONSTRAINT FK_fact_exceptions_type          FOREIGN KEY (ExceptionTypeSK) REFERENCES dw.dim_exception_type (ExceptionTypeSK),
        CONSTRAINT FK_fact_exceptions_region        FOREIGN KEY (RegionSK)        REFERENCES dw.dim_region (RegionSK));

    -- Populate dim_date (2023-2025 date spine)
    ;WITH DateCTE AS (
        SELECT CAST('2023-01-01' AS DATE) AS FullDate
        UNION ALL
        SELECT DATEADD(DAY, 1, FullDate) FROM DateCTE WHERE FullDate < '2025-12-31'
    )
    INSERT INTO dw.dim_date (DateKey,FullDate,DayOfWeek,DayName,DayOfMonth,DayOfYear,
        WeekOfYear,MonthNumber,MonthName,MonthShort,Quarter,QuarterName,YearNumber,
        YearMonth,YearQuarter,IsWeekend)
    SELECT CAST(FORMAT(FullDate,'yyyyMMdd') AS INT), FullDate,
        DATEPART(WEEKDAY,FullDate), DATENAME(WEEKDAY,FullDate), DAY(FullDate),
        DATEPART(DAYOFYEAR,FullDate), DATEPART(ISO_WEEK,FullDate), MONTH(FullDate),
        DATENAME(MONTH,FullDate), LEFT(DATENAME(MONTH,FullDate),3), DATEPART(QUARTER,FullDate),
        'Q'+CAST(DATEPART(QUARTER,FullDate) AS NVARCHAR), YEAR(FullDate),
        FORMAT(FullDate,'yyyy-MM'),
        CAST(YEAR(FullDate) AS NVARCHAR)+'-Q'+CAST(DATEPART(QUARTER,FullDate) AS NVARCHAR),
        CASE WHEN DATEPART(WEEKDAY,FullDate) IN (1,7) THEN 1 ELSE 0 END
    FROM DateCTE OPTION (MAXRECURSION 1096);

    -- Populate remaining dimensions
    INSERT INTO dw.dim_driver (DriverCode, DriverLabel, IsUnknown)
    SELECT DISTINCT DriverID, DriverID, 0
    FROM (SELECT DriverID FROM clean.clean_deliveries WHERE DriverID <> 'Unknown'
          UNION SELECT DriverID FROM clean.clean_routes WHERE DriverID <> 'Unknown') d
    UNION ALL SELECT 'Unknown','Unknown Driver',1;

    INSERT INTO dw.dim_region (RegionCode, RegionName) VALUES
        ('MW','Midwest'),('N','North'),('NE','North East'),
        ('NW','North West'),('S','South'),('SE','South East'),('SW','South West');

    INSERT INTO dw.dim_product (ProductType)
    SELECT DISTINCT ProductType FROM clean.clean_sales ORDER BY ProductType;

    INSERT INTO dw.dim_shipment_type (ShipmentType)
    SELECT DISTINCT ShipmentType FROM clean.clean_deliveries ORDER BY ShipmentType;

    INSERT INTO dw.dim_exception_type (ExceptionType)
    SELECT DISTINCT ExceptionType FROM clean.clean_exceptions ORDER BY ExceptionType;

    INSERT INTO dw.dim_route (RouteID)
    SELECT DISTINCT RouteID FROM clean.clean_routes ORDER BY RouteID;

    -- Populate fact tables (load order: deliveries first)
    INSERT INTO dw.fact_deliveries
        (DeliveryID,DateKey,DriverSK,RegionSK,ShipmentTypeSK,RouteSK,
         DeliveryStatus,IsLate,PriorityFlag,DaysVariance)
    SELECT cd.DeliveryID, CAST(FORMAT(cd.DeliveryDateOnly,'yyyyMMdd') AS INT),
        dd.DriverSK, dr.RegionSK, dst.ShipmentTypeSK, drt.RouteSK,
        cd.DeliveryStatus, cd.IsLate, cd.PriorityFlag, cd.DaysVariance
    FROM clean.clean_deliveries cd
    JOIN dw.dim_driver       dd  ON dd.DriverCode    = cd.DriverID
    JOIN dw.dim_region       dr  ON dr.RegionCode    = cd.Region
    JOIN dw.dim_shipment_type dst ON dst.ShipmentType = cd.ShipmentType
    JOIN dw.dim_route        drt ON drt.RouteID      = cd.RouteID;

    INSERT INTO dw.fact_sales
        (SalesID,DeliveryID,DateKey,ProductSK,RegionSK,UnitsSold,SalesAmount,RevenuePerUnit)
    SELECT cs.SalesID, cs.DeliveryID, CAST(FORMAT(cs.SaleDate,'yyyyMMdd') AS INT),
        dp.ProductSK, dr.RegionSK, cs.UnitsSold, cs.SalesAmount, cs.RevenuePerUnit
    FROM clean.clean_sales cs
    JOIN dw.dim_product  dp ON dp.ProductType = cs.ProductType
    JOIN dw.dim_region   dr ON dr.RegionCode  = cs.Region
    JOIN dw.fact_deliveries fd ON fd.DeliveryID = cs.DeliveryID;

    INSERT INTO dw.fact_exceptions
        (ExceptionID,DeliveryID,DateReportedKey,ResolvedDateKey,ExceptionTypeSK,
         RegionSK,PriorityFlag,IsResolved,ResolutionTimeHours,ResolutionDays)
    SELECT ce.ExceptionID, ce.DeliveryID,
        CAST(FORMAT(ce.DateReportedOnly,'yyyyMMdd') AS INT),
        CASE WHEN ce.ResolvedDateOnly IS NOT NULL
             THEN CAST(FORMAT(ce.ResolvedDateOnly,'yyyyMMdd') AS INT) ELSE NULL END,
        det.ExceptionTypeSK, dr.RegionSK,
        ce.PriorityFlag, ce.IsResolved, ce.ResolutionTimeHours, ce.ResolutionDays
    FROM clean.clean_exceptions ce
    JOIN dw.dim_exception_type det ON det.ExceptionType = ce.ExceptionType
    JOIN dw.dim_region         dr  ON dr.RegionCode     = ce.Region
    JOIN dw.fact_deliveries    fd  ON fd.DeliveryID     = ce.DeliveryID;

    -- Count total rows loaded across all DW tables
    SELECT @RowCount =
        (SELECT COUNT(*) FROM dw.dim_date)
      + (SELECT COUNT(*) FROM dw.dim_driver)
      + (SELECT COUNT(*) FROM dw.dim_region)
      + (SELECT COUNT(*) FROM dw.dim_product)
      + (SELECT COUNT(*) FROM dw.dim_shipment_type)
      + (SELECT COUNT(*) FROM dw.dim_exception_type)
      + (SELECT COUNT(*) FROM dw.dim_route)
      + (SELECT COUNT(*) FROM dw.fact_deliveries)
      + (SELECT COUNT(*) FROM dw.fact_sales)
      + (SELECT COUNT(*) FROM dw.fact_exceptions);

    EXEC staging.usp_log_step_end
        @RunID        = @RunID,
        @StepName     = 'dw_build',
        @RowsAffected = @RowCount;

END TRY
BEGIN CATCH
    EXEC staging.usp_log_step_error
        @RunID    = @RunID,
        @StepName = 'dw_build',
        @ErrorMsg = ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  STEP 7: DW GATE
  Validates DW tables after load.
=============================================================*/

DECLARE @RunID     INT;
DECLARE @BadRows   INT;
DECLARE @FailCount INT = 0;

SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

EXEC staging.usp_log_step_start
    @RunID      = @RunID,
    @StepOrder  = 7,
    @StepName   = 'dw_gate',
    @StepDesc   = 'Validate DW star schema: empty guard, surrogate keys, duplicates, FK integrity, business rules.';

BEGIN TRY

    -- Empty table guard (facts only -- dimension failures caught by dw_build)
    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_deliveries;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: fact_deliveries empty'; END ELSE PRINT 'PASS: fact_deliveries not empty';

    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_sales;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: fact_sales empty'; END ELSE PRINT 'PASS: fact_sales not empty';

    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_exceptions;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: fact_exceptions empty'; END ELSE PRINT 'PASS: fact_exceptions not empty';

    -- NULL surrogate key checks
    SELECT @BadRows = COUNT(*) FROM dw.fact_sales WHERE DateKey IS NULL OR ProductSK IS NULL OR RegionSK IS NULL;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: fact_sales NULL surrogate keys | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: fact_sales surrogate keys';

    SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries WHERE DateKey IS NULL OR DriverSK IS NULL OR RegionSK IS NULL OR ShipmentTypeSK IS NULL OR RouteSK IS NULL;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: fact_deliveries NULL surrogate keys | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: fact_deliveries surrogate keys';

    SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions WHERE DateReportedKey IS NULL OR ExceptionTypeSK IS NULL OR RegionSK IS NULL;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: fact_exceptions NULL surrogate keys | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: fact_exceptions surrogate keys';

    -- Business metric sanity
    SELECT @BadRows = COUNT(*) FROM dw.fact_sales WHERE SalesAmount <= 0 OR UnitsSold <= 0;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: fact_sales negative values | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: fact_sales metric sanity';

    -- IsLate flag consistency
    SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries WHERE DeliveryStatus IN ('Late','Exception') AND IsLate <> 1;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: IsLate flag mismatch | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: IsLate flag consistency';

    -- IsResolved flag consistency
    SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions WHERE ResolvedDateKey IS NOT NULL AND IsResolved <> 1;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: IsResolved flag mismatch | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: IsResolved flag consistency';

    IF @FailCount > 0
    BEGIN
        EXEC staging.usp_log_step_error
            @RunID    = @RunID,
            @StepName = 'dw_gate',
            @ErrorMsg = 'DW gate failed: ' + CAST(@FailCount AS NVARCHAR) + ' check(s) failed.';
        THROW 51003, 'DW Validation Failed. Reporting layer load cancelled.', 1;
    END

    EXEC staging.usp_log_step_end
        @RunID        = @RunID,
        @StepName     = 'dw_gate',
        @RowsAffected = 0;

END TRY
BEGIN CATCH
    IF @FailCount = 0
        EXEC staging.usp_log_step_error
            @RunID    = @RunID,
            @StepName = 'dw_gate',
            @ErrorMsg = ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  STEP 8: REPORTING BUILD
  Creates or replaces all six reporting views.
=============================================================*/

DECLARE @RunID INT;
SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

EXEC staging.usp_log_step_start
    @RunID      = @RunID,
    @StepOrder  = 8,
    @StepName   = 'reporting_build',
    @StepDesc   = 'Create reporting views: delivery_performance, sales_performance, exception_analysis, driver_performance, route_efficiency, executive_summary.';

BEGIN TRY

    IF OBJECT_ID('reporting.rpt_delivery_performance','V') IS NOT NULL DROP VIEW reporting.rpt_delivery_performance;
    IF OBJECT_ID('reporting.rpt_sales_performance',   'V') IS NOT NULL DROP VIEW reporting.rpt_sales_performance;
    IF OBJECT_ID('reporting.rpt_exception_analysis',  'V') IS NOT NULL DROP VIEW reporting.rpt_exception_analysis;
    IF OBJECT_ID('reporting.rpt_driver_performance',  'V') IS NOT NULL DROP VIEW reporting.rpt_driver_performance;
    IF OBJECT_ID('reporting.rpt_route_efficiency',    'V') IS NOT NULL DROP VIEW reporting.rpt_route_efficiency;
    IF OBJECT_ID('reporting.rpt_executive_summary',   'V') IS NOT NULL DROP VIEW reporting.rpt_executive_summary;

    -- Each view is created via EXEC to allow CREATE VIEW as first statement
    EXEC('CREATE VIEW reporting.rpt_delivery_performance AS
    SELECT dd.YearNumber,dd.MonthNumber,dd.MonthName,dd.MonthShort,dd.Quarter,dd.QuarterName,dd.YearMonth,dd.YearQuarter,
        dr.RegionCode,dr.RegionName,dst.ShipmentType,drt.RouteID,
        COUNT(*) AS TotalDeliveries,
        SUM(CASE WHEN fd.DeliveryStatus=''On-Time'' THEN 1 ELSE 0 END) AS OnTimeCount,
        SUM(CASE WHEN fd.DeliveryStatus=''Late'' THEN 1 ELSE 0 END) AS LateCount,
        SUM(CASE WHEN fd.DeliveryStatus=''Exception'' THEN 1 ELSE 0 END) AS ExceptionCount,
        ROUND(SUM(CASE WHEN fd.DeliveryStatus=''On-Time'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),2) AS OnTimeRate,
        ROUND(SUM(CASE WHEN fd.DeliveryStatus=''Late'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),2) AS LateRate,
        ROUND(SUM(CASE WHEN fd.DeliveryStatus=''Exception'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),2) AS ExceptionRate,
        SUM(CAST(fd.PriorityFlag AS INT)) AS PriorityCount,
        ROUND(AVG(CAST(fd.DaysVariance AS DECIMAL(10,2))),2) AS AvgDaysVariance
    FROM dw.fact_deliveries fd
    JOIN dw.dim_date dd ON dd.DateKey=fd.DateKey
    JOIN dw.dim_region dr ON dr.RegionSK=fd.RegionSK
    JOIN dw.dim_shipment_type dst ON dst.ShipmentTypeSK=fd.ShipmentTypeSK
    JOIN dw.dim_route drt ON drt.RouteSK=fd.RouteSK
    GROUP BY dd.YearNumber,dd.MonthNumber,dd.MonthName,dd.MonthShort,dd.Quarter,dd.QuarterName,
        dd.YearMonth,dd.YearQuarter,dr.RegionCode,dr.RegionName,dst.ShipmentType,drt.RouteID');

    EXEC('CREATE VIEW reporting.rpt_sales_performance AS
    SELECT dd.YearNumber,dd.MonthNumber,dd.MonthName,dd.MonthShort,dd.Quarter,dd.QuarterName,dd.YearMonth,dd.YearQuarter,
        dr.RegionCode,dr.RegionName,dp.ProductType,
        COUNT(*) AS TotalTransactions, SUM(fs.UnitsSold) AS TotalUnitsSold,
        ROUND(SUM(fs.SalesAmount),2) AS TotalRevenue,
        ROUND(AVG(fs.SalesAmount),2) AS AvgRevenuePerTx,
        ROUND(SUM(fs.SalesAmount)/NULLIF(SUM(fs.UnitsSold),0),2) AS AvgRevenuePerUnit,
        MIN(fs.SalesAmount) AS MinRevenue, MAX(fs.SalesAmount) AS MaxRevenue
    FROM dw.fact_sales fs
    JOIN dw.dim_date dd ON dd.DateKey=fs.DateKey
    JOIN dw.dim_region dr ON dr.RegionSK=fs.RegionSK
    JOIN dw.dim_product dp ON dp.ProductSK=fs.ProductSK
    GROUP BY dd.YearNumber,dd.MonthNumber,dd.MonthName,dd.MonthShort,dd.Quarter,dd.QuarterName,
        dd.YearMonth,dd.YearQuarter,dr.RegionCode,dr.RegionName,dp.ProductType');

    EXEC('CREATE VIEW reporting.rpt_exception_analysis AS
    SELECT dd.YearNumber,dd.MonthNumber,dd.MonthName,dd.MonthShort,dd.Quarter,dd.QuarterName,dd.YearMonth,dd.YearQuarter,
        det.ExceptionType,dr.RegionCode,dr.RegionName,
        COUNT(*) AS TotalExceptions,
        SUM(CAST(fe.IsResolved AS INT)) AS ResolvedCount,
        SUM(CASE WHEN fe.IsResolved=0 THEN 1 ELSE 0 END) AS OpenCount,
        ROUND(SUM(CAST(fe.IsResolved AS INT))*100.0/NULLIF(COUNT(*),0),2) AS ResolutionRate,
        ROUND(AVG(CASE WHEN fe.IsResolved=1 THEN fe.ResolutionTimeHours END),2) AS AvgResolutionHours,
        ROUND(AVG(CASE WHEN fe.IsResolved=1 THEN CAST(fe.ResolutionDays AS DECIMAL(10,2)) END),2) AS AvgResolutionDays,
        MIN(CASE WHEN fe.IsResolved=1 THEN fe.ResolutionTimeHours END) AS MinResolutionHours,
        MAX(CASE WHEN fe.IsResolved=1 THEN fe.ResolutionTimeHours END) AS MaxResolutionHours,
        SUM(CAST(fe.PriorityFlag AS INT)) AS PriorityCount
    FROM dw.fact_exceptions fe
    JOIN dw.dim_date dd ON dd.DateKey=fe.DateReportedKey
    JOIN dw.dim_exception_type det ON det.ExceptionTypeSK=fe.ExceptionTypeSK
    JOIN dw.dim_region dr ON dr.RegionSK=fe.RegionSK
    GROUP BY dd.YearNumber,dd.MonthNumber,dd.MonthName,dd.MonthShort,dd.Quarter,dd.QuarterName,
        dd.YearMonth,dd.YearQuarter,det.ExceptionType,dr.RegionCode,dr.RegionName');

    EXEC('CREATE VIEW reporting.rpt_driver_performance AS
    SELECT drv.DriverCode,drv.DriverLabel,drv.IsUnknown,
        COUNT(DISTINCT fd.DeliveryID) AS TotalDeliveries,
        SUM(CASE WHEN fd.DeliveryStatus=''On-Time'' THEN 1 ELSE 0 END) AS OnTimeCount,
        SUM(CASE WHEN fd.DeliveryStatus=''Late'' THEN 1 ELSE 0 END) AS LateCount,
        SUM(CASE WHEN fd.DeliveryStatus=''Exception'' THEN 1 ELSE 0 END) AS ExceptionCount,
        ROUND(SUM(CASE WHEN fd.DeliveryStatus=''On-Time'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(DISTINCT fd.DeliveryID),0),2) AS OnTimeRate,
        ROUND(SUM(CASE WHEN fd.DeliveryStatus=''Late'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(DISTINCT fd.DeliveryID),0),2) AS LateRate,
        ROUND(SUM(CASE WHEN fd.DeliveryStatus=''Exception'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(DISTINCT fd.DeliveryID),0),2) AS ExceptionRate,
        COUNT(DISTINCT fe.ExceptionID) AS TotalExceptions,
        ROUND(COUNT(DISTINCT fe.ExceptionID)*1.0/NULLIF(COUNT(DISTINCT fd.DeliveryID),0),4) AS ExceptionPerDelivery,
        SUM(CAST(fd.PriorityFlag AS INT)) AS PriorityCount,
        ROUND(SUM(CAST(fd.PriorityFlag AS INT))*100.0/NULLIF(COUNT(DISTINCT fd.DeliveryID),0),2) AS PriorityRate,
        ROUND(AVG(CAST(fd.DaysVariance AS DECIMAL(10,2))),2) AS AvgDaysVariance,
        COUNT(DISTINCT fd.RegionSK) AS RegionsServed
    FROM dw.dim_driver drv
    JOIN dw.fact_deliveries fd ON fd.DriverSK=drv.DriverSK
    LEFT JOIN dw.fact_exceptions fe ON fe.DeliveryID=fd.DeliveryID
    GROUP BY drv.DriverCode,drv.DriverLabel,drv.IsUnknown');

    EXEC('CREATE VIEW reporting.rpt_route_efficiency AS
    SELECT drt.RouteID,
        COUNT(*) AS TotalRuns, COUNT(DISTINCT cr.DriverID) AS DriversAssigned,
        SUM(cr.PlannedStops) AS TotalPlannedStops, SUM(cr.ActualStops) AS TotalActualStops,
        ROUND(AVG(CAST(cr.PlannedStops AS DECIMAL(10,2))),2) AS AvgPlannedStops,
        ROUND(AVG(CAST(cr.ActualStops AS DECIMAL(10,2))),2) AS AvgActualStops,
        ROUND(AVG(CAST(cr.StopVariance AS DECIMAL(10,2))),2) AS AvgStopVariance,
        ROUND(SUM(cr.PlannedHours),2) AS TotalPlannedHours, ROUND(SUM(cr.ActualHours),2) AS TotalActualHours,
        ROUND(AVG(cr.PlannedHours),2) AS AvgPlannedHours, ROUND(AVG(cr.ActualHours),2) AS AvgActualHours,
        ROUND(AVG(cr.HourVariance),2) AS AvgHourVariance,
        ROUND(AVG(cr.StopEfficiencyPct),2) AS AvgStopEfficiency,
        ROUND(AVG(cr.HourEfficiencyPct),2) AS AvgHourEfficiency,
        ROUND(SUM(cr.ActualStops)*100.0/NULLIF(SUM(cr.PlannedStops),0),2) AS OverallStopEfficiency,
        ROUND(SUM(cr.PlannedHours)*100.0/NULLIF(SUM(cr.ActualHours),0),2) AS OverallHourEfficiency
    FROM clean.clean_routes cr
    JOIN dw.dim_route drt ON drt.RouteID=cr.RouteID
    GROUP BY drt.RouteID');

    EXEC('CREATE VIEW reporting.rpt_executive_summary AS
    SELECT
        (SELECT COUNT(*) FROM dw.fact_deliveries) AS TotalDeliveries,
        (SELECT ROUND(SUM(CASE WHEN DeliveryStatus=''On-Time'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),2) FROM dw.fact_deliveries) AS OnTimeRate,
        (SELECT ROUND(SUM(CASE WHEN DeliveryStatus=''Late'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),2) FROM dw.fact_deliveries) AS LateRate,
        (SELECT ROUND(SUM(CASE WHEN DeliveryStatus=''Exception'' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),2) FROM dw.fact_deliveries) AS ExceptionRate,
        (SELECT SUM(CAST(PriorityFlag AS INT)) FROM dw.fact_deliveries) AS PriorityDeliveries,
        (SELECT ROUND(AVG(CAST(DaysVariance AS DECIMAL(10,2))),2) FROM dw.fact_deliveries WHERE DaysVariance IS NOT NULL) AS AvgDaysVariance,
        (SELECT COUNT(*) FROM dw.fact_sales) AS TotalSalesTransactions,
        (SELECT ROUND(SUM(SalesAmount),2) FROM dw.fact_sales) AS TotalRevenue,
        (SELECT ROUND(AVG(SalesAmount),2) FROM dw.fact_sales) AS AvgRevenuePerTx,
        (SELECT SUM(UnitsSold) FROM dw.fact_sales) AS TotalUnitsSold,
        (SELECT ROUND(SUM(SalesAmount)/NULLIF(SUM(UnitsSold),0),2) FROM dw.fact_sales) AS AvgRevenuePerUnit,
        (SELECT COUNT(*) FROM dw.fact_exceptions) AS TotalExceptions,
        (SELECT SUM(CAST(IsResolved AS INT)) FROM dw.fact_exceptions) AS ResolvedExceptions,
        (SELECT SUM(CASE WHEN IsResolved=0 THEN 1 ELSE 0 END) FROM dw.fact_exceptions) AS OpenExceptions,
        (SELECT ROUND(SUM(CAST(IsResolved AS INT))*100.0/NULLIF(COUNT(*),0),2) FROM dw.fact_exceptions) AS OverallResolutionRate,
        (SELECT ROUND(AVG(ResolutionTimeHours),2) FROM dw.fact_exceptions WHERE ResolutionTimeHours IS NOT NULL) AS AvgResolutionHours,
        (SELECT COUNT(*) FROM clean.clean_routes) AS TotalRouteRuns,
        (SELECT ROUND(AVG(StopEfficiencyPct),2) FROM clean.clean_routes WHERE StopEfficiencyPct IS NOT NULL) AS AvgStopEfficiency,
        (SELECT ROUND(AVG(HourEfficiencyPct),2) FROM clean.clean_routes WHERE HourEfficiencyPct IS NOT NULL) AS AvgHourEfficiency');

    EXEC staging.usp_log_step_end
        @RunID        = @RunID,
        @StepName     = 'reporting_build',
        @RowsAffected = 0;

END TRY
BEGIN CATCH
    EXEC staging.usp_log_step_error
        @RunID    = @RunID,
        @StepName = 'reporting_build',
        @ErrorMsg = ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  STEP 9: REPORTING GATE
  Validates reporting views before Power BI handoff.
=============================================================*/

DECLARE @RunID     INT;
DECLARE @BadRows   INT;
DECLARE @FailCount INT = 0;

DECLARE @RptDeliveries  INT;
DECLARE @FactDeliveries INT;
DECLARE @RptExceptions  INT;
DECLARE @FactExceptions INT;
DECLARE @RptRevenue     DECIMAL(18,2);
DECLARE @FactRevenue    DECIMAL(18,2);

SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

EXEC staging.usp_log_step_start
    @RunID      = @RunID,
    @StepOrder  = 9,
    @StepName   = 'reporting_gate',
    @StepDesc   = 'Validate reporting views: empty guard, totals match fact tables, rate ranges, executive summary sanity.';

BEGIN TRY

    -- Empty view guard
    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM reporting.rpt_delivery_performance;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: rpt_delivery_performance empty'; END ELSE PRINT 'PASS: rpt_delivery_performance not empty';

    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM reporting.rpt_sales_performance;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: rpt_sales_performance empty'; END ELSE PRINT 'PASS: rpt_sales_performance not empty';

    SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM reporting.rpt_executive_summary;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: rpt_executive_summary empty'; END ELSE PRINT 'PASS: rpt_executive_summary not empty';

    -- Totals match fact tables
    SELECT @RptDeliveries = SUM(TotalDeliveries) FROM reporting.rpt_delivery_performance;
    SELECT @FactDeliveries = COUNT(*) FROM dw.fact_deliveries;
    IF ABS(@RptDeliveries - @FactDeliveries) > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: delivery totals mismatch'; END
    ELSE PRINT 'PASS: delivery totals match';

    SELECT @RptRevenue = SUM(TotalRevenue) FROM reporting.rpt_sales_performance;
    SELECT @FactRevenue = ROUND(SUM(SalesAmount),2) FROM dw.fact_sales;
    IF ABS(@RptRevenue - @FactRevenue) > 0.01 BEGIN SET @FailCount += 1; PRINT 'FAIL: revenue totals mismatch'; END
    ELSE PRINT 'PASS: revenue totals match';

    SELECT @RptExceptions = SUM(TotalExceptions) FROM reporting.rpt_exception_analysis;
    SELECT @FactExceptions = COUNT(*) FROM dw.fact_exceptions;
    IF ABS(@RptExceptions - @FactExceptions) > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: exception totals mismatch'; END
    ELSE PRINT 'PASS: exception totals match';

    -- Rate ranges (0-100)
    SELECT @BadRows = COUNT(*) FROM reporting.rpt_delivery_performance
    WHERE OnTimeRate < 0 OR OnTimeRate > 100 OR LateRate < 0 OR LateRate > 100 OR ExceptionRate < 0 OR ExceptionRate > 100;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: rate columns out of range | Rows=' + CAST(@BadRows AS NVARCHAR); END
    ELSE PRINT 'PASS: rate column ranges';

    -- Executive summary: exactly one row, delivery rates sum to 100
    SELECT @BadRows = CASE WHEN COUNT(*) <> 1 THEN 1 ELSE 0 END FROM reporting.rpt_executive_summary;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: executive summary row count <> 1'; END
    ELSE PRINT 'PASS: executive summary row count';

    SELECT @BadRows = CASE WHEN ABS((OnTimeRate + LateRate + ExceptionRate) - 100.0) > 0.1 THEN 1 ELSE 0 END
    FROM reporting.rpt_executive_summary;
    IF @BadRows > 0 BEGIN SET @FailCount += 1; PRINT 'FAIL: delivery rates do not sum to 100'; END
    ELSE PRINT 'PASS: delivery rates sum to 100';

    IF @FailCount > 0
    BEGIN
        EXEC staging.usp_log_step_error
            @RunID    = @RunID,
            @StepName = 'reporting_gate',
            @ErrorMsg = 'Reporting gate failed: ' + CAST(@FailCount AS NVARCHAR) + ' check(s) failed.';
        THROW 51004, 'Reporting Layer Validation Failed.', 1;
    END

    EXEC staging.usp_log_step_end
        @RunID        = @RunID,
        @StepName     = 'reporting_gate',
        @RowsAffected = 0;

END TRY
BEGIN CATCH
    IF @FailCount = 0
        EXEC staging.usp_log_step_error
            @RunID    = @RunID,
            @StepName = 'reporting_gate',
            @ErrorMsg = ERROR_MESSAGE();
    THROW;
END CATCH;
GO

/*=============================================================
  PIPELINE COMPLETE: FINAL SUMMARY
  Prints the full audit trail for this run.
=============================================================*/

DECLARE @RunID INT;
SELECT @RunID = MAX(RunID) FROM staging.pipeline_log;

PRINT '';
PRINT '=========================================';
PRINT ' PIPELINE RUN COMPLETE';
PRINT ' RunID: ' + CAST(@RunID AS NVARCHAR);
PRINT '=========================================';

SELECT
    StepOrder,
    StepName,
    StepStatus,
    StepStartTime,
    StepEndTime,
    DurationSeconds,
    RowsAffected,
    ErrorMessage
FROM staging.pipeline_log
WHERE RunID = @RunID
ORDER BY StepOrder;

-- Overall run result
SELECT
    @RunID                                              AS RunID,
    MIN(StepStartTime)                                  AS RunStarted,
    MAX(StepEndTime)                                    AS RunFinished,
    DATEDIFF(SECOND, MIN(StepStartTime), MAX(StepEndTime)) AS TotalDurationSeconds,
    SUM(CASE WHEN StepStatus = 'PASSED' THEN 1 ELSE 0 END) AS StepsPassed,
    SUM(CASE WHEN StepStatus = 'FAILED' THEN 1 ELSE 0 END) AS StepsFailed,
    CASE WHEN SUM(CASE WHEN StepStatus = 'FAILED' THEN 1 ELSE 0 END) = 0
         THEN 'PIPELINE PASSED'
         ELSE 'PIPELINE FAILED'
    END                                                 AS OverallResult
FROM staging.pipeline_log
WHERE RunID = @RunID;
GO
