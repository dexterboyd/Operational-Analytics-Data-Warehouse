/*==============================================================
  DW LOAD SCRIPT
  Database: Fedex_Ops_Database
  Version:  2.0

  Purpose:
      Drop, recreate, and reload all Data Warehouse dimension
      and fact tables from the clean layer views.

  Pipeline:
      staging -> clean -> [THIS SCRIPT] -> dw -> validation
               -> reporting -> BI

  Execution order within this script:
      1.  Drop facts first  (removes FK dependencies)
      2.  Drop dimensions
      3.  Create dimensions
      4.  Create facts       (with FK constraints + PKs)
      5.  Load dim_date
      6.  Load all other dimensions
      7.  Load fact tables using surrogate key lookups

  Change Log:
      v2.0 - Removed duplicate commented block.
           - Replaced STRING_AGG drop with explicit ordered
             drops: facts first, then dimensions. STRING_AGG
             does not guarantee order, causing FK constraint
             errors on every run.
           - Wrapped entire script in BEGIN TRANSACTION /
             TRY / CATCH / ROLLBACK so a mid-load failure
             leaves the DW in a consistent empty state rather
             than partially loaded.
           - Added PRIMARY KEY constraints to all fact tables.
             fact_sales: PK (SalesID)
             fact_deliveries: PK (DeliveryID)
             fact_exceptions: PK (ExceptionID)
             fact_routes: composite PK (RouteID, DriverID)
           - Fixed dim_product_type load: removed erroneous
             UNION from vw_deliveries which has no ProductType
             column.
           - Fixed dim_date load: removed vw_routes from the
             UNION (vw_routes has no DateKey column). Added
             explicit CONVERT(INT, CONVERT(VARCHAR(8),
             DateKey, 112)) so the DATE source column is
             correctly cast to the INT DateKey format stored
             in dim_date.
           - Fixed fact_sales DateKey join: source DateKey is
             DATE typed; now explicitly converted to INT before
             joining to dim_date.
           - Renamed dim_driver.DriverName -> DriverCode and
             dim_route.RouteName -> RouteCode to accurately
             reflect that these columns store source codes
             (NVARCHAR IDs), not human-readable names.
           - Added DeliveryDateKey and ExpectedDeliveryDateKey
             to fact_deliveries so both actual and expected
             delivery dates are available for late-delivery
             analysis in reporting.
           - Added EfficiencyRatio and StopVariance to
             fact_routes, carried through from vw_routes v2.0.
==============================================================*/

SET XACT_ABORT ON;
PRINT '--- DW LOAD START ---';

BEGIN TRANSACTION;
BEGIN TRY

/*==============================================================
  STEP 1: DROP EXISTING TABLES
  Facts must be dropped before dimensions to satisfy FK
  constraints. Each table is dropped individually with an
  existence check so the script is safe to run on a fresh
  schema as well as a populated one.
==============================================================*/
PRINT 'STEP 1: Dropping existing DW tables...';

-- Drop facts first (they hold FK references to dimensions)
IF OBJECT_ID('dw.fact_sales',       'U') IS NOT NULL DROP TABLE dw.fact_sales;
IF OBJECT_ID('dw.fact_deliveries',  'U') IS NOT NULL DROP TABLE dw.fact_deliveries;
IF OBJECT_ID('dw.fact_routes',      'U') IS NOT NULL DROP TABLE dw.fact_routes;
IF OBJECT_ID('dw.fact_exceptions',  'U') IS NOT NULL DROP TABLE dw.fact_exceptions;

-- Drop dimensions after facts
IF OBJECT_ID('dw.dim_date',             'U') IS NOT NULL DROP TABLE dw.dim_date;
IF OBJECT_ID('dw.dim_product_type',     'U') IS NOT NULL DROP TABLE dw.dim_product_type;
IF OBJECT_ID('dw.dim_region',           'U') IS NOT NULL DROP TABLE dw.dim_region;
IF OBJECT_ID('dw.dim_driver',           'U') IS NOT NULL DROP TABLE dw.dim_driver;
IF OBJECT_ID('dw.dim_route',            'U') IS NOT NULL DROP TABLE dw.dim_route;
IF OBJECT_ID('dw.dim_shipment_type',    'U') IS NOT NULL DROP TABLE dw.dim_shipment_type;
IF OBJECT_ID('dw.dim_delivery_status',  'U') IS NOT NULL DROP TABLE dw.dim_delivery_status;
IF OBJECT_ID('dw.dim_exception_type',   'U') IS NOT NULL DROP TABLE dw.dim_exception_type;
IF OBJECT_ID('dw.dim_priority_flag',    'U') IS NOT NULL DROP TABLE dw.dim_priority_flag;

PRINT 'Existing DW tables dropped.';


/*==============================================================
  STEP 2: CREATE DIMENSION TABLES
==============================================================*/
PRINT 'STEP 2: Creating dimension tables...';

-- Date dimension
-- DateKey is stored as INT in YYYYMMDD format so it can be
-- used as a lightweight integer surrogate key in fact tables
-- and joined efficiently without implicit date conversions.
CREATE TABLE dw.dim_date
(
    DateKey     INT          NOT NULL CONSTRAINT PK_dim_date PRIMARY KEY,
    FullDate    DATE         NOT NULL,
    [Year]      INT          NOT NULL,
    Quarter     INT          NOT NULL,
    [Month]     INT          NOT NULL,
    [Day]       INT          NOT NULL,
    Weekday     INT          NOT NULL,  -- 1=Sunday ... 7=Saturday (@@DATEFIRST default)
    IsWeekend   BIT          NOT NULL,
    MonthName   NVARCHAR(20) NOT NULL,
    DayName     NVARCHAR(20) NOT NULL,
    WeekOfYear  INT          NOT NULL,
    MonthYear   NVARCHAR(7)  NOT NULL,  -- e.g. '2024-01'
    YearMonth   NVARCHAR(6)  NOT NULL,  -- e.g. '202401'
    FiscalYear  INT          NOT NULL,
    IsHoliday   BIT          NOT NULL DEFAULT 0  -- placeholder; update from a holiday table
);

-- Product type dimension
CREATE TABLE dw.dim_product_type
(
    ProductTypeID INT          NOT NULL IDENTITY(1,1) CONSTRAINT PK_dim_product_type PRIMARY KEY,
    ProductType   NVARCHAR(100) NOT NULL
);

-- Region dimension
CREATE TABLE dw.dim_region
(
    RegionID INT          NOT NULL IDENTITY(1,1) CONSTRAINT PK_dim_region PRIMARY KEY,
    Region   NVARCHAR(50) NOT NULL
);

-- Driver dimension
-- DriverCode stores the source NVARCHAR driver identifier.
-- Renamed from DriverName to avoid implying it is a display name.
CREATE TABLE dw.dim_driver
(
    DriverID   INT          NOT NULL IDENTITY(1,1) CONSTRAINT PK_dim_driver PRIMARY KEY,
    DriverCode NVARCHAR(50) NOT NULL  -- source DriverID from staging
);

-- Route dimension
-- RouteCode stores the source NVARCHAR route identifier.
CREATE TABLE dw.dim_route
(
    RouteID   INT           NOT NULL IDENTITY(1,1) CONSTRAINT PK_dim_route PRIMARY KEY,
    RouteCode NVARCHAR(10)  NOT NULL  -- source RouteID from staging
);

-- Shipment type dimension
CREATE TABLE dw.dim_shipment_type
(
    ShipmentTypeID INT          NOT NULL IDENTITY(1,1) CONSTRAINT PK_dim_shipment_type PRIMARY KEY,
    ShipmentType   NVARCHAR(50) NOT NULL
);

-- Delivery status dimension
CREATE TABLE dw.dim_delivery_status
(
    DeliveryStatusID INT          NOT NULL IDENTITY(1,1) CONSTRAINT PK_dim_delivery_status PRIMARY KEY,
    DeliveryStatus   NVARCHAR(50) NOT NULL
);

-- Exception type dimension
CREATE TABLE dw.dim_exception_type
(
    ExceptionTypeID INT           NOT NULL IDENTITY(1,1) CONSTRAINT PK_dim_exception_type PRIMARY KEY,
    ExceptionType   NVARCHAR(100) NOT NULL
);

-- Priority flag dimension
CREATE TABLE dw.dim_priority_flag
(
    PriorityFlagID INT NOT NULL IDENTITY(1,1) CONSTRAINT PK_dim_priority_flag PRIMARY KEY,
    PriorityFlag   BIT NOT NULL
);

PRINT 'Dimension tables created.';


/*==============================================================
  STEP 3: CREATE FACT TABLES
  All fact tables have explicit PRIMARY KEY constraints.
  Foreign keys reference dimension surrogate keys only —
  never source business keys.
==============================================================*/
PRINT 'STEP 3: Creating fact tables...';

-- Fact: Sales transactions
CREATE TABLE dw.fact_sales
(
    SalesID       INT            NOT NULL CONSTRAINT PK_fact_sales PRIMARY KEY,
    DeliveryID    INT            NOT NULL,
    DateKey       INT            NOT NULL,
    ProductTypeID INT            NOT NULL,
    RegionID      INT            NOT NULL,
    UnitsSold     INT            NOT NULL,
    SalesAmount   DECIMAL(18,2)  NOT NULL,
    CONSTRAINT FK_fact_sales_Date        FOREIGN KEY (DateKey)       REFERENCES dw.dim_date(DateKey),
    CONSTRAINT FK_fact_sales_ProductType FOREIGN KEY (ProductTypeID) REFERENCES dw.dim_product_type(ProductTypeID),
    CONSTRAINT FK_fact_sales_Region      FOREIGN KEY (RegionID)      REFERENCES dw.dim_region(RegionID)
);

-- Fact: Deliveries
-- DeliveryDateKey and ExpectedDeliveryDateKey are stored as
-- separate INT surrogate keys so reporting can analyze late
-- deliveries by slicing on either date independently.
CREATE TABLE dw.fact_deliveries
(
    DeliveryID              INT NOT NULL CONSTRAINT PK_fact_deliveries PRIMARY KEY,
    RouteID                 INT NOT NULL,
    DriverID                INT NOT NULL,
    ShipmentTypeID          INT NOT NULL,
    DeliveryDateKey         INT NOT NULL,   -- actual delivery date
    ExpectedDeliveryDateKey INT NOT NULL,   -- planned delivery date
    DeliveryStatusID        INT NOT NULL,
    PriorityFlagID          INT NOT NULL,
    CONSTRAINT FK_fact_deliveries_Route            FOREIGN KEY (RouteID)                 REFERENCES dw.dim_route(RouteID),
    CONSTRAINT FK_fact_deliveries_Driver           FOREIGN KEY (DriverID)                REFERENCES dw.dim_driver(DriverID),
    CONSTRAINT FK_fact_deliveries_Shipment         FOREIGN KEY (ShipmentTypeID)          REFERENCES dw.dim_shipment_type(ShipmentTypeID),
    CONSTRAINT FK_fact_deliveries_DeliveryDate     FOREIGN KEY (DeliveryDateKey)         REFERENCES dw.dim_date(DateKey),
    CONSTRAINT FK_fact_deliveries_ExpectedDate     FOREIGN KEY (ExpectedDeliveryDateKey) REFERENCES dw.dim_date(DateKey),
    CONSTRAINT FK_fact_deliveries_Status           FOREIGN KEY (DeliveryStatusID)        REFERENCES dw.dim_delivery_status(DeliveryStatusID),
    CONSTRAINT FK_fact_deliveries_Priority         FOREIGN KEY (PriorityFlagID)          REFERENCES dw.dim_priority_flag(PriorityFlagID)
);

-- Fact: Route performance
-- Composite PK matches staging_routes key structure.
-- EfficiencyRatio and StopVariance carried through from
-- clean.vw_routes v2.0 to avoid duplicating derived logic
-- in the reporting layer.
CREATE TABLE dw.fact_routes
(
    RouteID         INT           NOT NULL,
    DriverID        INT           NOT NULL,
    PlannedStops    INT           NOT NULL,
    ActualStops     INT           NOT NULL,
    PlannedHours    DECIMAL(10,2) NOT NULL,
    ActualHours     DECIMAL(10,2) NOT NULL,
    RegionID        INT           NOT NULL,
    EfficiencyRatio DECIMAL(10,4) NULL,   -- ActualHours / PlannedHours
    StopVariance    INT           NULL,   -- ActualStops - PlannedStops
    CONSTRAINT PK_fact_routes        PRIMARY KEY (RouteID, DriverID),
    CONSTRAINT FK_fact_routes_Route  FOREIGN KEY (RouteID)  REFERENCES dw.dim_route(RouteID),
    CONSTRAINT FK_fact_routes_Driver FOREIGN KEY (DriverID) REFERENCES dw.dim_driver(DriverID),
    CONSTRAINT FK_fact_routes_Region FOREIGN KEY (RegionID) REFERENCES dw.dim_region(RegionID)
);

-- Fact: Delivery exceptions
CREATE TABLE dw.fact_exceptions
(
    ExceptionID         INT           NOT NULL CONSTRAINT PK_fact_exceptions PRIMARY KEY,
    DeliveryID          INT           NOT NULL,
    ExceptionTypeID     INT           NOT NULL,
    DateKey             INT           NOT NULL,   -- DateReported converted to INT key
    ResolutionTimeHours DECIMAL(10,2) NULL,
    PriorityFlagID      INT           NOT NULL,
    RegionID            INT           NOT NULL,
    CONSTRAINT FK_fact_exceptions_ExceptionType FOREIGN KEY (ExceptionTypeID) REFERENCES dw.dim_exception_type(ExceptionTypeID),
    CONSTRAINT FK_fact_exceptions_Date          FOREIGN KEY (DateKey)          REFERENCES dw.dim_date(DateKey),
    CONSTRAINT FK_fact_exceptions_Priority      FOREIGN KEY (PriorityFlagID)   REFERENCES dw.dim_priority_flag(PriorityFlagID),
    CONSTRAINT FK_fact_exceptions_Region        FOREIGN KEY (RegionID)         REFERENCES dw.dim_region(RegionID)
);

PRINT 'Fact tables created.';


/*==============================================================
  STEP 4: LOAD dim_date
  Source DateKey columns are typed DATE in the clean views.
  CONVERT(INT, CONVERT(VARCHAR(8), <date_col>, 112)) produces
  the YYYYMMDD integer format stored in dim_date.DateKey.

  vw_routes is intentionally excluded — routes have no
  date column in the clean schema.

  dim_date must be loaded before all other dimensions and
  facts because facts reference it via FK constraints.
==============================================================*/
PRINT 'STEP 4: Loading dim_date...';

INSERT INTO dw.dim_date
(
    DateKey, FullDate, [Year], Quarter, [Month], [Day],
    Weekday, IsWeekend, MonthName, DayName, WeekOfYear,
    MonthYear, YearMonth, FiscalYear, IsHoliday
)
SELECT DISTINCT
    CONVERT(INT, CONVERT(VARCHAR(8), src.d, 112))                        AS DateKey,
    src.d                                                                 AS FullDate,
    DATEPART(YEAR,    src.d)                                              AS [Year],
    DATEPART(QUARTER, src.d)                                              AS Quarter,
    DATEPART(MONTH,   src.d)                                              AS [Month],
    DATEPART(DAY,     src.d)                                              AS [Day],
    DATEPART(WEEKDAY, src.d)                                              AS Weekday,
    CASE WHEN DATEPART(WEEKDAY, src.d) IN (1, 7) THEN 1 ELSE 0 END       AS IsWeekend,
    DATENAME(MONTH,   src.d)                                              AS MonthName,
    DATENAME(WEEKDAY, src.d)                                              AS DayName,
    DATEPART(WEEK,    src.d)                                              AS WeekOfYear,
    -- MonthYear: 'YYYY-MM'
    CAST(DATEPART(YEAR, src.d) AS VARCHAR(4))
        + '-' + RIGHT('0' + CAST(DATEPART(MONTH, src.d) AS VARCHAR(2)), 2) AS MonthYear,
    -- YearMonth: 'YYYYMM'
    CAST(DATEPART(YEAR,  src.d) AS VARCHAR(4))
        + RIGHT('0' + CAST(DATEPART(MONTH, src.d) AS VARCHAR(2)), 2)       AS YearMonth,
    DATEPART(YEAR, src.d)                                                  AS FiscalYear,
    0                                                                      AS IsHoliday  -- placeholder
FROM (
    -- Sales transaction dates
    SELECT DateKey AS d FROM clean.vw_sales
    UNION
    -- Actual delivery dates
    SELECT DeliveryDate AS d FROM clean.vw_deliveries
    UNION
    -- Expected delivery dates
    SELECT ExpectedDeliveryDate AS d FROM clean.vw_deliveries
    WHERE ExpectedDeliveryDate IS NOT NULL
    UNION
    -- Exception reported dates
    SELECT DateReported AS d FROM clean.vw_exceptions
) AS src;

PRINT 'dim_date loaded.';


/*==============================================================
  STEP 5: LOAD REMAINING DIMENSIONS
==============================================================*/
PRINT 'STEP 5: Loading dimensions...';

-- dim_product_type: sourced from vw_sales only
-- (vw_deliveries has no ProductType column)
INSERT INTO dw.dim_product_type (ProductType)
SELECT DISTINCT ProductType
FROM clean.vw_sales
WHERE ProductType IS NOT NULL;

-- dim_region: union across all clean views that carry Region
INSERT INTO dw.dim_region (Region)
SELECT DISTINCT Region FROM clean.vw_sales
UNION
SELECT DISTINCT Region FROM clean.vw_deliveries
UNION
SELECT DISTINCT Region FROM clean.vw_exceptions
UNION
SELECT DISTINCT Region FROM clean.vw_routes;

-- dim_driver: DriverCode stores the source NVARCHAR DriverID
INSERT INTO dw.dim_driver (DriverCode)
SELECT DISTINCT DriverID FROM clean.vw_deliveries
UNION
SELECT DISTINCT DriverID FROM clean.vw_routes;

-- dim_route: RouteCode stores the source NVARCHAR RouteID
INSERT INTO dw.dim_route (RouteCode)
SELECT DISTINCT RouteID FROM clean.vw_deliveries
UNION
SELECT DISTINCT RouteID FROM clean.vw_routes;

-- dim_shipment_type
INSERT INTO dw.dim_shipment_type (ShipmentType)
SELECT DISTINCT ShipmentType
FROM clean.vw_deliveries
WHERE ShipmentType IS NOT NULL;

-- dim_delivery_status
INSERT INTO dw.dim_delivery_status (DeliveryStatus)
SELECT DISTINCT DeliveryStatus
FROM clean.vw_deliveries
WHERE DeliveryStatus IS NOT NULL;

-- dim_exception_type
INSERT INTO dw.dim_exception_type (ExceptionType)
SELECT DISTINCT ExceptionType
FROM clean.vw_exceptions
WHERE ExceptionType IS NOT NULL;

-- dim_priority_flag: only two valid values (0, 1)
INSERT INTO dw.dim_priority_flag (PriorityFlag)
SELECT DISTINCT PriorityFlag
FROM clean.vw_deliveries
UNION
SELECT DISTINCT PriorityFlag
FROM clean.vw_exceptions;

PRINT 'Dimensions loaded.';


/*==============================================================
  STEP 6: LOAD FACT TABLES
  All joins are INNER JOINs to dimension tables. A row that
  cannot resolve a surrogate key (e.g. a missing date in
  dim_date) will be silently excluded. Row counts after load
  should be compared against clean view counts in the
  validation script to detect any unresolved lookups.

  DateKey columns: source DATE values are converted to INT
  (YYYYMMDD) before joining to dim_date, consistent with
  how dim_date was populated in Step 4.
==============================================================*/
PRINT 'STEP 6: Loading fact tables...';

-- fact_sales
INSERT INTO dw.fact_sales
    (SalesID, DeliveryID, DateKey, ProductTypeID, RegionID, UnitsSold, SalesAmount)
SELECT
    s.SalesID,
    s.DeliveryID,
    dd.DateKey,
    dp.ProductTypeID,
    dr.RegionID,
    s.UnitsSold,
    s.SalesAmount
FROM clean.vw_sales s
JOIN dw.dim_date         dd ON CONVERT(INT, CONVERT(VARCHAR(8), s.DateKey, 112)) = dd.DateKey
JOIN dw.dim_product_type dp ON s.ProductType = dp.ProductType
JOIN dw.dim_region       dr ON s.Region      = dr.Region;

PRINT 'fact_sales loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows.';

-- fact_deliveries
-- Both DeliveryDate and ExpectedDeliveryDate are joined to
-- dim_date to populate the two date surrogate key columns.
INSERT INTO dw.fact_deliveries
    (DeliveryID, RouteID, DriverID, ShipmentTypeID,
     DeliveryDateKey, ExpectedDeliveryDateKey,
     DeliveryStatusID, PriorityFlagID)
SELECT
    d.DeliveryID,
    r.RouteID,
    drv.DriverID,
    st.ShipmentTypeID,
    dd_actual.DateKey                                                  AS DeliveryDateKey,
    dd_expected.DateKey                                                AS ExpectedDeliveryDateKey,
    ds.DeliveryStatusID,
    pf.PriorityFlagID
FROM clean.vw_deliveries d
JOIN dw.dim_route           r           ON d.RouteID        = r.RouteCode
JOIN dw.dim_driver          drv         ON d.DriverID       = drv.DriverCode
JOIN dw.dim_shipment_type   st          ON d.ShipmentType   = st.ShipmentType
JOIN dw.dim_delivery_status ds          ON d.DeliveryStatus = ds.DeliveryStatus
JOIN dw.dim_priority_flag   pf          ON d.PriorityFlag   = pf.PriorityFlag
JOIN dw.dim_date             dd_actual   ON CONVERT(INT, CONVERT(VARCHAR(8), d.DeliveryDate,         112)) = dd_actual.DateKey
JOIN dw.dim_date             dd_expected ON CONVERT(INT, CONVERT(VARCHAR(8), d.ExpectedDeliveryDate, 112)) = dd_expected.DateKey;

PRINT 'fact_deliveries loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows.';

-- fact_routes
-- EfficiencyRatio and StopVariance sourced directly from
-- vw_routes v2.0 derived columns.
INSERT INTO dw.fact_routes
    (RouteID, DriverID, PlannedStops, ActualStops,
     PlannedHours, ActualHours, RegionID,
     EfficiencyRatio, StopVariance)
SELECT
    r.RouteID,
    drv.DriverID,
    rt.PlannedStops,
    rt.ActualStops,
    rt.PlannedHours,
    rt.ActualHours,
    rg.RegionID,
    rt.EfficiencyRatio,
    rt.StopVariance
FROM clean.vw_routes rt
JOIN dw.dim_route   r   ON rt.RouteID  = r.RouteCode
JOIN dw.dim_driver  drv ON rt.DriverID = drv.DriverCode
JOIN dw.dim_region  rg  ON rt.Region   = rg.Region;

PRINT 'fact_routes loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows.';

-- fact_exceptions
INSERT INTO dw.fact_exceptions
    (ExceptionID, DeliveryID, ExceptionTypeID, DateKey,
     ResolutionTimeHours, PriorityFlagID, RegionID)
SELECT
    e.ExceptionID,
    e.DeliveryID,
    et.ExceptionTypeID,
    dd.DateKey,
    e.ResolutionTimeHours,
    pf.PriorityFlagID,
    rg.RegionID
FROM clean.vw_exceptions e
JOIN dw.dim_exception_type et ON e.ExceptionType = et.ExceptionType
JOIN dw.dim_priority_flag  pf ON e.PriorityFlag  = pf.PriorityFlag
JOIN dw.dim_region         rg ON e.Region        = rg.Region
JOIN dw.dim_date           dd ON CONVERT(INT, CONVERT(VARCHAR(8), e.DateReported, 112)) = dd.DateKey;

PRINT 'fact_exceptions loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows.';


COMMIT TRANSACTION;
PRINT '--- DW LOAD COMPLETE ---';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '--- DW LOAD FAILED — all changes rolled back ---';
    PRINT 'Error ' + CAST(ERROR_NUMBER() AS VARCHAR) + ': ' + ERROR_MESSAGE();
    THROW;
END CATCH;
