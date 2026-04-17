/*=============================================================
  DATA WAREHOUSE LAYER
  Database: Fedex_Ops_Database
  Version:  1.0

  Purpose:
      Build the star schema in the dw schema. Populates seven
      dimension tables and three fact tables from the clean
      layer views. The DW layer is the source for all
      reporting and Power BI consumption.

  Run Order:
      1. etl_staging_setup.sql          -- build schemas/tables
      2. load_staging.py                   -- load CSV data
      3. staging_layer_validation.sql   -- validate staging
      4. clean_layer.sql                -- build clean views
      5. 07_clean_validation_gate.0     -- pipeline gate
      6. THIS SCRIPT                       -- build DW layer

  Star Schema:
      Dimensions:
          dw.dim_date            -- date spine 2023-2026
          dw.dim_driver          -- 20 drivers + Unknown
          dw.dim_region          -- 7 regions
          dw.dim_product         -- 4 product types
          dw.dim_shipment_type   -- 3 shipment types
          dw.dim_exception_type  -- 4 exception types
          dw.dim_route           -- 5 routes

      Facts:
          dw.fact_sales          -- one row per sales transaction
          dw.fact_deliveries     -- one row per delivery
          dw.fact_exceptions     -- one row per exception event

  Design Decisions:
      - Surrogate integer keys (SK) are used on all dimension
        tables. Natural keys are retained as alternate keys
        for traceability.
      - dim_date uses DateKey (INT in YYYYMMDD format) as its
        primary key for fast date joins in reporting tools.
      - fact_route_performance is intentionally excluded as a
        separate fact table. Route metrics (StopVariance,
        HourVariance, efficiency percentages) are better
        served as attributes on fact_deliveries via the
        RouteID join to dim_route, avoiding a fanout join
        problem caused by the duplicate RouteID+DriverID
        combinations in the source data.
      - All fact tables use TRUNCATE + INSERT so the script
        is safe to re-run and always reflects the latest
        clean layer data.
      - Foreign key constraints are defined but not enforced
        (NOCHECK) to allow fast bulk loads. Referential
        integrity is guaranteed by the clean layer gate.

=============================================================*/

USE Fedex_Ops_Database;
GO

/*=============================================================
  STEP 1: DROP EXISTING TABLES
  Drop facts before dimensions to respect FK dependencies.
  Safe to re-run -- tables are recreated in Step 2.
=============================================================*/

-- Drop fact tables first (reference dimensions)
IF OBJECT_ID('dw.fact_exceptions',  'U') IS NOT NULL DROP TABLE dw.fact_exceptions;
IF OBJECT_ID('dw.fact_sales',       'U') IS NOT NULL DROP TABLE dw.fact_sales;
IF OBJECT_ID('dw.fact_deliveries',  'U') IS NOT NULL DROP TABLE dw.fact_deliveries;

-- Drop dimension tables
IF OBJECT_ID('dw.dim_date',          'U') IS NOT NULL DROP TABLE dw.dim_date;
IF OBJECT_ID('dw.dim_driver',        'U') IS NOT NULL DROP TABLE dw.dim_driver;
IF OBJECT_ID('dw.dim_region',        'U') IS NOT NULL DROP TABLE dw.dim_region;
IF OBJECT_ID('dw.dim_product',       'U') IS NOT NULL DROP TABLE dw.dim_product;
IF OBJECT_ID('dw.dim_shipment_type', 'U') IS NOT NULL DROP TABLE dw.dim_shipment_type;
IF OBJECT_ID('dw.dim_exception_type','U') IS NOT NULL DROP TABLE dw.dim_exception_type;
IF OBJECT_ID('dw.dim_route',         'U') IS NOT NULL DROP TABLE dw.dim_route;
GO

/*=============================================================
  STEP 2: CREATE DIMENSION TABLES
=============================================================*/

-- -----------------------------------------------------------
-- DIMENSION: dim_date
-- Date spine covering 2023-01-01 to 2026-12-31 (1,461 rows).
-- DateKey is stored as INT in YYYYMMDD format for fast joins
-- in Power BI and SQL reporting tools. All time intelligence
-- attributes are pre-computed to avoid repeated derivation
-- in reporting queries.
-- -----------------------------------------------------------
CREATE TABLE dw.dim_date (
    DateKey         INT          NOT NULL,   -- YYYYMMDD e.g. 20230101
    FullDate        DATE         NOT NULL,
    DayOfWeek       INT          NOT NULL,   -- 1=Sunday, 7=Saturday
    DayName         NVARCHAR(10) NOT NULL,   -- Monday, Tuesday...
    DayOfMonth      INT          NOT NULL,   -- 1-31
    DayOfYear       INT          NOT NULL,   -- 1-366
    WeekOfYear      INT          NOT NULL,   -- ISO week number
    MonthNumber     INT          NOT NULL,   -- 1-12
    MonthName       NVARCHAR(10) NOT NULL,   -- January, February...
    MonthShort      NVARCHAR(3)  NOT NULL,   -- Jan, Feb...
    Quarter         INT          NOT NULL,   -- 1-4
    QuarterName     NVARCHAR(6)  NOT NULL,   -- Q1, Q2, Q3, Q4
    YearNumber      INT          NOT NULL,   -- 2023, 2024, 2025
    YearMonth       NVARCHAR(7)  NOT NULL,   -- 2023-01 format
    YearQuarter     NVARCHAR(7)  NOT NULL,   -- 2023-Q1 format
    IsWeekend       BIT          NOT NULL,   -- 1 if Saturday or Sunday
    CONSTRAINT PK_dim_date PRIMARY KEY (DateKey)
);

-- -----------------------------------------------------------
-- DIMENSION: dim_driver
-- 20 named drivers from source data plus one Unknown row to
-- handle deliveries and routes with no DriverID in source.
-- DriverCode is the natural key retained for traceability.
-- -----------------------------------------------------------
CREATE TABLE dw.dim_driver (
    DriverSK        INT IDENTITY(1,1) NOT NULL,
    DriverCode      NVARCHAR(50)      NOT NULL,   -- Natural key e.g. 'Driver 1'
    DriverLabel     NVARCHAR(50)      NOT NULL,   -- Display name
    IsUnknown       BIT               NOT NULL DEFAULT 0,
    CONSTRAINT PK_dim_driver PRIMARY KEY (DriverSK),
    CONSTRAINT UQ_dim_driver_code UNIQUE (DriverCode)
);

-- -----------------------------------------------------------
-- DIMENSION: dim_region
-- Seven clean region codes after truncation mapping applied.
-- RegionCode is the natural key. RegionName provides a
-- human-readable label for reporting.
-- -----------------------------------------------------------
CREATE TABLE dw.dim_region (
    RegionSK        INT IDENTITY(1,1) NOT NULL,
    RegionCode      NVARCHAR(5)       NOT NULL,   -- Natural key e.g. 'NE'
    RegionName      NVARCHAR(50)      NOT NULL,   -- e.g. 'North East'
    CONSTRAINT PK_dim_region PRIMARY KEY (RegionSK),
    CONSTRAINT UQ_dim_region_code UNIQUE (RegionCode)
);

-- -----------------------------------------------------------
-- DIMENSION: dim_product
-- Four product types from sales data after truncation mapping.
-- -----------------------------------------------------------
CREATE TABLE dw.dim_product (
    ProductSK       INT IDENTITY(1,1) NOT NULL,
    ProductType     NVARCHAR(50)      NOT NULL,   -- Natural key
    CONSTRAINT PK_dim_product PRIMARY KEY (ProductSK),
    CONSTRAINT UQ_dim_product_type UNIQUE (ProductType)
);

-- -----------------------------------------------------------
-- DIMENSION: dim_shipment_type
-- Three shipment types from deliveries after truncation mapping.
-- -----------------------------------------------------------
CREATE TABLE dw.dim_shipment_type (
    ShipmentTypeSK  INT IDENTITY(1,1) NOT NULL,
    ShipmentType    NVARCHAR(20)      NOT NULL,   -- Natural key
    CONSTRAINT PK_dim_shipment_type PRIMARY KEY (ShipmentTypeSK),
    CONSTRAINT UQ_dim_shipment_type UNIQUE (ShipmentType)
);

-- -----------------------------------------------------------
-- DIMENSION: dim_exception_type
-- Four exception types from exceptions data after truncation
-- mapping applied.
-- -----------------------------------------------------------
CREATE TABLE dw.dim_exception_type (
    ExceptionTypeSK INT IDENTITY(1,1) NOT NULL,
    ExceptionType   NVARCHAR(50)      NOT NULL,   -- Natural key
    CONSTRAINT PK_dim_exception_type PRIMARY KEY (ExceptionTypeSK),
    CONSTRAINT UQ_dim_exception_type UNIQUE (ExceptionType)
);

-- -----------------------------------------------------------
-- DIMENSION: dim_route
-- Five routes (R001-R005) from source data.
-- -----------------------------------------------------------
CREATE TABLE dw.dim_route (
    RouteSK         INT IDENTITY(1,1) NOT NULL,
    RouteID         NVARCHAR(10)      NOT NULL,   -- Natural key e.g. 'R001'
    CONSTRAINT PK_dim_route PRIMARY KEY (RouteSK),
    CONSTRAINT UQ_dim_route_id UNIQUE (RouteID)
);

PRINT 'Dimension tables created.';
GO

/*=============================================================
  STEP 3: CREATE FACT TABLES
=============================================================*/

-- -----------------------------------------------------------
-- FACT TABLE: fact_deliveries
-- Grain: one row per delivery.
-- Central fact table -- referenced by fact_sales and
-- fact_exceptions via DeliveryID.
-- Measures: DaysVariance, IsLate, PriorityFlag.
-- -----------------------------------------------------------
CREATE TABLE dw.fact_deliveries (
    DeliveryID      INT          NOT NULL,
    DateKey         INT          NOT NULL,   -- FK to dim_date
    DriverSK        INT          NOT NULL,   -- FK to dim_driver
    RegionSK        INT          NOT NULL,   -- FK to dim_region
    ShipmentTypeSK  INT          NOT NULL,   -- FK to dim_shipment_type
    RouteSK         INT          NOT NULL,   -- FK to dim_route
    DeliveryStatus  NVARCHAR(20) NOT NULL,   -- On-Time, Late, Exception
    IsLate          BIT          NOT NULL,   -- 1 = Late or Exception
    PriorityFlag    BIT          NOT NULL,   -- 1 = Priority delivery
    DaysVariance    INT          NULL,       -- Positive=late, Negative=early
    CONSTRAINT PK_fact_deliveries PRIMARY KEY (DeliveryID),
    CONSTRAINT FK_fact_deliveries_date
        FOREIGN KEY (DateKey)        REFERENCES dw.dim_date (DateKey),
    CONSTRAINT FK_fact_deliveries_driver
        FOREIGN KEY (DriverSK)       REFERENCES dw.dim_driver (DriverSK),
    CONSTRAINT FK_fact_deliveries_region
        FOREIGN KEY (RegionSK)       REFERENCES dw.dim_region (RegionSK),
    CONSTRAINT FK_fact_deliveries_shipment
        FOREIGN KEY (ShipmentTypeSK) REFERENCES dw.dim_shipment_type (ShipmentTypeSK),
    CONSTRAINT FK_fact_deliveries_route
        FOREIGN KEY (RouteSK)        REFERENCES dw.dim_route (RouteSK)
);

-- -----------------------------------------------------------
-- FACT TABLE: fact_sales
-- Grain: one row per sales transaction.
-- Measures: UnitsSold, SalesAmount, RevenuePerUnit.
-- Joins to fact_deliveries via DeliveryID for delivery context.
-- -----------------------------------------------------------
CREATE TABLE dw.fact_sales (
    SalesID         INT            NOT NULL,
    DeliveryID      INT            NOT NULL,   -- FK to fact_deliveries
    DateKey         INT            NOT NULL,   -- FK to dim_date
    ProductSK       INT            NOT NULL,   -- FK to dim_product
    RegionSK        INT            NOT NULL,   -- FK to dim_region
    UnitsSold       INT            NOT NULL,
    SalesAmount     DECIMAL(10,2)  NOT NULL,
    RevenuePerUnit  DECIMAL(10,2)  NULL,
    CONSTRAINT PK_fact_sales PRIMARY KEY (SalesID),
    CONSTRAINT FK_fact_sales_delivery
        FOREIGN KEY (DeliveryID) REFERENCES dw.fact_deliveries (DeliveryID),
    CONSTRAINT FK_fact_sales_date
        FOREIGN KEY (DateKey)    REFERENCES dw.dim_date (DateKey),
    CONSTRAINT FK_fact_sales_product
        FOREIGN KEY (ProductSK)  REFERENCES dw.dim_product (ProductSK),
    CONSTRAINT FK_fact_sales_region
        FOREIGN KEY (RegionSK)   REFERENCES dw.dim_region (RegionSK)
);

-- -----------------------------------------------------------
-- FACT TABLE: fact_exceptions
-- Grain: one row per exception event.
-- Measures: ResolutionTimeHours, ResolutionDays, IsResolved.
-- -----------------------------------------------------------
CREATE TABLE dw.fact_exceptions (
    ExceptionID         INT           NOT NULL,
    DeliveryID          INT           NOT NULL,   -- FK to fact_deliveries
    DateReportedKey     INT           NOT NULL,   -- FK to dim_date (reported)
    ResolvedDateKey     INT           NULL,       -- FK to dim_date (resolved), NULL if open
    ExceptionTypeSK     INT           NOT NULL,   -- FK to dim_exception_type
    RegionSK            INT           NOT NULL,   -- FK to dim_region
    PriorityFlag        BIT           NOT NULL,
    IsResolved          BIT           NOT NULL,   -- 1 = resolved, 0 = open
    ResolutionTimeHours DECIMAL(6,2)  NULL,       -- NULL if still open
    ResolutionDays      INT           NULL,       -- NULL if still open
    CONSTRAINT PK_fact_exceptions PRIMARY KEY (ExceptionID),
    CONSTRAINT FK_fact_exceptions_delivery
        FOREIGN KEY (DeliveryID)      REFERENCES dw.fact_deliveries (DeliveryID),
    CONSTRAINT FK_fact_exceptions_date_reported
        FOREIGN KEY (DateReportedKey) REFERENCES dw.dim_date (DateKey),
    CONSTRAINT FK_fact_exceptions_date_resolved
        FOREIGN KEY (ResolvedDateKey) REFERENCES dw.dim_date (DateKey),
    CONSTRAINT FK_fact_exceptions_type
        FOREIGN KEY (ExceptionTypeSK) REFERENCES dw.dim_exception_type (ExceptionTypeSK),
    CONSTRAINT FK_fact_exceptions_region
        FOREIGN KEY (RegionSK)        REFERENCES dw.dim_region (RegionSK)
);

PRINT 'Fact tables created.';
GO

/*=============================================================
  STEP 4: POPULATE DIMENSION TABLES
=============================================================*/

-- -----------------------------------------------------------
-- POPULATE: dim_date
-- Generates one row per calendar day from 2023-01-01 to
-- 2025-12-31 (1,096 rows) using a recursive CTE. All time
-- intelligence attributes are computed inline.
-- -----------------------------------------------------------
;WITH DateCTE AS (
    SELECT CAST('2023-01-01' AS DATE) AS FullDate
    UNION ALL
    SELECT DATEADD(DAY, 1, FullDate)
    FROM DateCTE
    WHERE FullDate < '2026-12-31'
)
INSERT INTO dw.dim_date (
    DateKey, FullDate, DayOfWeek, DayName, DayOfMonth, DayOfYear,
    WeekOfYear, MonthNumber, MonthName, MonthShort, Quarter,
    QuarterName, YearNumber, YearMonth, YearQuarter, IsWeekend
)
SELECT
    -- YYYYMMDD integer key for fast joins
    CAST(FORMAT(FullDate, 'yyyyMMdd') AS INT)           AS DateKey,
    FullDate,
    DATEPART(WEEKDAY, FullDate)                         AS DayOfWeek,
    DATENAME(WEEKDAY, FullDate)                         AS DayName,
    DAY(FullDate)                                       AS DayOfMonth,
    DATEPART(DAYOFYEAR, FullDate)                       AS DayOfYear,
    DATEPART(ISO_WEEK, FullDate)                        AS WeekOfYear,
    MONTH(FullDate)                                     AS MonthNumber,
    DATENAME(MONTH, FullDate)                           AS MonthName,
    LEFT(DATENAME(MONTH, FullDate), 3)                  AS MonthShort,
    DATEPART(QUARTER, FullDate)                         AS Quarter,
    'Q' + CAST(DATEPART(QUARTER, FullDate) AS NVARCHAR) AS QuarterName,
    YEAR(FullDate)                                      AS YearNumber,
    FORMAT(FullDate, 'yyyy-MM')                         AS YearMonth,
    CAST(YEAR(FullDate) AS NVARCHAR)
        + '-Q'
        + CAST(DATEPART(QUARTER, FullDate) AS NVARCHAR) AS YearQuarter,
    CASE
        WHEN DATEPART(WEEKDAY, FullDate) IN (1, 7)
		THEN 1
        ELSE 0
    END AS IsWeekend
FROM DateCTE
OPTION (MAXRECURSION 1461);

PRINT 'Populated: dw.dim_date (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

-- -----------------------------------------------------------
-- POPULATE: dim_driver
-- Sources distinct DriverID values from both clean_deliveries
-- and clean_routes to ensure all drivers are represented.
-- The Unknown row is included to handle NULL source values
-- that were replaced during clean layer transformation.
-- -----------------------------------------------------------
INSERT INTO dw.dim_driver (DriverCode, DriverLabel, IsUnknown)

    -- Named drivers from clean layer
    SELECT DISTINCT
        DriverID AS DriverCode,
        DriverID AS DriverLabel,
        0 AS IsUnknown
    FROM (
        SELECT DriverID FROM clean.clean_deliveries WHERE DriverID <> 'Unknown'
        UNION
        SELECT DriverID FROM clean.clean_routes     WHERE DriverID <> 'Unknown'
    ) drivers

UNION ALL

    -- Unknown row for unassigned deliveries and routes
    SELECT
        'Unknown' AS DriverCode,
        'Unknown Driver' AS DriverLabel,
        1 AS IsUnknown;

PRINT 'Populated: dw.dim_driver (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

-- -----------------------------------------------------------
-- POPULATE: dim_region
-- Seven clean region codes with full descriptive names.
-- RegionName provides human-readable labels for reporting.
-- -----------------------------------------------------------
INSERT INTO dw.dim_region (RegionCode, RegionName)
VALUES
    ('MW', 'Midwest'),
    ('N',  'North'),
    ('NE', 'North East'),
    ('NW', 'North West'),
    ('S',  'South'),
    ('SE', 'South East'),
    ('SW', 'South West');

PRINT 'Populated: dw.dim_region (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

-- -----------------------------------------------------------
-- POPULATE: dim_product
-- Four product types sourced from clean_sales.
-- -----------------------------------------------------------
INSERT INTO dw.dim_product (ProductType)
SELECT DISTINCT ProductType
FROM clean.clean_sales
ORDER BY ProductType;

PRINT 'Populated: dw.dim_product (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

-- -----------------------------------------------------------
-- POPULATE: dim_shipment_type
-- Three shipment types sourced from clean_deliveries.
-- -----------------------------------------------------------
INSERT INTO dw.dim_shipment_type (ShipmentType)
SELECT DISTINCT ShipmentType
FROM clean.clean_deliveries
ORDER BY ShipmentType;

PRINT 'Populated: dw.dim_shipment_type (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

-- -----------------------------------------------------------
-- POPULATE: dim_exception_type
-- Four exception types sourced from clean_exceptions.
-- -----------------------------------------------------------
INSERT INTO dw.dim_exception_type (ExceptionType)
SELECT DISTINCT ExceptionType
FROM clean.clean_exceptions
ORDER BY ExceptionType;

PRINT 'Populated: dw.dim_exception_type (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

-- -----------------------------------------------------------
-- POPULATE: dim_route
-- Five routes (R001-R005) sourced from clean_routes.
-- -----------------------------------------------------------
INSERT INTO dw.dim_route (RouteID)
SELECT DISTINCT RouteID
FROM clean.clean_routes
ORDER BY RouteID;

PRINT 'Populated: dw.dim_route (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

/*=============================================================
  STEP 5: POPULATE FACT TABLES
  Load order: fact_deliveries first as fact_sales and
  fact_exceptions both reference it via DeliveryID.
=============================================================*/

-- -----------------------------------------------------------
-- POPULATE: fact_deliveries
-- Joins clean_deliveries to all relevant dimensions to
-- resolve surrogate keys. DeliveryDateOnly (DATE) is
-- converted to YYYYMMDD INT to join dim_date.DateKey.
-- -----------------------------------------------------------
INSERT INTO dw.fact_deliveries (
    DeliveryID,
    DateKey,
    DriverSK,
    RegionSK,
    ShipmentTypeSK,
    RouteSK,
    DeliveryStatus,
    IsLate,
    PriorityFlag,
    DaysVariance
)
SELECT
    cd.DeliveryID,

    -- Convert DATE to YYYYMMDD INT for dim_date join
    CAST(FORMAT(cd.DeliveryDateOnly, 'yyyyMMdd') AS INT) AS DateKey,

    dd.DriverSK,
    dr.RegionSK,
    dst.ShipmentTypeSK,
    drt.RouteSK,
    cd.DeliveryStatus,
    cd.IsLate,
    cd.PriorityFlag,
    cd.DaysVariance

FROM clean.clean_deliveries cd
JOIN dw.dim_driver          dd  ON dd.DriverCode    = cd.DriverID
JOIN dw.dim_region          dr  ON dr.RegionCode    = cd.Region
JOIN dw.dim_shipment_type   dst ON dst.ShipmentType = cd.ShipmentType
JOIN dw.dim_route           drt ON drt.RouteID      = cd.RouteID;

PRINT 'Populated: dw.fact_deliveries (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

-- -----------------------------------------------------------
-- POPULATE: fact_sales
-- Joins clean_sales to dim_product, dim_region, and dim_date.
-- Also joins to fact_deliveries to enforce the FK constraint.
-- DateKey converted from DATETIME2 SaleDate to YYYYMMDD INT.
-- -----------------------------------------------------------
INSERT INTO dw.fact_sales (
    SalesID,
    DeliveryID,
    DateKey,
    ProductSK,
    RegionSK,
    UnitsSold,
    SalesAmount,
    RevenuePerUnit
)
SELECT
    cs.SalesID,
    cs.DeliveryID,

    -- Convert DATE to YYYYMMDD INT for dim_date join
    CAST(FORMAT(cs.SaleDate, 'yyyyMMdd') AS INT) AS DateKey,

    dp.ProductSK,
    dr.RegionSK,
    cs.UnitsSold,
    cs.SalesAmount,
    cs.RevenuePerUnit

FROM clean.clean_sales              cs
JOIN dw.dim_product         dp  ON dp.ProductType = cs.ProductType
JOIN dw.dim_region          dr  ON dr.RegionCode  = cs.Region
JOIN dw.fact_deliveries     fd  ON fd.DeliveryID  = cs.DeliveryID;

PRINT 'Populated: dw.fact_sales (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

-- -----------------------------------------------------------
-- POPULATE: fact_exceptions
-- Joins clean_exceptions to dim_exception_type, dim_region,
-- and dim_date (twice: DateReportedKey and ResolvedDateKey).
-- ResolvedDateKey is NULL for open exceptions.
-- -----------------------------------------------------------
INSERT INTO dw.fact_exceptions (
    ExceptionID,
    DeliveryID,
    DateReportedKey,
    ResolvedDateKey,
    ExceptionTypeSK,
    RegionSK,
    PriorityFlag,
    IsResolved,
    ResolutionTimeHours,
    ResolutionDays
)
SELECT
    ce.ExceptionID,
    ce.DeliveryID,

    -- DateReportedKey: convert DATE to YYYYMMDD INT
    CAST(FORMAT(ce.DateReportedOnly, 'yyyyMMdd') AS INT) AS DateReportedKey,

    -- ResolvedDateKey: NULL for open exceptions
    CASE
        WHEN ce.ResolvedDateOnly IS NOT NULL
        THEN CAST(FORMAT(ce.ResolvedDateOnly, 'yyyyMMdd') AS INT)
        ELSE NULL
    END AS ResolvedDateKey,

    det.ExceptionTypeSK,
    dr.RegionSK,
    ce.PriorityFlag,
    ce.IsResolved,
    ce.ResolutionTimeHours,
    ce.ResolutionDays

FROM clean.clean_exceptions         ce
JOIN dw.dim_exception_type  det ON det.ExceptionType = ce.ExceptionType
JOIN dw.dim_region          dr  ON dr.RegionCode     = ce.Region
JOIN dw.fact_deliveries     fd  ON fd.DeliveryID     = ce.DeliveryID;

PRINT 'Populated: dw.fact_exceptions (' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows)';
GO

/*=============================================================
  STEP 6: ROW COUNT VERIFICATION
  Confirms expected row counts loaded into each table.
  Dimension counts are small and fixed. Fact counts should
  match clean layer row counts exactly.

  Expected:
      dim_date           1,461 rows  (2023-01-01 to 2026-12-31)
      dim_driver            21 rows  (20 named + 1 Unknown)
      dim_region             7 rows
      dim_product            4 rows
      dim_shipment_type      3 rows
      dim_exception_type     4 rows
      dim_route              5 rows
      fact_deliveries    5,620 rows
      fact_sales         4,550 rows
      fact_exceptions    1,010 rows
=============================================================*/

PRINT '--- DW ROW COUNT VERIFICATION ---';

SELECT 'dim_date'           AS TableName, COUNT(*) AS RowsCount FROM dw.dim_date
UNION ALL
SELECT 'dim_driver'         AS TableName, COUNT(*) AS RowsCount FROM dw.dim_driver
UNION ALL
SELECT 'dim_region'         AS TableName, COUNT(*) AS RowsCount FROM dw.dim_region
UNION ALL
SELECT 'dim_product'        AS TableName, COUNT(*) AS RowsCount FROM dw.dim_product
UNION ALL
SELECT 'dim_shipment_type'  AS TableName, COUNT(*) AS RowsCount FROM dw.dim_shipment_type
UNION ALL
SELECT 'dim_exception_type' AS TableName, COUNT(*) AS RowsCount FROM dw.dim_exception_type
UNION ALL
SELECT 'dim_route'          AS TableName, COUNT(*) AS RowsCount FROM dw.dim_route
UNION ALL
SELECT 'fact_deliveries'    AS TableName, COUNT(*) AS RowsCount FROM dw.fact_deliveries
UNION ALL
SELECT 'fact_sales'         AS TableName, COUNT(*) AS RowsCount FROM dw.fact_sales
UNION ALL
SELECT 'fact_exceptions'    AS TableName, COUNT(*) AS RowsCount FROM dw.fact_exceptions;

PRINT '--- END OF DW ROW COUNT VERIFICATION ---';
GO
