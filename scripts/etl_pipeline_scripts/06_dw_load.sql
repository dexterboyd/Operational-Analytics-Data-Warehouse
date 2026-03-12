/*==============================================================
  DW LOAD SCRIPT - DYNAMIC & SURROGATE KEY SAFE
  Purpose:
      Load dimensions and facts from clean views into the DW.
      Fully dynamic, metadata-driven, and surrogate key safe.

  Pipeline: staging → clean → DW → validation → reporting → BI
==============================================================*/

PRINT '--- DW LOAD START ---';

-----------------------------------------------------
-- STEP 1: DROP ALL DW TABLES DYNAMICALLY
-----------------------------------------------------
DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql = STRING_AGG('DROP TABLE dw.' + QUOTENAME(t.name) + ';', CHAR(13))
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dw';

IF @sql IS NOT NULL
    EXEC sp_executesql @sql;

PRINT 'All DW tables dropped dynamically';

-----------------------------------------------------
-- STEP 2: CREATE DIMENSION TABLES
-----------------------------------------------------
-- Date dimension
CREATE TABLE dw.dim_date
(
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL,
    [Year] INT,
    Quarter INT,
    [Month] INT,
    [Day] INT,
    Weekday INT,
    IsWeekend BIT,
    MonthName NVARCHAR(20),
    IsHoliday BIT,
    DayName NVARCHAR(20),
    WeekOfYear INT,
    MonthYear NVARCHAR(7),
    YearMonth NVARCHAR(7),
    FiscalYear INT
);

-- Generic dimensions (other dimensions follow naming convention)
CREATE TABLE dw.dim_product_type (ProductTypeID INT IDENTITY(1,1) PRIMARY KEY, ProductType NVARCHAR(100) NOT NULL);
CREATE TABLE dw.dim_region (RegionID INT IDENTITY(1,1) PRIMARY KEY, Region NVARCHAR(50) NOT NULL);
CREATE TABLE dw.dim_driver (DriverID INT IDENTITY(1,1) PRIMARY KEY, DriverName NVARCHAR(100) NOT NULL);
CREATE TABLE dw.dim_route (RouteID INT IDENTITY(1,1) PRIMARY KEY, RouteName NVARCHAR(100) NOT NULL);
CREATE TABLE dw.dim_shipment_type (ShipmentTypeID INT IDENTITY(1,1) PRIMARY KEY, ShipmentType NVARCHAR(50) NOT NULL);
CREATE TABLE dw.dim_delivery_status (DeliveryStatusID INT IDENTITY(1,1) PRIMARY KEY, DeliveryStatus NVARCHAR(50) NOT NULL);
CREATE TABLE dw.dim_exception_type (ExceptionTypeID INT IDENTITY(1,1) PRIMARY KEY, ExceptionType NVARCHAR(100) NOT NULL);
CREATE TABLE dw.dim_priority_flag (PriorityFlagID INT IDENTITY(1,1) PRIMARY KEY, PriorityFlag BIT NOT NULL);

PRINT 'Dimension tables created';

-----------------------------------------------------
-- STEP 3: CREATE FACT TABLES
-----------------------------------------------------
CREATE TABLE dw.fact_sales
(
    SalesID INT NOT NULL,
    DeliveryID INT NOT NULL,
    DateKey INT NOT NULL,
    ProductTypeID INT NOT NULL,
    RegionID INT NOT NULL,
    UnitsSold INT NOT NULL,
    SalesAmount DECIMAL(18,2) NOT NULL,
    CONSTRAINT FK_fact_sales_ProductType FOREIGN KEY (ProductTypeID) REFERENCES dw.dim_product_type(ProductTypeID),
    CONSTRAINT FK_fact_sales_Region FOREIGN KEY (RegionID) REFERENCES dw.dim_region(RegionID),
    CONSTRAINT FK_fact_sales_Date FOREIGN KEY (DateKey) REFERENCES dw.dim_date(DateKey)
);

CREATE TABLE dw.fact_deliveries
(
    DeliveryID INT NOT NULL,
    RouteID INT NOT NULL,
    DriverID INT NOT NULL,
    ShipmentTypeID INT NOT NULL,
    DateKey INT NOT NULL,
    DeliveryStatusID INT NOT NULL,
    PriorityFlagID INT NOT NULL,
    CONSTRAINT FK_fact_deliveries_Route FOREIGN KEY (RouteID) REFERENCES dw.dim_route(RouteID),
    CONSTRAINT FK_fact_deliveries_Driver FOREIGN KEY (DriverID) REFERENCES dw.dim_driver(DriverID),
    CONSTRAINT FK_fact_deliveries_Shipment FOREIGN KEY (ShipmentTypeID) REFERENCES dw.dim_shipment_type(ShipmentTypeID),
    CONSTRAINT FK_fact_deliveries_Date FOREIGN KEY (DateKey) REFERENCES dw.dim_date(DateKey),
    CONSTRAINT FK_fact_deliveries_Status FOREIGN KEY (DeliveryStatusID) REFERENCES dw.dim_delivery_status(DeliveryStatusID),
    CONSTRAINT FK_fact_deliveries_Priority FOREIGN KEY (PriorityFlagID) REFERENCES dw.dim_priority_flag(PriorityFlagID)
);

CREATE TABLE dw.fact_routes
(
    RouteID INT NOT NULL,
    DriverID INT NOT NULL,
    PlannedStops INT NOT NULL,
    ActualStops INT NOT NULL,
    PlannedHours DECIMAL(10,2) NOT NULL,
    ActualHours DECIMAL(10,2) NOT NULL,
    RegionID INT NOT NULL,
    CONSTRAINT FK_fact_routes_Route FOREIGN KEY (RouteID) REFERENCES dw.dim_route(RouteID),
    CONSTRAINT FK_fact_routes_Driver FOREIGN KEY (DriverID) REFERENCES dw.dim_driver(DriverID),
    CONSTRAINT FK_fact_routes_Region FOREIGN KEY (RegionID) REFERENCES dw.dim_region(RegionID)
);

CREATE TABLE dw.fact_exceptions
(
    ExceptionID INT NOT NULL,
    DeliveryID INT NOT NULL,
    ExceptionTypeID INT NOT NULL,
    DateKey INT NOT NULL,
    ResolutionTimeHours DECIMAL(10,2) NULL,
    PriorityFlagID INT NOT NULL,
    RegionID INT NOT NULL,
    CONSTRAINT FK_fact_exceptions_ExceptionType FOREIGN KEY (ExceptionTypeID) REFERENCES dw.dim_exception_type(ExceptionTypeID),
    CONSTRAINT FK_fact_exceptions_Date FOREIGN KEY (DateKey) REFERENCES dw.dim_date(DateKey),
    CONSTRAINT FK_fact_exceptions_Priority FOREIGN KEY (PriorityFlagID) REFERENCES dw.dim_priority_flag(PriorityFlagID),
    CONSTRAINT FK_fact_exceptions_Region FOREIGN KEY (RegionID) REFERENCES dw.dim_region(RegionID)
);

PRINT 'Fact tables created';

-----------------------------------------------------
-- STEP 4: LOAD DIMENSIONS DYNAMICALLY
-----------------------------------------------------
-- ProductType dimension
INSERT INTO dw.dim_product_type (ProductType)
SELECT DISTINCT ProductType FROM clean.vw_sales
UNION
SELECT DISTINCT ProductType FROM clean.vw_deliveries;

-- Region dimension
INSERT INTO dw.dim_region (Region)
SELECT DISTINCT Region FROM clean.vw_sales
UNION
SELECT DISTINCT Region FROM clean.vw_deliveries
UNION
SELECT DISTINCT Region FROM clean.vw_exceptions
UNION
SELECT DISTINCT Region FROM clean.vw_routes;

-- Driver dimension
INSERT INTO dw.dim_driver (DriverName)
SELECT DISTINCT DriverID FROM clean.vw_deliveries
UNION
SELECT DISTINCT DriverID FROM clean.vw_routes;

-- Route dimension
INSERT INTO dw.dim_route (RouteName)
SELECT DISTINCT RouteID FROM clean.vw_deliveries
UNION
SELECT DISTINCT RouteID FROM clean.vw_routes;

-- ShipmentType dimension
INSERT INTO dw.dim_shipment_type (ShipmentType)
SELECT DISTINCT ShipmentType FROM clean.vw_deliveries;

-- DeliveryStatus dimension
INSERT INTO dw.dim_delivery_status (DeliveryStatus)
SELECT DISTINCT DeliveryStatus FROM clean.vw_deliveries;

-- ExceptionType dimension
INSERT INTO dw.dim_exception_type (ExceptionType)
SELECT DISTINCT ExceptionType FROM clean.vw_exceptions;

-- PriorityFlag dimension
INSERT INTO dw.dim_priority_flag (PriorityFlag)
SELECT DISTINCT PriorityFlag FROM clean.vw_deliveries
UNION
SELECT DISTINCT PriorityFlag FROM clean.vw_exceptions;

-- DimDate (collect dates from all clean views)
INSERT INTO dw.dim_date (DateKey, FullDate, [Year], Quarter, [Month], [Day], Weekday, IsWeekend, MonthName, IsHoliday, DayName, WeekOfYear, MonthYear, YearMonth, FiscalYear)
SELECT DISTINCT
    DateKey,
    CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE),
    DATEPART(YEAR, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(QUARTER, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(MONTH, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(DAY, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(WEEKDAY, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    CASE WHEN DATEPART(WEEKDAY, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)) IN (1,7) THEN 1 ELSE 0 END,
    DATENAME(MONTH, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    0, -- IsHoliday placeholder
    DATENAME(WEEKDAY, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(WEEK, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    RIGHT(CONVERT(VARCHAR(8), DateKey, 112),4)+'-'+LEFT(CONVERT(VARCHAR(8), DateKey, 112),2),
    LEFT(CONVERT(VARCHAR(8), DateKey, 112),6),
    DATEPART(YEAR, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE))
FROM (
    SELECT DateKey FROM clean.vw_sales
    UNION
    SELECT DateKey FROM clean.vw_deliveries
    UNION
    SELECT DateKey FROM clean.vw_exceptions
    UNION
    SELECT DateKey FROM clean.vw_routes
) AS all_dates;

PRINT 'Dimensions loaded successfully';

-----------------------------------------------------
-- STEP 5: LOAD FACTS USING SURROGATE KEYS
-----------------------------------------------------
-- Fact Sales
INSERT INTO dw.fact_sales (SalesID, DeliveryID, DateKey, ProductTypeID, RegionID, UnitsSold, SalesAmount)
SELECT
    s.SalesID,
    s.DeliveryID,
    dd.DateKey,
    dp.ProductTypeID,
    dr.RegionID,
    s.UnitsSold,
    s.SalesAmount
FROM clean.vw_sales s
JOIN dw.dim_product_type dp ON s.ProductType = dp.ProductType
JOIN dw.dim_region dr ON s.Region = dr.Region
JOIN dw.dim_date dd ON s.DateKey = dd.DateKey;

-- Fact Deliveries
INSERT INTO dw.fact_deliveries (DeliveryID, RouteID, DriverID, ShipmentTypeID, DateKey, DeliveryStatusID, PriorityFlagID)
SELECT
    d.DeliveryID,
    r.RouteID,
    drv.DriverID,
    st.ShipmentTypeID,
    dd.DateKey,
    ds.DeliveryStatusID,
    pf.PriorityFlagID
FROM clean.vw_deliveries d
JOIN dw.dim_route r ON d.RouteID = r.RouteName
JOIN dw.dim_driver drv ON d.DriverID = drv.DriverName
JOIN dw.dim_shipment_type st ON d.ShipmentType = st.ShipmentType
JOIN dw.dim_delivery_status ds ON d.DeliveryStatus = ds.DeliveryStatus
JOIN dw.dim_priority_flag pf ON d.PriorityFlag = pf.PriorityFlag
JOIN dw.dim_date dd ON CONVERT(INT, CONVERT(VARCHAR(8), d.DeliveryDate, 112)) = dd.DateKey;

-- Fact Routes
INSERT INTO dw.fact_routes (RouteID, DriverID, PlannedStops, ActualStops, PlannedHours, ActualHours, RegionID)
SELECT
    r.RouteID,
    drv.DriverID,
    rt.PlannedStops,
    rt.ActualStops,
    rt.PlannedHours,
    rt.ActualHours,
    rg.RegionID
FROM clean.vw_routes rt
JOIN dw.dim_route r ON rt.RouteID = r.RouteName
JOIN dw.dim_driver drv ON rt.DriverID = drv.DriverName
JOIN dw.dim_region rg ON rt.Region = rg.Region;

-- Fact Exceptions
INSERT INTO dw.fact_exceptions (ExceptionID, DeliveryID, ExceptionTypeID, DateKey, ResolutionTimeHours, PriorityFlagID, RegionID)
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
JOIN dw.dim_priority_flag pf ON e.PriorityFlag = pf.PriorityFlag
JOIN dw.dim_region rg ON e.Region = rg.Region
JOIN dw.dim_date dd ON CONVERT(INT, CONVERT(VARCHAR(8), e.DateReported, 112)) = dd.DateKey;

PRINT 'Fact tables loaded successfully';
PRINT '--- DW LOAD COMPLETE ---';


/*
-----------------------------------------------------
-- STEP 1: DROP EXISTING DW TABLES (OPTIONAL)
-- Purpose: Allows full reload of DW for testing or rebuilds
-----------------------------------------------------
IF OBJECT_ID('dw.fact_sales', 'U') IS NOT NULL DROP TABLE dw.fact_sales;
IF OBJECT_ID('dw.fact_deliveries', 'U') IS NOT NULL DROP TABLE dw.fact_deliveries;
IF OBJECT_ID('dw.fact_routes', 'U') IS NOT NULL DROP TABLE dw.fact_routes;
IF OBJECT_ID('dw.fact_exceptions', 'U') IS NOT NULL DROP TABLE dw.fact_exceptions;

IF OBJECT_ID('dw.dim_date', 'U') IS NOT NULL DROP TABLE dw.dim_date;
IF OBJECT_ID('dw.dim_delivery_status', 'U') IS NOT NULL DROP TABLE dw.dim_delivery_status;
IF OBJECT_ID('dw.dim_driver', 'U') IS NOT NULL DROP TABLE dw.dim_driver;
IF OBJECT_ID('dw.dim_exception_type', 'U') IS NOT NULL DROP TABLE dw.dim_exception_type;
IF OBJECT_ID('dw.dim_priority_flag', 'U') IS NOT NULL DROP TABLE dw.dim_priority_flag;
IF OBJECT_ID('dw.dim_product_type', 'U') IS NOT NULL DROP TABLE dw.dim_product_type;
IF OBJECT_ID('dw.dim_region', 'U') IS NOT NULL DROP TABLE dw.dim_region;
IF OBJECT_ID('dw.dim_route', 'U') IS NOT NULL DROP TABLE dw.dim_route;
IF OBJECT_ID('dw.dim_shipment_type', 'U') IS NOT NULL DROP TABLE dw.dim_shipment_type;

PRINT 'Dropped old DW tables if they existed';

-----------------------------------------------------
-- STEP 2: CREATE DIMENSION TABLES
-- Purpose: Build lookup tables with surrogate keys
-----------------------------------------------------
CREATE TABLE dw.dim_date
(
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL,
    [Year] INT,
    Quarter INT,
    [Month] INT,
    [Day] INT,
    Weekday INT,
    IsWeekend BIT,
    MonthName NVARCHAR(20),
    IsHoliday BIT,
    DayName NVARCHAR(20),
    WeekOfYear INT,
    MonthYear NVARCHAR(7),
    YearMonth NVARCHAR(7),
    FiscalYear INT
);

CREATE TABLE dw.dim_product_type (ProductTypeID INT IDENTITY(1,1) PRIMARY KEY, ProductType NVARCHAR(100) NOT NULL);
CREATE TABLE dw.dim_region (RegionID INT IDENTITY(1,1) PRIMARY KEY, Region NVARCHAR(50) NOT NULL);
CREATE TABLE dw.dim_driver (DriverID INT IDENTITY(1,1) PRIMARY KEY, DriverName NVARCHAR(100) NOT NULL);
CREATE TABLE dw.dim_route (RouteID INT IDENTITY(1,1) PRIMARY KEY, RouteName NVARCHAR(100) NOT NULL);
CREATE TABLE dw.dim_shipment_type (ShipmentTypeID INT IDENTITY(1,1) PRIMARY KEY, ShipmentType NVARCHAR(50) NOT NULL);
CREATE TABLE dw.dim_delivery_status (DeliveryStatusID INT IDENTITY(1,1) PRIMARY KEY, DeliveryStatus NVARCHAR(50) NOT NULL);
CREATE TABLE dw.dim_exception_type (ExceptionTypeID INT IDENTITY(1,1) PRIMARY KEY, ExceptionType NVARCHAR(100) NOT NULL);
CREATE TABLE dw.dim_priority_flag (PriorityFlagID INT IDENTITY(1,1) PRIMARY KEY, PriorityFlag BIT NOT NULL);

PRINT 'Dimension tables created';

-----------------------------------------------------
-- STEP 3: CREATE FACT TABLES
-- Purpose: Fact tables reference surrogate keys
-----------------------------------------------------
CREATE TABLE dw.fact_sales
(
    SalesID INT NOT NULL,
    DeliveryID INT NOT NULL,
    DateKey INT NOT NULL,
    ProductTypeID INT NOT NULL,
    RegionID INT NOT NULL,
    UnitsSold INT NOT NULL,
    SalesAmount DECIMAL(18,2) NOT NULL,
    CONSTRAINT FK_fact_sales_ProductType FOREIGN KEY (ProductTypeID) REFERENCES dw.dim_product_type(ProductTypeID),
    CONSTRAINT FK_fact_sales_Region FOREIGN KEY (RegionID) REFERENCES dw.dim_region(RegionID),
    CONSTRAINT FK_fact_sales_Date FOREIGN KEY (DateKey) REFERENCES dw.dim_date(DateKey)
);

CREATE TABLE dw.fact_deliveries
(
    DeliveryID INT NOT NULL,
    RouteID INT NOT NULL,
    DriverID INT NOT NULL,
    ShipmentTypeID INT NOT NULL,
    DateKey INT NOT NULL,
    DeliveryStatusID INT NOT NULL,
    PriorityFlagID INT NOT NULL,
    CONSTRAINT FK_fact_deliveries_Route FOREIGN KEY (RouteID) REFERENCES dw.dim_route(RouteID),
    CONSTRAINT FK_fact_deliveries_Driver FOREIGN KEY (DriverID) REFERENCES dw.dim_driver(DriverID),
    CONSTRAINT FK_fact_deliveries_Shipment FOREIGN KEY (ShipmentTypeID) REFERENCES dw.dim_shipment_type(ShipmentTypeID),
    CONSTRAINT FK_fact_deliveries_Date FOREIGN KEY (DateKey) REFERENCES dw.dim_date(DateKey),
    CONSTRAINT FK_fact_deliveries_Status FOREIGN KEY (DeliveryStatusID) REFERENCES dw.dim_delivery_status(DeliveryStatusID),
    CONSTRAINT FK_fact_deliveries_Priority FOREIGN KEY (PriorityFlagID) REFERENCES dw.dim_priority_flag(PriorityFlagID)
);

CREATE TABLE dw.fact_routes
(
    RouteID INT NOT NULL,
    DriverID INT NOT NULL,
    PlannedStops INT NOT NULL,
    ActualStops INT NOT NULL,
    PlannedHours DECIMAL(10,2) NOT NULL,
    ActualHours DECIMAL(10,2) NOT NULL,
    RegionID INT NOT NULL,
    CONSTRAINT FK_fact_routes_Route FOREIGN KEY (RouteID) REFERENCES dw.dim_route(RouteID),
    CONSTRAINT FK_fact_routes_Driver FOREIGN KEY (DriverID) REFERENCES dw.dim_driver(DriverID),
    CONSTRAINT FK_fact_routes_Region FOREIGN KEY (RegionID) REFERENCES dw.dim_region(RegionID)
);

CREATE TABLE dw.fact_exceptions
(
    ExceptionID INT NOT NULL,
    DeliveryID INT NOT NULL,
    ExceptionTypeID INT NOT NULL,
    DateKey INT NOT NULL,
    ResolutionTimeHours DECIMAL(10,2) NULL,
    PriorityFlagID INT NOT NULL,
    RegionID INT NOT NULL,
    CONSTRAINT FK_fact_exceptions_ExceptionType FOREIGN KEY (ExceptionTypeID) REFERENCES dw.dim_exception_type(ExceptionTypeID),
    CONSTRAINT FK_fact_exceptions_Date FOREIGN KEY (DateKey) REFERENCES dw.dim_date(DateKey),
    CONSTRAINT FK_fact_exceptions_Priority FOREIGN KEY (PriorityFlagID) REFERENCES dw.dim_priority_flag(PriorityFlagID),
    CONSTRAINT FK_fact_exceptions_Region FOREIGN KEY (RegionID) REFERENCES dw.dim_region(RegionID)
);
PRINT 'Fact tables created';

-----------------------------------------------------
-- STEP 4: LOAD DIMENSIONS FROM CLEAN VIEWS
-- Purpose: Create surrogate key mappings
-----------------------------------------------------

-- ProductType
INSERT INTO dw.dim_product_type (ProductType)
SELECT DISTINCT ProductType FROM clean.vw_sales;

-- Region
INSERT INTO dw.dim_region (Region)
SELECT DISTINCT Region FROM clean.vw_sales
UNION
SELECT DISTINCT Region FROM clean.vw_deliveries
UNION
SELECT DISTINCT Region FROM clean.vw_exceptions
UNION
SELECT DISTINCT Region FROM clean.vw_routes;

-- Driver
INSERT INTO dw.dim_driver (DriverName)
SELECT DISTINCT DriverID FROM clean.vw_deliveries
UNION
SELECT DISTINCT DriverID FROM clean.vw_routes;

-- Route
INSERT INTO dw.dim_route (RouteName)
SELECT DISTINCT RouteID FROM clean.vw_deliveries
UNION
SELECT DISTINCT RouteID FROM clean.vw_routes;

-- ShipmentType
INSERT INTO dw.dim_shipment_type (ShipmentType)
SELECT DISTINCT ShipmentType FROM clean.vw_deliveries;

-- DeliveryStatus
INSERT INTO dw.dim_delivery_status (DeliveryStatus)
SELECT DISTINCT DeliveryStatus FROM clean.vw_deliveries;

-- ExceptionType
INSERT INTO dw.dim_exception_type (ExceptionType)
SELECT DISTINCT ExceptionType FROM clean.vw_exceptions;

-- PriorityFlag
INSERT INTO dw.dim_priority_flag (PriorityFlag)
SELECT DISTINCT PriorityFlag FROM clean.vw_deliveries
UNION
SELECT DISTINCT PriorityFlag FROM clean.vw_exceptions;

-- DimDate
INSERT INTO dw.dim_date (DateKey, FullDate, [Year], Quarter, [Month], [Day], Weekday, IsWeekend, MonthName, IsHoliday, DayName, WeekOfYear, MonthYear, YearMonth, FiscalYear)
SELECT DISTINCT
    DateKey,
    CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE),
    DATEPART(YEAR, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(QUARTER, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(MONTH, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(DAY, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(WEEKDAY, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    CASE WHEN DATEPART(WEEKDAY, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)) IN (1,7) THEN 1 ELSE 0 END,
    DATENAME(MONTH, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    0, -- IsHoliday placeholder
    DATENAME(WEEKDAY, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    DATEPART(WEEK, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE)),
    RIGHT(CONVERT(VARCHAR(8), DateKey, 112),4)+'-'+LEFT(CONVERT(VARCHAR(8), DateKey, 112),2),
    LEFT(CONVERT(VARCHAR(8), DateKey, 112),6),
    DATEPART(YEAR, CAST(CONVERT(VARCHAR(8), DateKey, 112) AS DATE))
FROM clean.vw_sales;

PRINT 'Dimensions loaded';

-----------------------------------------------------
-- STEP 5: LOAD FACT TABLES USING SURROGATE KEYS
-- Purpose: Maintain referential integrity
-----------------------------------------------------

-- Fact Sales
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
JOIN dw.dim_product_type dp ON s.ProductType = dp.ProductType
JOIN dw.dim_region dr ON s.Region = dr.Region
JOIN dw.dim_date dd ON s.DateKey = dd.DateKey;

-- Fact Deliveries
INSERT INTO dw.fact_deliveries
(DeliveryID, RouteID, DriverID, ShipmentTypeID, DateKey, DeliveryStatusID, PriorityFlagID)
SELECT
    d.DeliveryID,
    r.RouteID,
    drv.DriverID,
    st.ShipmentTypeID,
    dd.DateKey,
    ds.DeliveryStatusID,
    pf.PriorityFlagID
FROM clean.vw_deliveries d
JOIN dw.dim_route r ON d.RouteID = r.RouteName
JOIN dw.dim_driver drv ON d.DriverID = drv.DriverName
JOIN dw.dim_shipment_type st ON d.ShipmentType = st.ShipmentType
JOIN dw.dim_delivery_status ds ON d.DeliveryStatus = ds.DeliveryStatus
JOIN dw.dim_priority_flag pf ON d.PriorityFlag = pf.PriorityFlag
JOIN dw.dim_date dd ON CONVERT(INT, CONVERT(VARCHAR(8), d.DeliveryDate, 112)) = dd.DateKey;

-- Fact Routes
INSERT INTO dw.fact_routes
(RouteID, DriverID, PlannedStops, ActualStops, PlannedHours, ActualHours, RegionID)
SELECT
    r.RouteID,
    drv.DriverID,
    rt.PlannedStops,
    rt.ActualStops,
    rt.PlannedHours,
    rt.ActualHours,
    rg.RegionID
FROM clean.vw_routes rt
JOIN dw.dim_route r ON rt.RouteID = r.RouteName
JOIN dw.dim_driver drv ON rt.DriverID = drv.DriverName
JOIN dw.dim_region rg ON rt.Region = rg.Region;

-- Fact Exceptions
INSERT INTO dw.fact_exceptions
(ExceptionID, DeliveryID, ExceptionTypeID, DateKey, ResolutionTimeHours, PriorityFlagID, RegionID)
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
JOIN dw.dim_priority_flag pf ON e.PriorityFlag = pf.PriorityFlag
JOIN dw.dim_region rg ON e.Region = rg.Region
JOIN dw.dim_date dd ON CONVERT(INT, CONVERT(VARCHAR(8), e.DateReported, 112)) = dd.DateKey;

PRINT 'Fact tables loaded successfully';
PRINT 'DW Load Complete';
*/