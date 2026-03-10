-- =========================================
-- CREATE DATA WAREHOUSE SCHEMA (IF NOT EXISTS)
-- =========================================
-- The DW schema contains dimension and fact tables
-- used for analytics and reporting.

IF SCHEMA_ID('dw') IS NULL
BEGIN
    EXEC('CREATE SCHEMA dw');
    PRINT 'Schema created: dw';
END
ELSE
BEGIN
    PRINT 'Schema already exists: dw';
END
GO

-- =========================================
-- DIMENSION TABLE: DATE
-- =========================================
-- Calendar dimension used for time-based analysis
-- across sales, deliveries, and exceptions.

IF OBJECT_ID('dw.dim_date','U') IS NULL
BEGIN
CREATE TABLE dw.dim_date (
    DateKey INT PRIMARY KEY,      -- Surrogate key in YYYYMMDD format
    FullDate DATE,                -- Actual calendar date
    Year INT,                     -- Year value
    Quarter INT,                  -- Quarter (1-4)
    Month INT,                    -- Month (1-12)
    Day INT,                      -- Day of month
    Weekday INT,                  -- Day of week (1-7)
    IsWeekend BIT                 -- Weekend indicator
);
END
GO

-- =========================================
-- DIMENSION TABLE: REGION
-- =========================================
-- Geographic region dimension used to segment
-- deliveries, routes, and sales.

IF OBJECT_ID('dw.dim_region','U') IS NULL
BEGIN
CREATE TABLE dw.dim_region (
    RegionKey INT PRIMARY KEY,    -- Surrogate key
    RegionName NVARCHAR(10)       -- Region name or code
);
END
GO

-- =========================================
-- DIMENSION TABLE: DRIVER
-- =========================================
-- Driver dimension representing delivery personnel.

IF OBJECT_ID('dw.dim_driver','U') IS NULL
BEGIN
CREATE TABLE dw.dim_driver (
    DriverKey INT PRIMARY KEY,    -- Surrogate key
    DriverName NVARCHAR(50)       -- Driver identifier or name
);
END
GO

-- =========================================
-- DIMENSION TABLE: ROUTE
-- =========================================
-- Route dimension used for analyzing delivery
-- route performance metrics.

IF OBJECT_ID('dw.dim_route','U') IS NULL
BEGIN
CREATE TABLE dw.dim_route (
    RouteKey INT PRIMARY KEY,     -- Surrogate key
    RouteID NVARCHAR(10)          -- Route identifier
);
END
GO

-- =========================================
-- DIMENSION TABLE: SHIPMENT TYPE
-- =========================================
-- Shipment classification used for delivery
-- performance analysis.

IF OBJECT_ID('dw.dim_shipment_type','U') IS NULL
BEGIN
CREATE TABLE dw.dim_shipment_type (
    ShipmentTypeKey INT PRIMARY KEY, 
    ShipmentType NVARCHAR(20)     -- Standard, Express, etc.
);
END
GO

-- =========================================
-- DIMENSION TABLE: EXCEPTION TYPE
-- =========================================
-- Describes operational issues that occur during
-- the delivery process.

IF OBJECT_ID('dw.dim_exception_type','U') IS NULL
BEGIN
CREATE TABLE dw.dim_exception_type (
    ExceptionTypeKey INT PRIMARY KEY,
    ExceptionType NVARCHAR(50)
);
END
GO

-- =========================================
-- DIMENSION TABLE: PRODUCT
-- =========================================
-- Product dimension used for sales analytics.

IF OBJECT_ID('dw.dim_product','U') IS NULL
BEGIN
CREATE TABLE dw.dim_product (
    ProductKey INT PRIMARY KEY,
    ProductType NVARCHAR(50)
);
END
GO

-- =========================================
-- DIMENSION TABLE: PRIORITY FLAG
-- =========================================
-- Indicates whether shipments or exceptions
-- are marked as high priority.

IF OBJECT_ID('dw.dim_priority_flag','U') IS NULL
BEGIN
CREATE TABLE dw.dim_priority_flag (
    PriorityFlagKey INT PRIMARY KEY,
    PriorityFlag BIT
);
END
GO

-- =====================================================
-- POPULATE DIMENSION TABLES
-- Data Source: Staging Layer
-- Purpose: Load cleaned distinct values into DW dimensions
-- =====================================================


-- =====================================================
-- DIMENSION: DATE
-- Generates a calendar table from 2023–2025
-- =====================================================

DECLARE @Date DATE = '2023-01-01';

WHILE @Date <= '2025-12-31'
BEGIN

IF NOT EXISTS (
    SELECT 1 FROM dw.dim_date 
    WHERE DateKey = CONVERT(INT, FORMAT(@Date,'yyyyMMdd'))
)

INSERT INTO dw.dim_date
(
    DateKey,
    FullDate,
    Year,
    Quarter,
    Month,
    Day,
    Weekday,
    IsWeekend
)
VALUES
(
    CONVERT(INT, FORMAT(@Date,'yyyyMMdd')),
    @Date,
    YEAR(@Date),
    DATEPART(QUARTER,@Date),
    MONTH(@Date),
    DAY(@Date),
    DATEPART(WEEKDAY,@Date),
    CASE WHEN DATEPART(WEEKDAY,@Date) IN (1,7) THEN 1 ELSE 0 END
);

SET @Date = DATEADD(DAY,1,@Date);

END;

-- =====================================================
-- DIMENSION: REGION
-- =====================================================

INSERT INTO dw.dim_region (RegionName)

SELECT DISTINCT
    LTRIM(RTRIM(Region))

FROM staging.staging_routes r

WHERE Region IS NOT NULL

AND NOT EXISTS
(
    SELECT 1
    FROM dw.dim_region d
    WHERE d.RegionName = LTRIM(RTRIM(r.Region))
);

-- =====================================================
-- DIMENSION: DRIVER
-- =====================================================

INSERT INTO dw.dim_driver (DriverName)

SELECT DISTINCT
    LTRIM(RTRIM(DriverID))

FROM staging.staging_deliveries d

WHERE DriverID IS NOT NULL

AND NOT EXISTS
(
    SELECT 1
    FROM dw.dim_driver dd
    WHERE dd.DriverName = LTRIM(RTRIM(d.DriverID))
);

-- =====================================================
-- DIMENSION: ROUTE
-- =====================================================

INSERT INTO dw.dim_route (RouteID)

SELECT DISTINCT
    LTRIM(RTRIM(RouteID))

FROM staging.staging_routes r

WHERE RouteID IS NOT NULL

AND NOT EXISTS
(
    SELECT 1
    FROM dw.dim_route dr
    WHERE dr.RouteID = LTRIM(RTRIM(r.RouteID))
);

-- =====================================================
-- DIMENSION: PRODUCT TYPE
-- =====================================================

INSERT INTO dw.dim_product (ProductType)

SELECT DISTINCT
    LTRIM(RTRIM(ProductType))

FROM staging.staging_sales s

WHERE ProductType IS NOT NULL

AND NOT EXISTS
(
    SELECT 1
    FROM dw.dim_product dp
    WHERE dp.ProductType = LTRIM(RTRIM(s.ProductType))
);

-- =====================================================
-- DIMENSION: SHIPMENT TYPE
-- =====================================================

INSERT INTO dw.dim_shipment_type (ShipmentType)

SELECT DISTINCT
    LTRIM(RTRIM(ShipmentType))

FROM staging.staging_deliveries d

WHERE ShipmentType IS NOT NULL

AND NOT EXISTS
(
    SELECT 1
    FROM dw.dim_shipment_type dst
    WHERE dst.ShipmentType = LTRIM(RTRIM(d.ShipmentType))
);

-- =====================================================
-- DIMENSION: DELIVERY STATUS
-- =====================================================

IF OBJECT_ID('dw.dim_delivery_status','U') IS NULL
BEGIN

CREATE TABLE dw.dim_delivery_status
(
    DeliveryStatusKey INT IDENTITY(1,1) PRIMARY KEY,
    DeliveryStatus NVARCHAR(100) NOT NULL
);

END

INSERT INTO dw.dim_delivery_status (DeliveryStatus)

SELECT DISTINCT
    LTRIM(RTRIM(DeliveryStatus))

FROM staging.staging_deliveries d

WHERE DeliveryStatus IS NOT NULL

AND NOT EXISTS
(
    SELECT 1
    FROM dw.dim_delivery_status dds
    WHERE dds.DeliveryStatus = LTRIM(RTRIM(d.DeliveryStatus))
);

-- =====================================================
-- DIMENSION: EXCEPTION TYPE
-- =====================================================

INSERT INTO dw.dim_exception_type (ExceptionType)

SELECT DISTINCT
    LTRIM(RTRIM(ExceptionType))

FROM staging.staging_exceptions e

WHERE ExceptionType IS NOT NULL

AND NOT EXISTS
(
    SELECT 1
    FROM dw.dim_exception_type det
    WHERE det.ExceptionType = LTRIM(RTRIM(e.ExceptionType))
);

-- =====================================================
-- DIMENSION: PRIORITY FLAG
-- =====================================================

INSERT INTO dw.dim_priority_flag (PriorityFlag)

SELECT DISTINCT
    PriorityFlag

FROM
(
    SELECT PriorityFlag FROM staging.staging_deliveries
    UNION
    SELECT PriorityFlag FROM staging.staging_exceptions
) pf

WHERE PriorityFlag IS NOT NULL

AND NOT EXISTS
(
    SELECT 1
    FROM dw.dim_priority_flag dpf
    WHERE dpf.PriorityFlag = pf.PriorityFlag
);


/*
-- =========================================
--  CREATE DIMENSION TABLES
-- =========================================

CREATE TABLE dim_date (
    DateKey INT PRIMARY KEY,
    FullDate DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    Weekday INT,
    IsWeekend BIT
);

CREATE TABLE dim_region (
    RegionKey INT PRIMARY KEY,
    RegionName NVARCHAR(10)
);

CREATE TABLE dim_driver (
    DriverKey INT PRIMARY KEY,
    DriverName NVARCHAR(50)
);

CREATE TABLE dim_route (
    RouteKey INT PRIMARY KEY,
    RouteID NVARCHAR(10)
);

CREATE TABLE dim_shipment_type (
    ShipmentTypeKey INT PRIMARY KEY,
    ShipmentType NVARCHAR(20)
);

CREATE TABLE dim_exception_type (
    ExceptionTypeKey INT PRIMARY KEY,
    ExceptionType NVARCHAR(50)
);

CREATE TABLE dim_product (
    ProductKey INT PRIMARY KEY,
    ProductType NVARCHAR(50)
);

CREATE TABLE dim_priority_flag (
    PriorityFlagKey INT PRIMARY KEY,
    PriorityFlag BIT
);

-- CHECK ROW COUNTS
---------------------------------------------
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dw.dim_date
UNION ALL
SELECT 'dim_delivery_status', COUNT(*) FROM dw.dim_delivery_status
UNION ALL
SELECT 'dim_driver', COUNT(*) FROM dw.dim_driver
UNION ALL
SELECT 'dim_exception_type', COUNT(*) FROM dw.dim_exception_type
UNION ALL
SELECT 'dim_priority_flag', COUNT(*) FROM dw.dim_priority_flag
UNION ALL
SELECT 'dim_product_type', COUNT(*) FROM dw.dim_product_type
UNION ALL
SELECT 'dim_region', COUNT(*) FROM dw.dim_region
UNION ALL
SELECT 'dim_route', COUNT(*) FROM dw.dim_route
UNION ALL
SELECT 'dim_shipment_type', COUNT(*) FROM dw.dim_shipment_type;

-- CHECK TABLE STRUCTURE
----------------------------------------------
SELECT * FROM dw.dim_date;
SELECT * FROM dw.dim_delivery_status;
SELECT * FROM dw.dim_driver;
SELECT * FROM dw.dim_exception_type;
SELECT * FROM dw.dim_priority_flag;
SELECT * FROM dw.dim_product_type;
SELECT * FROM dw.dim_region;
SELECT * FROM dw.dim_route;
SELECT * FROM dw.dim_shipment_type;
--------------------------------------------------------
*/
