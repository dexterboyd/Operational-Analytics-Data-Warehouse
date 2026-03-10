-- =====================================================
-- CONFIGURATION: ENABLE / DISABLE STAGING VALIDATION
-- =====================================================
-- 1 = Run validation checks
-- 0 = Skip validation
-- This allows validation to be turned off for faster loads
-- in production environments if needed.

DECLARE @EnableStagingValidation BIT = 1;

-- =====================================================
-- STAGING → CLEAN VALIDATION
-- Purpose:
-- Validate raw staging data before transformations
-- are applied in the Clean layer.
-- =====================================================

IF @EnableStagingValidation = 1
BEGIN

PRINT '=====================================';
PRINT 'RUNNING STAGING LAYER VALIDATION';
PRINT '=====================================';

-- =====================================================
-- 1. ROW COUNT VALIDATION
-- Ensures data was loaded into staging tables
-- =====================================================

PRINT 'Checking row counts...';

SELECT 'staging_sales' AS TableName, COUNT(*) AS RowCount
FROM staging.staging_sales

UNION ALL

SELECT 'staging_deliveries', COUNT(*)
FROM staging.staging_deliveries

UNION ALL

SELECT 'staging_routes', COUNT(*)
FROM staging.staging_routes

UNION ALL

SELECT 'staging_exceptions', COUNT(*)
FROM staging.staging_exceptions;

-- Automatic warnings for empty tables

IF (SELECT COUNT(*) FROM staging.staging_sales) = 0
    RAISERROR('WARNING: staging_sales table contains ZERO rows.',10,1);

IF (SELECT COUNT(*) FROM staging.staging_deliveries) = 0
    RAISERROR('WARNING: staging_deliveries table contains ZERO rows.',10,1);

IF (SELECT COUNT(*) FROM staging.staging_routes) = 0
    RAISERROR('WARNING: staging_routes table contains ZERO rows.',10,1);

IF (SELECT COUNT(*) FROM staging.staging_exceptions) = 0
    RAISERROR('WARNING: staging_exceptions table contains ZERO rows.',10,1);

-- =====================================================
-- 2. CRITICAL NULL VALUE CHECKS
-- Ensures primary identifiers exist before ETL
-- =====================================================

PRINT 'Checking NULL values in key columns...';

-- SALES VALIDATION
SELECT 
    'staging_sales' AS TableName,
    SUM(CASE WHEN SalesID IS NULL THEN 1 ELSE 0 END) AS NullSalesID,
    SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID,
    SUM(CASE WHEN UnitsSold IS NULL THEN 1 ELSE 0 END) AS NullUnitsSold,
    SUM(CASE WHEN SalesAmount IS NULL THEN 1 ELSE 0 END) AS NullSalesAmount
FROM staging.staging_sales;

IF EXISTS (SELECT 1 FROM staging.staging_sales WHERE SalesID IS NULL)
    RAISERROR('WARNING: staging_sales contains NULL SalesID values.',10,1);

IF EXISTS (SELECT 1 FROM staging.staging_sales WHERE DeliveryID IS NULL)
    RAISERROR('WARNING: staging_sales contains NULL DeliveryID values.',10,1);

-- DELIVERIES VALIDATION
SELECT
    'staging_deliveries' AS TableName,
    SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID,
    SUM(CASE WHEN RouteID IS NULL THEN 1 ELSE 0 END) AS NullRouteID,
    SUM(CASE WHEN DriverID IS NULL THEN 1 ELSE 0 END) AS NullDriverID
FROM staging.staging_deliveries;

IF EXISTS (SELECT 1 FROM staging.staging_deliveries WHERE DeliveryID IS NULL)
    RAISERROR('WARNING: staging_deliveries contains NULL DeliveryID values.',10,1);

-- ROUTES VALIDATION
SELECT
    'staging_routes' AS TableName,
    SUM(CASE WHEN RouteID IS NULL THEN 1 ELSE 0 END) AS NullRouteID,
    SUM(CASE WHEN DriverID IS NULL THEN 1 ELSE 0 END) AS NullDriverID
FROM staging.staging_routes;

IF EXISTS (SELECT 1 FROM staging.staging_routes WHERE RouteID IS NULL)
    RAISERROR('WARNING: staging_routes contains NULL RouteID values.',10,1);

-- EXCEPTIONS VALIDATION
SELECT
    'staging_exceptions' AS TableName,
    SUM(CASE WHEN ExceptionID IS NULL THEN 1 ELSE 0 END) AS NullExceptionID,
    SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID
FROM staging.staging_exceptions;

IF EXISTS (SELECT 1 FROM staging.staging_exceptions WHERE ExceptionID IS NULL)
    RAISERROR('WARNING: staging_exceptions contains NULL ExceptionID values.',10,1);

PRINT '=====================================';
PRINT 'STAGING VALIDATION COMPLETED';
PRINT '=====================================';
END

ELSE
BEGIN
PRINT 'STAGING VALIDATION SKIPPED (CONFIG DISABLED)';
END

/*
-- CONFIGURATION: ENABLE/DISABLE STAGING VALIDATION
-----------------------------------------------------
DECLARE @EnableStagingValidation BIT = 1; -- Set to 1 to run validation, 0 to skip

-----------------------------------------------------
-- STAGING → CLEAN VALIDATION
-- PRE-CLEANING VALIDATION
-- Controlled by @EnableStagingValidation
-----------------------------------------------------

IF @EnableStagingValidation = 1
BEGIN
    PRINT '--- STAGING LAYER VALIDATION ---';

    -- Row counts
    SELECT 'staging_sales' AS TableName, COUNT(*) AS [RowCount]
	FROM staging.staging_sales
    UNION ALL
    SELECT 'staging_deliveries', COUNT(*) FROM staging.staging_deliveries
    UNION ALL
    SELECT 'staging_routes', COUNT(*) FROM staging.staging_routes
    UNION ALL
    SELECT 'staging_exceptions', COUNT(*) FROM staging.staging_exceptions;

    -- Key NULL checks (example for critical columns)
    SELECT 'staging_sales' AS TableName,
           SUM(CASE WHEN SalesID IS NULL THEN 1 ELSE 0 END) AS NullSalesID,
           SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID,
           SUM(CASE WHEN UnitsSold IS NULL THEN 1 ELSE 0 END) AS NullUnitsSold,
           SUM(CASE WHEN SalesAmount IS NULL THEN 1 ELSE 0 END) AS NullSalesAmount
    FROM staging.staging_sales;

    SELECT 'staging_deliveries' AS TableName,
           SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID,
           SUM(CASE WHEN RouteID IS NULL THEN 1 ELSE 0 END) AS NullRouteID,
           SUM(CASE WHEN DriverID IS NULL THEN 1 ELSE 0 END) AS NullDriverID
    FROM staging.staging_deliveries;

    SELECT 'staging_routes' AS TableName,
           SUM(CASE WHEN RouteID IS NULL THEN 1 ELSE 0 END) AS NullRouteID,
           SUM(CASE WHEN DriverID IS NULL THEN 1 ELSE 0 END) AS NullDriverID
    FROM staging.staging_routes;

    SELECT 'staging_exceptions' AS TableName,
           SUM(CASE WHEN ExceptionID IS NULL THEN 1 ELSE 0 END) AS NullExceptionID,
           SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID
    FROM staging.staging_exceptions;
END
*/
