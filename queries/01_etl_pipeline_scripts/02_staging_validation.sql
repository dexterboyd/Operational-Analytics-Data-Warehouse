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