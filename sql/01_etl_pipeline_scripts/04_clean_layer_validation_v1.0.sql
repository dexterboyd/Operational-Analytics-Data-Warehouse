/*==============================================================
  CLEAN LAYER VALIDATION
  Purpose:
      Validate cleansed data before DW loading.

  Checks Performed:
      1. Row count comparison (staging vs clean)
      2. Required field NULL validation
      3. Business rule verification
      4. Data profiling metrics
==============================================================*/

--------------------------------------------------------------
-- ROW COUNT VALIDATION
--------------------------------------------------------------

PRINT '===== ROW COUNT VALIDATION =====';

SELECT 'Sales' AS TableName,
       (SELECT COUNT(*) FROM staging.staging_sales) AS StagingRows,
       (SELECT COUNT(*) FROM clean.vw_sales) AS CleanRows;

SELECT 'Deliveries' AS TableName,
       (SELECT COUNT(*) FROM staging.staging_deliveries),
       (SELECT COUNT(*) FROM clean.vw_deliveries);

SELECT 'Exceptions' AS TableName,
       (SELECT COUNT(*) FROM staging.staging_exceptions),
       (SELECT COUNT(*) FROM clean.vw_exceptions);

SELECT 'Routes' AS TableName,
       (SELECT COUNT(*) FROM staging.staging_routes),
       (SELECT COUNT(*) FROM clean.vw_routes);

--------------------------------------------------------------
-- REQUIRED FIELD NULL CHECKS
--------------------------------------------------------------

PRINT '===== NULL VALIDATION =====';

-- Sales Critical Fields
SELECT *
FROM clean.vw_sales
WHERE DateKey IS NULL
   OR SalesAmount IS NULL
   OR UnitsSold IS NULL;

-- Deliveries Critical Fields
SELECT *
FROM clean.vw_deliveries
WHERE DeliveryID IS NULL
   OR RouteID IS NULL
   OR DriverID IS NULL;

-- Exceptions Integrity
SELECT *
FROM clean.vw_exceptions
WHERE ExceptionID IS NULL
   OR DeliveryID IS NULL;

--------------------------------------------------------------
-- BUSINESS RULE VALIDATION
--------------------------------------------------------------

PRINT '===== BUSINESS RULE CHECKS =====';

-- Late deliveries correctly flagged
SELECT TOP 20 *
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'Late';

-- Priority normalization check
SELECT PriorityFlag, COUNT(*) AS RecordCount
FROM clean.vw_deliveries
GROUP BY PriorityFlag;

--------------------------------------------------------------
-- DATA PROFILING — SALES
--------------------------------------------------------------

PRINT '===== SALES DATA PROFILE =====';

SELECT
    MIN(SalesAmount) AS MinSales,
    MAX(SalesAmount) AS MaxSales,
    AVG(SalesAmount) AS AvgSales,
    SUM(SalesAmount) AS TotalSales,
    COUNT(*) AS RecordCount
FROM clean.vw_sales;

--------------------------------------------------------------
-- DATA PROFILING — REGION DISTRIBUTION
--------------------------------------------------------------

PRINT '===== REGION DISTRIBUTION =====';

SELECT Region,
       COUNT(*) AS RecordCount
FROM clean.vw_sales
GROUP BY Region
ORDER BY RecordCount DESC;

--------------------------------------------------------------
-- ROUTE PERFORMANCE VALIDATION
--------------------------------------------------------------

PRINT '===== ROUTE VALIDATION =====';

SELECT
    MIN(ActualStops) AS MinStops,
    MAX(ActualStops) AS MaxStops,
    AVG(ActualHours) AS AvgHours
FROM clean.vw_routes;

--------------------------------------------------------------
-- VALIDATION SUMMARY CHECK
--------------------------------------------------------------

PRINT '===== VALIDATION COMPLETE =====';
PRINT 'Review result sets before DW load.';