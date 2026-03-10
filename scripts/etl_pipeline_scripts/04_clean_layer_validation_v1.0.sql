/*==============================================================
  CLEAN LAYER VALIDATION
  Schema: clean

  PURPOSE
  -------
  Validate cleansed datasets before loading them into the
  Data Warehouse (dw schema).

  This validation layer helps detect data quality issues
  that could cause ETL failures or inaccurate reporting.

  CHECKS PERFORMED
  ----------------
  1. Row count comparison (staging vs clean)
  2. Required field NULL validation
  3. Business rule verification
  4. Data profiling metrics

  PIPELINE FLOW
  -------------
  staging → clean → dw → reporting → Power BI
==============================================================*/



/*==============================================================
  1. ROW COUNT VALIDATION
  Purpose:
      Ensure that records successfully flowed from
      staging tables into clean views.

      Large row discrepancies may indicate filtering
      issues or bad source data.
==============================================================*/

PRINT '===== ROW COUNT VALIDATION =====';

SELECT
    'Sales' AS TableName,
    (SELECT COUNT(*) FROM staging.staging_sales) AS StagingRows,
    (SELECT COUNT(*) FROM clean.vw_sales) AS CleanRows;

SELECT
    'Deliveries' AS TableName,
    (SELECT COUNT(*) FROM staging.staging_deliveries) AS StagingRows,
    (SELECT COUNT(*) FROM clean.vw_deliveries) AS CleanRows;

SELECT
    'Exceptions' AS TableName,
    (SELECT COUNT(*) FROM staging.staging_exceptions) AS StagingRows,
    (SELECT COUNT(*) FROM clean.vw_exceptions) AS CleanRows;

SELECT
    'Routes' AS TableName,
    (SELECT COUNT(*) FROM staging.staging_routes) AS StagingRows,
    (SELECT COUNT(*) FROM clean.vw_routes) AS CleanRows;



/*==============================================================
  2. REQUIRED FIELD NULL CHECKS
  Purpose:
      Identify records missing critical identifiers
      that would break fact table loading or dimension joins.
==============================================================*/

PRINT '===== NULL VALIDATION =====';



-- SALES CRITICAL FIELD VALIDATION
SELECT *
FROM clean.vw_sales
WHERE DateKey IS NULL
   OR SalesAmount IS NULL
   OR UnitsSold IS NULL;



-- DELIVERY INTEGRITY CHECK
SELECT *
FROM clean.vw_deliveries
WHERE DeliveryID IS NULL
   OR RouteID IS NULL
   OR DriverID IS NULL;



-- EXCEPTION INTEGRITY CHECK
SELECT *
FROM clean.vw_exceptions
WHERE ExceptionID IS NULL
   OR DeliveryID IS NULL;



/*==============================================================
  3. BUSINESS RULE VALIDATION
  Purpose:
      Confirm that transformation rules in the clean layer
      were applied correctly.
==============================================================*/

PRINT '===== BUSINESS RULE CHECKS =====';



-- LATE DELIVERY RULE VALIDATION
-- Ensures deliveries past expected date are flagged properly
SELECT TOP 20 *
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'LATE';



-- PRIORITY FLAG NORMALIZATION
-- Confirms boolean normalization worked correctly
SELECT
    PriorityFlag,
    COUNT(*) AS RecordCount
FROM clean.vw_deliveries
GROUP BY PriorityFlag;



/*==============================================================
  4. DATA PROFILING — SALES METRICS
  Purpose:
      Provide quick statistical insight into cleaned sales data
      for anomaly detection.
==============================================================*/

PRINT '===== SALES DATA PROFILE =====';

SELECT
    MIN(SalesAmount) AS MinSales,
    MAX(SalesAmount) AS MaxSales,
    AVG(SalesAmount) AS AvgSales,
    SUM(SalesAmount) AS TotalSales,
    COUNT(*) AS RecordCount
FROM clean.vw_sales;



/*==============================================================
  5. DATA PROFILING — REGION DISTRIBUTION
  Purpose:
      Identify regional distribution patterns
      and detect unexpected region values.
==============================================================*/

PRINT '===== REGION DISTRIBUTION =====';

SELECT
    Region,
    COUNT(*) AS RecordCount
FROM clean.vw_sales
GROUP BY Region
ORDER BY RecordCount DESC;



/*==============================================================
  6. ROUTE PERFORMANCE VALIDATION
  Purpose:
      Ensure operational metrics are within reasonable ranges.
==============================================================*/

PRINT '===== ROUTE VALIDATION =====';

SELECT
    MIN(ActualStops) AS MinStops,
    MAX(ActualStops) AS MaxStops,
    AVG(ActualHours) AS AvgHours
FROM clean.vw_routes;



/*==============================================================
  VALIDATION SUMMARY
  Purpose:
      Final message confirming validation completed.
      Results should be reviewed before DW loading.
==============================================================*/

PRINT '===== VALIDATION COMPLETE =====';
PRINT 'Review result sets above before loading DW tables.';

/*
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
*/
