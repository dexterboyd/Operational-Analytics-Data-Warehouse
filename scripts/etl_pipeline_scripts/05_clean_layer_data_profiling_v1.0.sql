/*==============================================================
  CLEAN LAYER VALIDATION SCRIPT
  Schema: clean

  PURPOSE
  -------
  Validate cleansed views before loading into the Data Warehouse (DW).

  VALIDATION STEPS
  ----------------
  1. Row count checks (staging vs clean)
  2. Required field NULL checks
  3. Business rule validations
  4. Optional data profiling for insight
==============================================================*/

PRINT '--- CLEAN LAYER VALIDATION START ---';

/*==============================================================
  STEP 1: ROW COUNT CHECKS
  Purpose:
      Compare number of records in staging vs clean views to 
      detect filtering or ETL issues.
==============================================================*/

PRINT '--- STEP 1: ROW COUNT CHECKS ---';

-- Sales
SELECT 
    'vw_sales' AS ViewName,
    COUNT(*) AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_sales 
     WHERE UnitsSold > 0 AND SalesAmount > 0) AS StagingValidRowCount,
    COUNT(*) * 1.0 / NULLIF((SELECT COUNT(*) FROM staging.staging_sales 
                             WHERE UnitsSold > 0 AND SalesAmount > 0),0) AS PercentOfValidRows
FROM clean.vw_sales;

-- Deliveries
SELECT 
    'vw_deliveries' AS ViewName,
    COUNT(*) AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_deliveries) AS StagingRowCount
FROM clean.vw_deliveries;

-- Exceptions
SELECT 
    'vw_exceptions' AS ViewName,
    COUNT(*) AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_exceptions) AS StagingRowCount
FROM clean.vw_exceptions;

-- Routes
SELECT 
    'vw_routes' AS ViewName,
    COUNT(*) AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_routes
     WHERE PlannedStops >=0 AND ActualStops >=0 AND PlannedHours <>0 AND ActualHours>0) AS StagingValidRowCount
FROM clean.vw_routes;

/*==============================================================
  STEP 2: REQUIRED FIELD NULL CHECKS
  Purpose:
      Identify missing critical fields that may break DW load.
==============================================================*/

PRINT '--- STEP 2: REQUIRED FIELD NULL CHECKS ---';

-- Sales: required fields
SELECT COUNT(*) AS NullCount, 'vw_sales.DateKey' AS FieldName
FROM clean.vw_sales
WHERE DateKey IS NULL
UNION ALL
SELECT COUNT(*), 'vw_sales.SalesAmount'
FROM clean.vw_sales
WHERE SalesAmount IS NULL
UNION ALL
SELECT COUNT(*), 'vw_sales.UnitsSold'
FROM clean.vw_sales
WHERE UnitsSold IS NULL;

-- Deliveries: required fields
SELECT COUNT(*) AS NullCount, 'vw_deliveries.DeliveryDate' AS FieldName
FROM clean.vw_deliveries
WHERE DeliveryDate IS NULL
UNION ALL
SELECT COUNT(*), 'vw_deliveries.DeliveryStatus'
FROM clean.vw_deliveries
WHERE DeliveryStatus IS NULL
UNION ALL
SELECT COUNT(*), 'vw_deliveries.PriorityFlag'
FROM clean.vw_deliveries
WHERE PriorityFlag IS NULL;

-- Exceptions: required fields
SELECT COUNT(*) AS NullCount, 'vw_exceptions.ExceptionID' AS FieldName
FROM clean.vw_exceptions
WHERE ExceptionID IS NULL
UNION ALL
SELECT COUNT(*), 'vw_exceptions.ResolvedDate'
FROM clean.vw_exceptions
WHERE ResolvedDate IS NULL;

-- Routes: required fields
SELECT COUNT(*) AS NullCount, 'vw_routes.PlannedStops' AS FieldName
FROM clean.vw_routes
WHERE PlannedStops IS NULL
UNION ALL
SELECT COUNT(*), 'vw_routes.ActualStops'
FROM clean.vw_routes
WHERE ActualStops IS NULL
UNION ALL
SELECT COUNT(*), 'vw_routes.PlannedHours'
FROM clean.vw_routes
WHERE PlannedHours IS NULL
UNION ALL
SELECT COUNT(*), 'vw_routes.ActualHours'
FROM clean.vw_routes
WHERE ActualHours IS NULL;

/*==============================================================
  STEP 3: BUSINESS RULE VALIDATIONS
  Purpose:
      Confirm that transformations and business logic rules 
      were applied correctly in the clean layer.
==============================================================*/

PRINT '--- STEP 3: BUSINESS RULE VALIDATIONS ---';

-- Late deliveries flagged correctly
SELECT COUNT(*) AS IncorrectLateFlag
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'LATE';

-- Priority normalized to 0/1
SELECT COUNT(*) AS InvalidPriorityFlag
FROM clean.vw_deliveries
WHERE PriorityFlag NOT IN (0,1);

/*==============================================================
  STEP 4: OPTIONAL DATA PROFILING
  Purpose:
      Provide descriptive statistics and detect anomalies.
==============================================================*/

PRINT '--- STEP 4: DATA PROFILING ---';

-- Sales amount statistics by Region
SELECT 
    Region,
    COUNT(*) AS Transactions,
    SUM(SalesAmount) AS TotalSales,
    AVG(SalesAmount) AS AvgSale,
    MIN(SalesAmount) AS MinSale,
    MAX(SalesAmount) AS MaxSale
FROM clean.vw_sales
GROUP BY Region
ORDER BY Region;

-- UnitsSold distribution by ProductType
SELECT 
    ProductType,
    COUNT(*) AS Transactions,
    SUM(UnitsSold) AS TotalUnits,
    AVG(UnitsSold) AS AvgUnits,
    MIN(UnitsSold) AS MinUnits,
    MAX(UnitsSold) AS MaxUnits
FROM clean.vw_sales
GROUP BY ProductType
ORDER BY ProductType;

/*==============================================================
  VALIDATION COMPLETE
==============================================================*/

PRINT '--- CLEAN LAYER VALIDATION END ---';
PRINT 'Review all result sets carefully before loading DW tables.';

/*
-----------------------------------------------------
-- CLEAN LAYER VALIDATION SCRIPT
-- Validate clean views before loading into DW
-----------------------------------------------------

PRINT '--- CLEAN LAYER VALIDATION START ---';

-----------------------------------------------------
-- STEP 1: ROW COUNT CHECKS
-----------------------------------------------------

-- Sales
SELECT 
    'vw_sales' AS ViewName,
    COUNT(*) AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_sales 
     WHERE UnitsSold > 0 AND SalesAmount > 0) AS StagingValidRowCount,
    COUNT(*) * 1.0 / NULLIF((SELECT COUNT(*) FROM staging.staging_sales 
                             WHERE UnitsSold > 0 AND SalesAmount > 0),0) AS PercentOfValidRows
FROM clean.vw_sales;

-- Deliveries
SELECT 
    'vw_deliveries' AS ViewName,
    COUNT(*) AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_deliveries) AS StagingRowCount
FROM clean.vw_deliveries;

-- Exceptions
SELECT 
    'vw_exceptions' AS ViewName,
    COUNT(*) AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_exceptions) AS StagingRowCount
FROM clean.vw_exceptions;

-- Routes
SELECT 
    'vw_routes' AS ViewName,
    COUNT(*) AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_routes
     WHERE PlannedStops >=0 AND ActualStops >=0 AND PlannedHours <>0 AND ActualHours>0) AS StagingValidRowCount
FROM clean.vw_routes;

-----------------------------------------------------
-- STEP 2: NULL CHECKS ON REQUIRED FIELDS
-----------------------------------------------------

-- Sales: required fields
SELECT COUNT(*) AS NullCount, 'vw_sales.DateKey' AS FieldName
FROM clean.vw_sales
WHERE DateKey IS NULL
UNION ALL
SELECT COUNT(*), 'vw_sales.SalesAmount'
FROM clean.vw_sales
WHERE SalesAmount IS NULL
UNION ALL
SELECT COUNT(*), 'vw_sales.UnitsSold'
FROM clean.vw_sales
WHERE UnitsSold IS NULL;

-- Deliveries: required fields
SELECT COUNT(*) AS NullCount, 'vw_deliveries.DeliveryDate' AS FieldName
FROM clean.vw_deliveries
WHERE DeliveryDate IS NULL
UNION ALL
SELECT COUNT(*), 'vw_deliveries.DeliveryStatus'
FROM clean.vw_deliveries
WHERE DeliveryStatus IS NULL
UNION ALL
SELECT COUNT(*), 'vw_deliveries.PriorityFlag'
FROM clean.vw_deliveries
WHERE PriorityFlag IS NULL;

-- Exceptions: required fields
SELECT COUNT(*) AS NullCount, 'vw_exceptions.ExceptionID' AS FieldName
FROM clean.vw_exceptions
WHERE ExceptionID IS NULL
UNION ALL
SELECT COUNT(*), 'vw_exceptions.ResolvedDate'
FROM clean.vw_exceptions
WHERE ResolvedDate IS NULL;

-- Routes: required fields
SELECT COUNT(*) AS NullCount, 'vw_routes.PlannedStops' AS FieldName
FROM clean.vw_routes
WHERE PlannedStops IS NULL
UNION ALL
SELECT COUNT(*), 'vw_routes.ActualStops'
FROM clean.vw_routes
WHERE ActualStops IS NULL
UNION ALL
SELECT COUNT(*), 'vw_routes.PlannedHours'
FROM clean.vw_routes
WHERE PlannedHours IS NULL
UNION ALL
SELECT COUNT(*), 'vw_routes.ActualHours'
FROM clean.vw_routes
WHERE ActualHours IS NULL;

-----------------------------------------------------
-- STEP 3: BUSINESS RULE VALIDATIONS
-----------------------------------------------------

-- Late deliveries flagged correctly
SELECT COUNT(*) AS IncorrectLateFlag
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'Late';

-- Priority normalized to 0/1
SELECT COUNT(*) AS InvalidPriorityFlag
FROM clean.vw_deliveries
WHERE PriorityFlag NOT IN (0,1);

-----------------------------------------------------
-- STEP 4: OPTIONAL DATA PROFILING
-----------------------------------------------------

-- Example: Sales amount stats by Region
SELECT Region,
       COUNT(*) AS Transactions,
       SUM(SalesAmount) AS TotalSales,
       AVG(SalesAmount) AS AvgSale,
       MIN(SalesAmount) AS MinSale,
       MAX(SalesAmount) AS MaxSale
FROM clean.vw_sales
GROUP BY Region
ORDER BY Region;

-- Example: UnitsSold distribution by ProductType
SELECT ProductType,
       COUNT(*) AS Transactions,
       SUM(UnitsSold) AS TotalUnits,
       AVG(UnitsSold) AS AvgUnits,
       MIN(UnitsSold) AS MinUnits,
       MAX(UnitsSold) AS MaxUnits
FROM clean.vw_sales
GROUP BY ProductType
ORDER BY ProductType;

PRINT '--- CLEAN LAYER VALIDATION END ---';
*/
