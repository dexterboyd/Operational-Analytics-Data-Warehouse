/*==============================================================
  CLEAN LAYER VALIDATION
  Schema: clean
  Version: 2.0

  PURPOSE
  -------
  Validate cleansed datasets before loading them into the
  Data Warehouse (dw schema). This script is informational —
  it surfaces issues for human review but does not halt the
  pipeline. For an automated hard stop, use
  07_clean_validation_gate_v2.0.sql.

  CHECKS PERFORMED
  ----------------
  1. Row count comparison (staging vs clean)
  2. Required field NULL validation
  3. Business rule verification
  4. Referential integrity checks
  5. Data profiling metrics

  PIPELINE FLOW
  -------------
  staging -> clean -> [THIS SCRIPT] -> dw -> reporting -> Power BI

  CHANGE LOG
  ----------
  v2.0 - Removed duplicate commented block (versioning artifact).
       - Fixed late delivery check: 'Late' corrected to 'LATE'
         to match the value produced by vw_deliveries.
       - Replaced SELECT * in NULL checks with key columns only
         to avoid unnecessarily wide result sets in production.
       - Added referential integrity checks for DeliveryID
         across vw_sales and vw_exceptions.
       - Renamed file prefix from 05_ to 06_ to avoid
         filename-ordering collision with the profiling script.
==============================================================*/

PRINT '===== CLEAN LAYER VALIDATION START =====';


/*==============================================================
  1. ROW COUNT VALIDATION
  Purpose:
      Ensure records successfully flowed from staging into
      the clean views. Large discrepancies may indicate a
      failed bulk load or unexpected data quality drops.
==============================================================*/

PRINT '===== 1. ROW COUNT VALIDATION =====';

SELECT
    'Sales'       AS TableName,
    (SELECT COUNT(*) FROM staging.staging_sales)       AS StagingRows,
    (SELECT COUNT(*) FROM clean.vw_sales)              AS CleanRows,
    (SELECT COUNT(*) FROM staging.staging_sales)
        - (SELECT COUNT(*) FROM clean.vw_sales)        AS DroppedRows;

SELECT
    'Deliveries'  AS TableName,
    (SELECT COUNT(*) FROM staging.staging_deliveries)  AS StagingRows,
    (SELECT COUNT(*) FROM clean.vw_deliveries)         AS CleanRows,
    (SELECT COUNT(*) FROM staging.staging_deliveries)
        - (SELECT COUNT(*) FROM clean.vw_deliveries)   AS DroppedRows;

SELECT
    'Exceptions'  AS TableName,
    (SELECT COUNT(*) FROM staging.staging_exceptions)  AS StagingRows,
    (SELECT COUNT(*) FROM clean.vw_exceptions)         AS CleanRows,
    (SELECT COUNT(*) FROM staging.staging_exceptions)
        - (SELECT COUNT(*) FROM clean.vw_exceptions)   AS DroppedRows;

SELECT
    'Routes'      AS TableName,
    (SELECT COUNT(*) FROM staging.staging_routes)      AS StagingRows,
    (SELECT COUNT(*) FROM clean.vw_routes)             AS CleanRows,
    (SELECT COUNT(*) FROM staging.staging_routes)
        - (SELECT COUNT(*) FROM clean.vw_routes)       AS DroppedRows;


/*==============================================================
  2. REQUIRED FIELD NULL CHECKS
  Purpose:
      Identify records missing critical identifiers that would
      break fact table loading or dimension joins.
      Only key columns are returned (not SELECT *) to keep
      output actionable in production environments.
==============================================================*/

PRINT '===== 2. NULL VALIDATION =====';

-- Sales: rows missing any critical field
SELECT
    SalesID,
    DeliveryID,
    DateKey,
    UnitsSold,
    SalesAmount,
    'NULL critical field' AS Issue
FROM clean.vw_sales
WHERE DateKey     IS NULL
   OR SalesAmount IS NULL
   OR UnitsSold   IS NULL;

-- Deliveries: rows missing any required identifier
SELECT
    DeliveryID,
    RouteID,
    DriverID,
    'NULL critical field' AS Issue
FROM clean.vw_deliveries
WHERE DeliveryID IS NULL
   OR RouteID    IS NULL
   OR DriverID   IS NULL;

-- Exceptions: rows missing required identifiers
-- NOTE: ResolvedDate IS NULL is valid (open exception); it is
-- not treated as an error here.
SELECT
    ExceptionID,
    DeliveryID,
    DateReported,
    'NULL critical field' AS Issue
FROM clean.vw_exceptions
WHERE ExceptionID  IS NULL
   OR DeliveryID   IS NULL
   OR DateReported IS NULL;


/*==============================================================
  3. BUSINESS RULE VALIDATION
  Purpose:
      Confirm that transformation rules in the clean layer
      were applied correctly.
==============================================================*/

PRINT '===== 3. BUSINESS RULE CHECKS =====';

-- LATE DELIVERY RULE VALIDATION
-- vw_deliveries produces 'LATE' (all caps). Any rows returned
-- here indicate the business rule was not applied correctly.
SELECT TOP 20
    DeliveryID,
    DeliveryDate,
    ExpectedDeliveryDate,
    DeliveryStatus,
    'Expected LATE status' AS Issue
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'LATE';

-- PRIORITY FLAG NORMALIZATION
-- Should show only values 0 and 1. Any other value is an error.
SELECT
    PriorityFlag,
    COUNT(*) AS RecordCount
FROM clean.vw_deliveries
GROUP BY PriorityFlag
ORDER BY PriorityFlag;

-- DATE CORRECTION AUDIT
-- Informational: how many exception records had their
-- ResolvedDate corrected due to chronology errors.
SELECT
    COUNT(*) AS DateCorrectedCount
FROM clean.vw_exceptions
WHERE IsDateCorrected = 1;


/*==============================================================
  4. REFERENTIAL INTEGRITY CHECKS
  Purpose:
      Validate that DeliveryIDs in vw_sales and vw_exceptions
      exist in vw_deliveries. Orphaned IDs will silently lose
      rows during DW fact table joins.
==============================================================*/

PRINT '===== 4. REFERENTIAL INTEGRITY =====';

-- Sales DeliveryIDs with no matching delivery record
SELECT
    s.SalesID,
    s.DeliveryID,
    'No matching delivery' AS Issue
FROM clean.vw_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.vw_deliveries d
    WHERE d.DeliveryID = s.DeliveryID
);

-- Exception DeliveryIDs with no matching delivery record
SELECT
    e.ExceptionID,
    e.DeliveryID,
    'No matching delivery' AS Issue
FROM clean.vw_exceptions e
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.vw_deliveries d
    WHERE d.DeliveryID = e.DeliveryID
);


/*==============================================================
  5. DATA PROFILING — SALES METRICS
  Purpose:
      Provide quick statistical insight into cleaned sales data
      for anomaly detection before DW load.
==============================================================*/

PRINT '===== 5. SALES DATA PROFILE =====';

SELECT
    MIN(SalesAmount)  AS MinSales,
    MAX(SalesAmount)  AS MaxSales,
    AVG(SalesAmount)  AS AvgSales,
    SUM(SalesAmount)  AS TotalSales,
    COUNT(*)          AS RecordCount
FROM clean.vw_sales;


/*==============================================================
  6. DATA PROFILING — REGION DISTRIBUTION
  Purpose:
      Identify regional distribution patterns and detect
      unexpected or missing region values.
==============================================================*/

PRINT '===== 6. REGION DISTRIBUTION =====';

SELECT
    Region,
    COUNT(*) AS RecordCount
FROM clean.vw_sales
GROUP BY Region
ORDER BY RecordCount DESC;


/*==============================================================
  7. ROUTE PERFORMANCE VALIDATION
  Purpose:
      Ensure operational metrics are within reasonable ranges
      and derived columns are producing sensible values.
==============================================================*/

PRINT '===== 7. ROUTE VALIDATION =====';

SELECT
    MIN(ActualStops)       AS MinStops,
    MAX(ActualStops)       AS MaxStops,
    AVG(ActualHours)       AS AvgHours,
    MIN(EfficiencyRatio)   AS MinEfficiency,
    MAX(EfficiencyRatio)   AS MaxEfficiency,
    AVG(EfficiencyRatio)   AS AvgEfficiency
FROM clean.vw_routes;


/*==============================================================
  VALIDATION COMPLETE
==============================================================*/

PRINT '===== VALIDATION COMPLETE =====';
PRINT 'Review result sets above before loading DW tables.';
PRINT 'To halt the pipeline automatically on failures, run 07_clean_validation_gate_v2.0.sql.';
