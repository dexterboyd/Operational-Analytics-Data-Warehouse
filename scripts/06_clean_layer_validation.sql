/*=============================================================
  CLEAN LAYER VALIDATION
  Database: Fedex_Ops_Database
  Version:  3.0

  Purpose:
      Validate clean datasets before loading them into the
      Data Warehouse (dw schema). This script is informational 
	  for review but does not halt the pipeline.

	  For an automated hard stop, use clean_validation_gate.sql.

  Run Order:
      1. etl_staging_setup.sql          -- build schemas/tables
      2. load_staging.py                   -- load CSV data
      3. staging_layer_validation.sql   -- validate staging
      4. clean_layer.sql                -- build clean views
      5. clean_layer_data_profiling     -- profile clean data
      6. THIS SCRIPT                       -- human review

  Clean Layer Views Referenced:
      clean.clean_sales        -- standardized sales transactions
      clean.clean_deliveries   -- standardized delivery records
      clean.clean_routes       -- route performance with surrogate key
      clean.clean_exceptions   -- standardized exception records

  Checks Performed:
      1. Row count comparison (staging vs clean)
      2. Required field NULL validation
      3. Business rule verification
      4. Referential integrity checks
      5. Data profiling metrics
=============================================================*/

USE Fedex_Ops_Database;
GO

PRINT '===== CLEAN LAYER VALIDATION START =====';

/*=============================================================
  1. ROW COUNT VALIDATION
  Purpose:
      Ensure all records flow from staging to the clean
      views. The clean layer applies transformations only --
      it does not filter rows -- so CleanRows should always
      equal StagingRows. A non-zero DroppedRows count
      indicates unexpected row loss and must be investigated
      before the DW load.
=============================================================*/

PRINT '===== 1. ROW COUNT VALIDATION =====';

SELECT
    'Sales'                                             AS TableName,
    (SELECT COUNT(*) FROM staging.staging_sales)        AS StagingRows,
    (SELECT COUNT(*) FROM clean.clean_sales)            AS CleanRows,
    (SELECT COUNT(*) FROM staging.staging_sales)
        - (SELECT COUNT(*) FROM clean.clean_sales)      AS DroppedRows;

SELECT
    'Deliveries'                                        AS TableName,
    (SELECT COUNT(*) FROM staging.staging_deliveries)   AS StagingRows,
    (SELECT COUNT(*) FROM clean.clean_deliveries)       AS CleanRows,
    (SELECT COUNT(*) FROM staging.staging_deliveries)
        - (SELECT COUNT(*) FROM clean.clean_deliveries) AS DroppedRows;

SELECT
    'Exceptions'                                        AS TableName,
    (SELECT COUNT(*) FROM staging.staging_exceptions)   AS StagingRows,
    (SELECT COUNT(*) FROM clean.clean_exceptions)       AS CleanRows,
    (SELECT COUNT(*) FROM staging.staging_exceptions)
        - (SELECT COUNT(*) FROM clean.clean_exceptions) AS DroppedRows;

SELECT
    'Routes'                                            AS TableName,
    (SELECT COUNT(*) FROM staging.staging_routes)       AS StagingRows,
    (SELECT COUNT(*) FROM clean.clean_routes)           AS CleanRows,
    (SELECT COUNT(*) FROM staging.staging_routes)
        - (SELECT COUNT(*) FROM clean.clean_routes)     AS DroppedRows;

/*=============================================================
  2. REQUIRED FIELD NULL CHECKS
  Purpose:
      Return rows missing critical identifiers that would
      break fact table loading or dimension joins. Only key
      columns are returned.

      NOTE: clean_exceptions.ResolvedDate is excluded --
      NULL means the exception is still open, which is valid.

      NOTE: clean_deliveries.DriverID is excluded -- the view
      replaces all NULLs with 'Unknown'.
=============================================================*/

PRINT '===== 2. NULL VALIDATION =====';

-- clean_sales: rows missing any critical field
SELECT
    SalesID,
    DeliveryID,
    DateKey,
    UnitsSold,
    SalesAmount,
    'NULL critical field'                               AS Issue
FROM clean.clean_sales
WHERE DateKey     IS NULL
   OR SalesAmount IS NULL
   OR UnitsSold   IS NULL;

-- clean_deliveries: rows missing any required identifier
-- DriverID excluded: view guarantees 'Unknown' not NULL
SELECT
    DeliveryID,
    RouteID,
    DeliveryDate,
    DeliveryStatus,
    'NULL critical field'                               AS Issue
FROM clean.clean_deliveries
WHERE DeliveryID     IS NULL
   OR RouteID        IS NULL
   OR DeliveryDate   IS NULL
   OR DeliveryStatus IS NULL;

-- clean_exceptions: rows missing required identifiers
-- ResolvedDate excluded: NULL = open exception, not an error
SELECT
    ExceptionID,
    DeliveryID,
    DateReported,
    'NULL critical field'                               AS Issue
FROM clean.clean_exceptions
WHERE ExceptionID  IS NULL
   OR DeliveryID   IS NULL
   OR DateReported IS NULL;

-- clean_routes: rows missing required identifiers
-- DriverID excluded: view guarantees 'Unknown' not NULL
SELECT
    RouteID,
    PlannedStops,
    ActualStops,
    PlannedHours,
    ActualHours,
    'NULL critical field'                               AS Issue
FROM clean.clean_routes
WHERE RouteID      IS NULL
   OR PlannedStops IS NULL
   OR ActualStops  IS NULL
   OR PlannedHours IS NULL
   OR ActualHours  IS NULL;

/*=============================================================
  3. BUSINESS RULE VALIDATION
  Purpose:
      Confirm transformation rules in the clean layer were
      applied correctly. All result sets should return 0 rows.

      NOTE: DeliveryStatus casing matches the staging source
      exactly -- 'Late', 'On-Time', 'Exception' -- not 'LATE'.
=============================================================*/

PRINT '===== 3. BUSINESS RULE CHECKS =====';

-- IsLate flag check:
-- Every delivery with status 'Late' or 'Exception' must have
-- IsLate = 1. Rows returned here are mismatches.
SELECT TOP 20
    DeliveryID,
    DeliveryDate,
    ExpectedDeliveryDate,
    DeliveryStatus,
    IsLate,
    'IsLate should be 1'                                AS Issue
FROM clean.clean_deliveries
WHERE DeliveryStatus IN ('Late', 'Exception')
  AND IsLate <> 1;

-- IsLate not set on On-Time deliveries:
-- Every 'On-Time' delivery must have IsLate = 0.
-- Rows returned here are mismatches.
SELECT TOP 20
    DeliveryID,
    DeliveryStatus,
    IsLate,
    'IsLate should be 0'                                AS Issue
FROM clean.clean_deliveries
WHERE DeliveryStatus = 'On-Time'
  AND IsLate <> 0;

-- PriorityFlag normalization:
-- Should show only values 0 and 1. Any other value is an error.
SELECT
    PriorityFlag,
    COUNT(*)                                            AS RecordCount
FROM clean.clean_deliveries
GROUP BY PriorityFlag
ORDER BY PriorityFlag;

-- IsResolved flag check:
-- Every exception with a populated ResolvedDate must have
-- IsResolved = 1. Rows returned here are mismatches.
SELECT TOP 20
    ExceptionID,
    ResolvedDate,
    IsResolved,
    'IsResolved should be 1'                            AS Issue
FROM clean.clean_exceptions
WHERE ResolvedDate IS NOT NULL
  AND IsResolved <> 1;

-- Truncation check: no abbreviated values should remain
-- after clean layer transformation. All counts should be 0.
SELECT 'clean_sales ProductType' AS Check_, COUNT(*) AS Remaining
FROM clean.clean_sales
WHERE ProductType LIKE '_.'
UNION ALL

SELECT 'clean_sales Region' AS Check_, COUNT(*) AS Remaining
FROM clean.clean_sales
WHERE Region LIKE '_.'
UNION ALL

SELECT 'clean_deliveries  ShipmentType' AS Check_, COUNT(*) AS Remaining
FROM clean.clean_deliveries
WHERE ShipmentType  LIKE '_.'
UNION ALL

SELECT 'clean_deliveries  Region' AS Check_, COUNT(*) AS Remaining
FROM clean.clean_deliveries
WHERE Region LIKE '_.'
UNION ALL

SELECT 'clean_exceptions  ExceptionType' AS Check_, COUNT(*) AS Remaining
FROM   clean.clean_exceptions
WHERE ExceptionType LIKE '_.'
UNION ALL

SELECT 'clean_routes Region' AS Check_, COUNT(*) AS Remaining
FROM clean.clean_routes WHERE Region LIKE '_.'
UNION ALL

SELECT 'clean_routes Unknown DriverID' AS Check_, COUNT(*) AS Remaining
FROM clean.clean_routes
WHERE DriverID = 'Unknown';

/*=============================================================
  4. REFERENTIAL INTEGRITY CHECKS
  Purpose:
      Validate that DeliveryIDs in clean_sales and
      clean_exceptions all exist in clean_deliveries.
      Orphaned DeliveryIDs will silently drop rows during
      DW fact table joins if not caught here.
=============================================================*/

PRINT '===== 4. REFERENTIAL INTEGRITY =====';

-- clean_sales DeliveryIDs with no matching delivery record
SELECT
    s.SalesID,
    s.DeliveryID,
    'No matching delivery'                              AS Issue
FROM clean.clean_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.clean_deliveries d
    WHERE d.DeliveryID = s.DeliveryID
);

-- clean_exceptions DeliveryIDs with no matching delivery record
SELECT
    e.ExceptionID,
    e.DeliveryID,
    'No matching delivery'                              AS Issue
FROM clean.clean_exceptions e
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.clean_deliveries d
    WHERE d.DeliveryID = e.DeliveryID
);

/*=============================================================
  5. DATA PROFILING METRICS
  Purpose:
      Quick statistical summary for anomaly detection before
      DW load. Review for unexpected distributions or values.
=============================================================*/

PRINT '===== 5. DATA PROFILING =====';

-- Sales summary statistics
SELECT
    MIN(SalesAmount)                                    AS MinSales,
    MAX(SalesAmount)                                    AS MaxSales,
    ROUND(AVG(SalesAmount), 2)                          AS AvgSales,
    SUM(SalesAmount)                                    AS TotalSales,
    COUNT(*)                                            AS RecordCount
FROM clean.clean_sales;

-- Sales region distribution
SELECT
    Region,
    COUNT(*)                                            AS RecordCount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)   AS PctOfTotal
FROM clean.clean_sales
GROUP BY Region
ORDER BY RecordCount DESC;

-- Delivery status distribution
SELECT
    DeliveryStatus,
    COUNT(*)                                            AS RecordCount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)   AS PctOfTotal
FROM clean.clean_deliveries
GROUP BY DeliveryStatus
ORDER BY RecordCount DESC;

-- Route efficiency metrics
-- StopEfficiencyPct: actual stops as % of planned stops
-- HourEfficiencyPct: planned hours as % of actual hours
SELECT
    MIN(StopEfficiencyPct)                              AS MinStopEfficiency,
    MAX(StopEfficiencyPct)                              AS MaxStopEfficiency,
    ROUND(AVG(StopEfficiencyPct), 2)                    AS AvgStopEfficiency,
    MIN(HourEfficiencyPct)                              AS MinHourEfficiency,
    MAX(HourEfficiencyPct)                              AS MaxHourEfficiency,
    ROUND(AVG(HourEfficiencyPct), 2)                    AS AvgHourEfficiency
FROM clean.clean_routes;

-- Exception type distribution
SELECT
    ExceptionType,
    COUNT(*)                                            AS RecordCount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)   AS PctOfTotal
FROM clean.clean_exceptions
GROUP BY ExceptionType
ORDER BY RecordCount DESC;

/*=============================================================
  VALIDATION COMPLETE
=============================================================*/

PRINT '===== CLEAN LAYER VALIDATION COMPLETE =====';
PRINT 'Review result sets above before loading DW tables.';
PRINT 'To halt the pipeline automatically on failures, run 07_clean_validation_gate_v3.0.sql.';
GO
