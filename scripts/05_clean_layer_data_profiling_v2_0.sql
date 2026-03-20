/*==============================================================
  CLEAN LAYER DATA PROFILING
  Schema: clean
  Version: 2.0

  PURPOSE
  -------
  Provide descriptive statistics and row-level insight into
  the clean views after transformation. This script is
  informational only — it does not halt the pipeline on
  findings. Run it to investigate anomalies or verify
  transformation output before a DW load.

  For pipeline gate logic (PASS/FAIL with hard stops), use
  07_clean_validation_gate_v2.0.sql instead.

  STEPS
  -----
  1. Row count comparison  (staging vs clean, with drop rate)
  2. Required field NULL counts
  3. Business rule spot-checks
  4. Data profiling statistics

  PIPELINE FLOW
  -------------
  staging -> clean -> [THIS SCRIPT] -> dw -> reporting -> Power BI

  CHANGE LOG
  ----------
  v2.0 - Removed duplicate commented block (versioning artifact).
       - Fixed routes subquery: changed PlannedHours <> 0 to
         PlannedHours > 0 to match vw_routes filter exactly.
       - Deliveries and exceptions subqueries now apply the same
         WHERE conditions used by their respective views so
         row-count discrepancies reflect true data quality drops,
         not expected filter differences.
       - Removed vw_exceptions.ResolvedDate from NULL checks;
         NULL ResolvedDate is valid (open exception) and would
         always produce a non-zero count, making the check
         misleading.
       - Added referential integrity check: DeliveryIDs in
         vw_sales and vw_exceptions that have no matching row
         in vw_deliveries.
       - Added negative value checks on numeric columns.
       - Added date range sanity checks.
==============================================================*/

PRINT '--- CLEAN LAYER DATA PROFILING START ---';

/*==============================================================
  STEP 1: ROW COUNT COMPARISON
  Purpose:
      Compare staging row counts (after applying the same
      filters the views use) against clean view counts.
      A ratio < 1.0 indicates rows were dropped; the drop
      rate column makes this immediately visible.

      NOTE: Some drop is expected (null keys, invalid amounts).
      Investigate if drop rate falls below ~0.95 unexpectedly.
==============================================================*/

PRINT '--- STEP 1: ROW COUNT CHECKS ---';

-- Sales: subquery mirrors vw_sales WHERE clause exactly
SELECT
    'vw_sales'                                              AS ViewName,
    COUNT(*)                                                AS CleanRowCount,
    (SELECT COUNT(*)
     FROM staging.staging_sales
     WHERE SalesID    IS NOT NULL
       AND DeliveryID IS NOT NULL
       AND DateKey    IS NOT NULL
       AND UnitsSold  > 0
       AND SalesAmount > 0)                                 AS StagingValidRowCount,
    CAST(COUNT(*) AS DECIMAL(10,4))
        / NULLIF(
            (SELECT COUNT(*)
             FROM staging.staging_sales
             WHERE SalesID    IS NOT NULL
               AND DeliveryID IS NOT NULL
               AND DateKey    IS NOT NULL
               AND UnitsSold  > 0
               AND SalesAmount > 0),
          0)                                                AS RetentionRate
FROM clean.vw_sales;

-- Deliveries: subquery mirrors vw_deliveries WHERE clause
SELECT
    'vw_deliveries'                                         AS ViewName,
    COUNT(*)                                                AS CleanRowCount,
    (SELECT COUNT(*)
     FROM staging.staging_deliveries
     WHERE DeliveryID           IS NOT NULL
       AND RouteID              IS NOT NULL
       AND DriverID             IS NOT NULL
       AND DeliveryDate         IS NOT NULL
       AND ExpectedDeliveryDate IS NOT NULL)                AS StagingValidRowCount,
    CAST(COUNT(*) AS DECIMAL(10,4))
        / NULLIF(
            (SELECT COUNT(*)
             FROM staging.staging_deliveries
             WHERE DeliveryID           IS NOT NULL
               AND RouteID              IS NOT NULL
               AND DriverID             IS NOT NULL
               AND DeliveryDate         IS NOT NULL
               AND ExpectedDeliveryDate IS NOT NULL),
          0)                                                AS RetentionRate
FROM clean.vw_deliveries;

-- Exceptions: subquery mirrors vw_exceptions WHERE clause
SELECT
    'vw_exceptions'                                         AS ViewName,
    COUNT(*)                                                AS CleanRowCount,
    (SELECT COUNT(*)
     FROM staging.staging_exceptions
     WHERE ExceptionID  IS NOT NULL
       AND DeliveryID   IS NOT NULL
       AND DateReported IS NOT NULL)                        AS StagingValidRowCount,
    CAST(COUNT(*) AS DECIMAL(10,4))
        / NULLIF(
            (SELECT COUNT(*)
             FROM staging.staging_exceptions
             WHERE ExceptionID  IS NOT NULL
               AND DeliveryID   IS NOT NULL
               AND DateReported IS NOT NULL),
          0)                                                AS RetentionRate
FROM clean.vw_exceptions;

-- Routes: subquery mirrors vw_routes WHERE clause exactly
SELECT
    'vw_routes'                                             AS ViewName,
    COUNT(*)                                                AS CleanRowCount,
    (SELECT COUNT(*)
     FROM staging.staging_routes
     WHERE RouteID      IS NOT NULL
       AND DriverID     IS NOT NULL
       AND PlannedStops > 0
       AND ActualStops  > 0
       AND PlannedHours > 0
       AND ActualHours  > 0)                                AS StagingValidRowCount,
    CAST(COUNT(*) AS DECIMAL(10,4))
        / NULLIF(
            (SELECT COUNT(*)
             FROM staging.staging_routes
             WHERE RouteID      IS NOT NULL
               AND DriverID     IS NOT NULL
               AND PlannedStops > 0
               AND ActualStops  > 0
               AND PlannedHours > 0
               AND ActualHours  > 0),
          0)                                                AS RetentionRate
FROM clean.vw_routes;


/*==============================================================
  STEP 2: REQUIRED FIELD NULL COUNTS
  Purpose:
      Count NULLs in fields that must be populated for a
      successful DW load. A non-zero count here warrants
      investigation before loading.

      NOTE: vw_exceptions.ResolvedDate is intentionally excluded;
      NULL means the exception is still open, which is valid.
==============================================================*/

PRINT '--- STEP 2: REQUIRED FIELD NULL CHECKS ---';

-- Sales required fields
SELECT COUNT(*) AS NullCount, 'vw_sales.SalesID'     AS FieldName FROM clean.vw_sales WHERE SalesID     IS NULL
UNION ALL
SELECT COUNT(*),               'vw_sales.DeliveryID'              FROM clean.vw_sales WHERE DeliveryID   IS NULL
UNION ALL
SELECT COUNT(*),               'vw_sales.DateKey'                 FROM clean.vw_sales WHERE DateKey      IS NULL
UNION ALL
SELECT COUNT(*),               'vw_sales.UnitsSold'               FROM clean.vw_sales WHERE UnitsSold    IS NULL
UNION ALL
SELECT COUNT(*),               'vw_sales.SalesAmount'             FROM clean.vw_sales WHERE SalesAmount  IS NULL;

-- Deliveries required fields
SELECT COUNT(*) AS NullCount, 'vw_deliveries.DeliveryID'          AS FieldName FROM clean.vw_deliveries WHERE DeliveryID           IS NULL
UNION ALL
SELECT COUNT(*),               'vw_deliveries.RouteID'                           FROM clean.vw_deliveries WHERE RouteID              IS NULL
UNION ALL
SELECT COUNT(*),               'vw_deliveries.DriverID'                          FROM clean.vw_deliveries WHERE DriverID             IS NULL
UNION ALL
SELECT COUNT(*),               'vw_deliveries.DeliveryDate'                      FROM clean.vw_deliveries WHERE DeliveryDate         IS NULL
UNION ALL
SELECT COUNT(*),               'vw_deliveries.DeliveryStatus'                    FROM clean.vw_deliveries WHERE DeliveryStatus       IS NULL;

-- Exceptions required fields (ResolvedDate excluded — valid to be NULL)
SELECT COUNT(*) AS NullCount, 'vw_exceptions.ExceptionID'         AS FieldName FROM clean.vw_exceptions WHERE ExceptionID  IS NULL
UNION ALL
SELECT COUNT(*),               'vw_exceptions.DeliveryID'                        FROM clean.vw_exceptions WHERE DeliveryID   IS NULL
UNION ALL
SELECT COUNT(*),               'vw_exceptions.DateReported'                      FROM clean.vw_exceptions WHERE DateReported IS NULL;

-- Routes required fields
SELECT COUNT(*) AS NullCount, 'vw_routes.RouteID'                 AS FieldName FROM clean.vw_routes WHERE RouteID      IS NULL
UNION ALL
SELECT COUNT(*),               'vw_routes.DriverID'                              FROM clean.vw_routes WHERE DriverID     IS NULL
UNION ALL
SELECT COUNT(*),               'vw_routes.PlannedStops'                          FROM clean.vw_routes WHERE PlannedStops IS NULL
UNION ALL
SELECT COUNT(*),               'vw_routes.ActualStops'                           FROM clean.vw_routes WHERE ActualStops  IS NULL
UNION ALL
SELECT COUNT(*),               'vw_routes.PlannedHours'                          FROM clean.vw_routes WHERE PlannedHours IS NULL
UNION ALL
SELECT COUNT(*),               'vw_routes.ActualHours'                           FROM clean.vw_routes WHERE ActualHours  IS NULL;


/*==============================================================
  STEP 3: BUSINESS RULE SPOT-CHECKS
  Purpose:
      Confirm transformations in the clean layer were applied
      correctly. These should all return 0 rows/count.
==============================================================*/

PRINT '--- STEP 3: BUSINESS RULE CHECKS ---';

-- Late deliveries flagged correctly (must compare 'LATE', all caps)
SELECT COUNT(*) AS IncorrectLateFlag
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'LATE';

-- PriorityFlag normalized to 0/1 only
SELECT COUNT(*) AS InvalidPriorityFlag
FROM clean.vw_deliveries
WHERE PriorityFlag NOT IN (0, 1);

-- Date-corrected exceptions (informational: how many had bad chronology)
SELECT COUNT(*) AS DateCorrectedExceptions
FROM clean.vw_exceptions
WHERE IsDateCorrected = 1;

-- Negative or zero numeric values in routes (should be 0 after view filter)
SELECT COUNT(*) AS InvalidRouteMetrics
FROM clean.vw_routes
WHERE PlannedStops <= 0
   OR ActualStops  <= 0
   OR PlannedHours <= 0
   OR ActualHours  <= 0;


/*==============================================================
  STEP 4: REFERENTIAL INTEGRITY CHECKS
  Purpose:
      Verify that DeliveryIDs in sales and exceptions exist
      in the deliveries view. Orphaned IDs will silently
      drop rows during DW fact table joins.
==============================================================*/

PRINT '--- STEP 4: REFERENTIAL INTEGRITY ---';

-- Sales DeliveryIDs with no matching delivery record
SELECT COUNT(*) AS OrphanedSalesDeliveryIDs
FROM clean.vw_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.vw_deliveries d
    WHERE d.DeliveryID = s.DeliveryID
);

-- Exception DeliveryIDs with no matching delivery record
SELECT COUNT(*) AS OrphanedExceptionDeliveryIDs
FROM clean.vw_exceptions e
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.vw_deliveries d
    WHERE d.DeliveryID = e.DeliveryID
);


/*==============================================================
  STEP 5: DATA RANGE SANITY CHECKS
  Purpose:
      Flag obviously out-of-range values that may indicate
      CSV parsing errors or source data issues.
==============================================================*/

PRINT '--- STEP 5: DATA RANGE SANITY ---';

-- Negative or zero sales values (should be 0 after view filter)
SELECT COUNT(*) AS NegativeOrZeroSales
FROM clean.vw_sales
WHERE UnitsSold  <= 0
   OR SalesAmount <= 0;

-- Future-dated deliveries (DeliveryDate beyond today)
SELECT COUNT(*) AS FutureDatedDeliveries
FROM clean.vw_deliveries
WHERE DeliveryDate > CAST(GETDATE() AS DATE);

-- Exceptions reported in the future
SELECT COUNT(*) AS FutureDatedExceptions
FROM clean.vw_exceptions
WHERE DateReported > CAST(GETDATE() AS DATE);


/*==============================================================
  STEP 6: DESCRIPTIVE PROFILING
  Purpose:
      Provide summary statistics for anomaly detection and
      pre-load sense checking.
==============================================================*/

PRINT '--- STEP 6: DATA PROFILING ---';

-- Sales amount statistics by region
SELECT
    Region,
    COUNT(*)            AS Transactions,
    SUM(SalesAmount)    AS TotalSales,
    AVG(SalesAmount)    AS AvgSale,
    MIN(SalesAmount)    AS MinSale,
    MAX(SalesAmount)    AS MaxSale
FROM clean.vw_sales
GROUP BY Region
ORDER BY Region;

-- Units sold distribution by product type
SELECT
    ProductType,
    COUNT(*)            AS Transactions,
    SUM(UnitsSold)      AS TotalUnits,
    AVG(UnitsSold)      AS AvgUnits,
    MIN(UnitsSold)      AS MinUnits,
    MAX(UnitsSold)      AS MaxUnits
FROM clean.vw_sales
GROUP BY ProductType
ORDER BY ProductType;

-- Delivery status distribution
SELECT
    DeliveryStatus,
    COUNT(*)            AS RecordCount
FROM clean.vw_deliveries
GROUP BY DeliveryStatus
ORDER BY RecordCount DESC;

-- Route efficiency summary
SELECT
    MIN(EfficiencyRatio)    AS MinEfficiency,
    MAX(EfficiencyRatio)    AS MaxEfficiency,
    AVG(EfficiencyRatio)    AS AvgEfficiency,
    MIN(StopVariance)       AS MinStopVariance,
    MAX(StopVariance)       AS MaxStopVariance,
    AVG(CAST(StopVariance AS DECIMAL(10,2))) AS AvgStopVariance
FROM clean.vw_routes;


/*==============================================================
  PROFILING COMPLETE
==============================================================*/

PRINT '--- CLEAN LAYER DATA PROFILING END ---';
PRINT 'Review all result sets carefully before loading DW tables.';
PRINT 'For a hard pipeline stop on failures, run 07_clean_validation_gate_v2.0.sql.';
