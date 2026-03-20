/*=============================================================
  CLEAN LAYER DATA PROFILING
  Database: Fedex_Ops_Database
  Version:  3.0

  Purpose:
      Provide descriptive statistics and row-level insight into
      the clean views after transformation. This script is
      informational only -- it does not halt the pipeline on
      any finding. Run it to investigate anomalies or verify
      transformation output before a DW load.

      For a hard pipeline stop on failures, use
      07_clean_validation_gate_v3.0.sql instead.

  Run Order:
      1. etl_staging_setup_v5.sql          -- build schemas/tables
      2. load_staging.py                   -- load CSV data
      3. staging_layer_validation_v2.sql   -- validate staging
      4. clean_layer_v1.sql                -- build clean views
      5. THIS SCRIPT                       -- profile clean data
      6. 06_clean_layer_validation_v3.0    -- human review
      7. 07_clean_validation_gate_v3.0     -- pipeline gate

  Clean Layer Views Referenced:
      clean.clean_sales        -- standardized sales transactions
      clean.clean_deliveries   -- standardized delivery records
      clean.clean_routes       -- route performance with surrogate key
      clean.clean_exceptions   -- standardized exception records

  Steps:
      1. Row count comparison     (staging vs clean, with retention rate)
      2. Required field NULL counts
      3. Business rule spot-checks
      4. Referential integrity checks
      5. Data range sanity checks
      6. Descriptive profiling statistics

=============================================================*/

USE Fedex_Ops_Database;
GO

PRINT '--- CLEAN LAYER DATA PROFILING START ---';

/*=============================================================
  STEP 1: ROW COUNT COMPARISON
  Purpose:
      Compare staging row counts against clean view counts.
      The clean views do not filter any rows -- they apply
      transformations only -- so CleanRowCount should always
      equal StagingRowCount. A RetentionRate below 1.0
      indicates unexpected row loss and warrants investigation
      before the DW load.
=============================================================*/

PRINT '--- STEP 1: ROW COUNT COMPARISON ---';

-- clean_sales vs staging_sales
SELECT
    'clean_sales'                                       AS ViewName,
    COUNT(*)                                            AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_sales)        AS StagingRowCount,
    CAST(COUNT(*) AS DECIMAL(10,4))
        / NULLIF(
            (SELECT COUNT(*) FROM staging.staging_sales),
          0)                                            AS RetentionRate
FROM clean.clean_sales;

-- clean_deliveries vs staging_deliveries
SELECT
    'clean_deliveries'                                  AS ViewName,
    COUNT(*)                                            AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_deliveries)   AS StagingRowCount,
    CAST(COUNT(*) AS DECIMAL(10,4))
        / NULLIF(
            (SELECT COUNT(*) FROM staging.staging_deliveries),
          0)                                            AS RetentionRate
FROM clean.clean_deliveries;

-- clean_routes vs staging_routes
SELECT
    'clean_routes'                                      AS ViewName,
    COUNT(*)                                            AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_routes)       AS StagingRowCount,
    CAST(COUNT(*) AS DECIMAL(10,4))
        / NULLIF(
            (SELECT COUNT(*) FROM staging.staging_routes),
          0)                                            AS RetentionRate
FROM clean.clean_routes;

-- clean_exceptions vs staging_exceptions
SELECT
    'clean_exceptions'                                  AS ViewName,
    COUNT(*)                                            AS CleanRowCount,
    (SELECT COUNT(*) FROM staging.staging_exceptions)   AS StagingRowCount,
    CAST(COUNT(*) AS DECIMAL(10,4))
        / NULLIF(
            (SELECT COUNT(*) FROM staging.staging_exceptions),
          0)                                            AS RetentionRate
FROM clean.clean_exceptions;


/*=============================================================
  STEP 2: REQUIRED FIELD NULL COUNTS
  Purpose:
      Count NULLs in columns that must be populated for a
      successful DW load. A non-zero count warrants
      investigation before loading.

      NOTE: clean_exceptions.ResolvedDate is excluded here.
      NULL ResolvedDate is valid -- it means the exception is
      still open. Flagging it as an error would always produce
      a non-zero count, making the check misleading.

      NOTE: clean_deliveries.DriverID is excluded here.
      The view replaces all NULLs with 'Unknown' so it can
      never be NULL. The Unknown DriverID count is reported
      separately in Step 6 profiling instead.
=============================================================*/

PRINT '--- STEP 2: REQUIRED FIELD NULL COUNTS ---';

-- clean_sales required fields
SELECT COUNT(*) AS NullCount, 'clean_sales.SalesID'         AS FieldName
FROM clean.clean_sales WHERE SalesID     IS NULL
UNION ALL
SELECT COUNT(*),               'clean_sales.DeliveryID'
FROM clean.clean_sales WHERE DeliveryID  IS NULL
UNION ALL
SELECT COUNT(*),               'clean_sales.DateKey'
FROM clean.clean_sales WHERE DateKey     IS NULL
UNION ALL
SELECT COUNT(*),               'clean_sales.UnitsSold'
FROM clean.clean_sales WHERE UnitsSold   IS NULL
UNION ALL
SELECT COUNT(*),               'clean_sales.SalesAmount'
FROM clean.clean_sales WHERE SalesAmount IS NULL;

-- clean_deliveries required fields (DriverID excluded - always 'Unknown' not NULL)
SELECT COUNT(*) AS NullCount, 'clean_deliveries.DeliveryID'     AS FieldName
FROM clean.clean_deliveries WHERE DeliveryID     IS NULL
UNION ALL
SELECT COUNT(*),               'clean_deliveries.RouteID'
FROM clean.clean_deliveries WHERE RouteID        IS NULL
UNION ALL
SELECT COUNT(*),               'clean_deliveries.DeliveryDate'
FROM clean.clean_deliveries WHERE DeliveryDate   IS NULL
UNION ALL
SELECT COUNT(*),               'clean_deliveries.DeliveryStatus'
FROM clean.clean_deliveries WHERE DeliveryStatus IS NULL;

-- clean_exceptions required fields (ResolvedDate excluded - NULL means open exception)
SELECT COUNT(*) AS NullCount, 'clean_exceptions.ExceptionID'    AS FieldName
FROM clean.clean_exceptions WHERE ExceptionID  IS NULL
UNION ALL
SELECT COUNT(*),               'clean_exceptions.DeliveryID'
FROM clean.clean_exceptions WHERE DeliveryID   IS NULL
UNION ALL
SELECT COUNT(*),               'clean_exceptions.DateReported'
FROM clean.clean_exceptions WHERE DateReported IS NULL;

-- clean_routes required fields
SELECT COUNT(*) AS NullCount, 'clean_routes.RouteID'            AS FieldName
FROM clean.clean_routes WHERE RouteID      IS NULL
UNION ALL
SELECT COUNT(*),               'clean_routes.PlannedStops'
FROM clean.clean_routes WHERE PlannedStops IS NULL
UNION ALL
SELECT COUNT(*),               'clean_routes.ActualStops'
FROM clean.clean_routes WHERE ActualStops  IS NULL
UNION ALL
SELECT COUNT(*),               'clean_routes.PlannedHours'
FROM clean.clean_routes WHERE PlannedHours IS NULL
UNION ALL
SELECT COUNT(*),               'clean_routes.ActualHours'
FROM clean.clean_routes WHERE ActualHours  IS NULL;


/*=============================================================
  STEP 3: BUSINESS RULE SPOT-CHECKS
  Purpose:
      Confirm transformations in the clean layer were applied
      correctly. All counts should return 0.

      NOTE: DeliveryStatus values in source data are mixed
      case ('Late', 'On-Time', 'Exception'). Comparisons use
      the exact casing from staging -- not 'LATE' or 'LATE'.
=============================================================*/

PRINT '--- STEP 3: BUSINESS RULE SPOT-CHECKS ---';

-- IsLate flag set correctly:
-- Any delivery where DeliveryStatus is 'Late' or 'Exception'
-- must have IsLate = 1. Any row returned here is a mismatch.
SELECT COUNT(*) AS IsLateFlagMismatch
FROM clean.clean_deliveries
WHERE DeliveryStatus IN ('Late', 'Exception')
  AND IsLate <> 1;

-- IsLate not set on On-Time deliveries:
-- Any delivery where DeliveryStatus is 'On-Time' must have
-- IsLate = 0. Any row returned here is a mismatch.
SELECT COUNT(*) AS IsLateOnTimeMismatch
FROM clean.clean_deliveries
WHERE DeliveryStatus = 'On-Time'
  AND IsLate <> 0;

-- PriorityFlag contains only 0 or 1:
-- The source BIT column should only ever produce these values.
SELECT COUNT(*) AS InvalidPriorityFlag
FROM clean.clean_deliveries
WHERE PriorityFlag NOT IN (0, 1);

-- IsResolved set correctly:
-- Any exception with a populated ResolvedDate must have
-- IsResolved = 1. Any row returned here is a mismatch.
SELECT COUNT(*) AS IsResolvedFlagMismatch
FROM clean.clean_exceptions
WHERE ResolvedDate IS NOT NULL
  AND IsResolved <> 1;

-- Negative or zero numeric values in routes:
-- StopEfficiencyPct and HourEfficiencyPct are derived from
-- positive denominators in the view, but checking the source
-- confirms no invalid values slipped through.
SELECT COUNT(*) AS InvalidRouteMetrics
FROM staging.staging_routes
WHERE PlannedStops <= 0
   OR ActualStops  <= 0
   OR PlannedHours <= 0
   OR ActualHours  <= 0;


/*=============================================================
  STEP 4: REFERENTIAL INTEGRITY CHECKS
  Purpose:
      Verify that DeliveryIDs in clean_sales and
      clean_exceptions all exist in clean_deliveries.
      Orphaned DeliveryIDs will silently lose rows during
      DW fact table joins if not caught here.
=============================================================*/

PRINT '--- STEP 4: REFERENTIAL INTEGRITY ---';

-- clean_sales DeliveryIDs with no matching delivery record
SELECT COUNT(*) AS OrphanedSalesDeliveryIDs
FROM clean.clean_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.clean_deliveries d
    WHERE d.DeliveryID = s.DeliveryID
);

-- clean_exceptions DeliveryIDs with no matching delivery record
SELECT COUNT(*) AS OrphanedExceptionDeliveryIDs
FROM clean.clean_exceptions e
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.clean_deliveries d
    WHERE d.DeliveryID = e.DeliveryID
);


/*=============================================================
  STEP 5: DATA RANGE SANITY CHECKS
  Purpose:
      Flag obviously out-of-range values that may indicate
      CSV parsing errors or source data issues.
      Source data spans 2023-2025 based on CSV inspection.
=============================================================*/

PRINT '--- STEP 5: DATA RANGE SANITY ---';

-- Negative or zero sales values
SELECT COUNT(*) AS NegativeOrZeroSales
FROM clean.clean_sales
WHERE UnitsSold   <= 0
   OR SalesAmount <= 0;

-- Sales dates outside expected range (2023-2025)
SELECT COUNT(*) AS SalesOutOfRange
FROM clean.clean_sales
WHERE SaleDate < '2023-01-01'
   OR SaleDate > '2025-12-31';

-- Delivery dates outside expected range (2023-2025)
SELECT COUNT(*) AS DeliveryDatesOutOfRange
FROM clean.clean_deliveries
WHERE DeliveryDateOnly < '2023-01-01'
   OR DeliveryDateOnly > '2025-12-31';

-- Exception dates outside expected range (2023-2025)
SELECT COUNT(*) AS ExceptionDatesOutOfRange
FROM clean.clean_exceptions
WHERE DateReportedOnly < '2023-01-01'
   OR DateReportedOnly > '2025-12-31';

-- ResolvedDate before DateReported (chronology error)
-- Should always be 0 -- the clean layer does not correct
-- date ordering so any row here is a source data defect.
SELECT COUNT(*) AS ResolvedBeforeReported
FROM clean.clean_exceptions
WHERE ResolvedDate IS NOT NULL
  AND ResolvedDate < DateReported;


/*=============================================================
  STEP 6: DESCRIPTIVE PROFILING
  Purpose:
      Provide summary statistics for anomaly detection and
      pre-load sense checking. Review for unexpected
      distributions or outlier values before DW load.
=============================================================*/

PRINT '--- STEP 6: DESCRIPTIVE PROFILING ---';

-- Sales amount statistics by region
SELECT
    Region,
    COUNT(*)                                            AS Transactions,
    SUM(SalesAmount)                                    AS TotalSales,
    ROUND(AVG(SalesAmount), 2)                          AS AvgSale,
    MIN(SalesAmount)                                    AS MinSale,
    MAX(SalesAmount)                                    AS MaxSale
FROM clean.clean_sales
GROUP BY Region
ORDER BY Region;

-- Units sold distribution by product type
SELECT
    ProductType,
    COUNT(*)                                            AS Transactions,
    SUM(UnitsSold)                                      AS TotalUnits,
    ROUND(AVG(CAST(UnitsSold AS DECIMAL(10,2))), 2)     AS AvgUnits,
    MIN(UnitsSold)                                      AS MinUnits,
    MAX(UnitsSold)                                      AS MaxUnits
FROM clean.clean_sales
GROUP BY ProductType
ORDER BY ProductType;

-- Delivery status distribution
SELECT
    DeliveryStatus,
    COUNT(*)                                            AS RecordCount,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(),
    2)                                                  AS PctOfTotal
FROM clean.clean_deliveries
GROUP BY DeliveryStatus
ORDER BY RecordCount DESC;

-- IsLate distribution (confirms binary flag is populated)
SELECT
    IsLate,
    COUNT(*)                                            AS RecordCount
FROM clean.clean_deliveries
GROUP BY IsLate
ORDER BY IsLate;

-- Route efficiency summary
-- StopEfficiencyPct: how many actual stops vs planned (%)
-- HourEfficiencyPct: how well time was used vs actual (%)
SELECT
    MIN(StopEfficiencyPct)                              AS MinStopEfficiency,
    MAX(StopEfficiencyPct)                              AS MaxStopEfficiency,
    ROUND(AVG(StopEfficiencyPct), 2)                    AS AvgStopEfficiency,
    MIN(HourEfficiencyPct)                              AS MinHourEfficiency,
    MAX(HourEfficiencyPct)                              AS MaxHourEfficiency,
    ROUND(AVG(HourEfficiencyPct), 2)                    AS AvgHourEfficiency,
    MIN(StopVariance)                                   AS MinStopVariance,
    MAX(StopVariance)                                   AS MaxStopVariance,
    ROUND(AVG(CAST(StopVariance AS DECIMAL(10,2))), 2)  AS AvgStopVariance
FROM clean.clean_routes;

-- Unknown DriverID count in clean_routes
-- Reports the extent of missing driver data that was replaced
-- with 'Unknown' during the clean layer transformation.
SELECT
    COUNT(*)                                            AS UnknownDriverCount
FROM clean.clean_routes
WHERE DriverID = 'Unknown';

-- Exception type distribution
SELECT
    ExceptionType,
    COUNT(*)                                            AS RecordCount,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(),
    2)                                                  AS PctOfTotal
FROM clean.clean_exceptions
GROUP BY ExceptionType
ORDER BY RecordCount DESC;

-- Open vs resolved exceptions
SELECT
    IsResolved,
    COUNT(*)                                            AS RecordCount
FROM clean.clean_exceptions
GROUP BY IsResolved
ORDER BY IsResolved;


/*=============================================================
  PROFILING COMPLETE
=============================================================*/

PRINT '--- CLEAN LAYER DATA PROFILING END ---';
PRINT 'Review all result sets carefully before loading DW tables.';
PRINT 'For a hard pipeline stop on failures, run 07_clean_validation_gate_v3.0.sql.';
GO
