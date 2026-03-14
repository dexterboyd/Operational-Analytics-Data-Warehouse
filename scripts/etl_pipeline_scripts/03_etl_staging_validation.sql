/*=============================================================
  STAGING LAYER VALIDATION
  Database: Fedex_Ops_Database
  Version:  2.0

  Purpose:
      Validate raw staging data after bulk load and before
      any clean-layer transformations are applied. Designed
      to run as a SQL Agent job step; THROW halts the job
      if critical checks fail.

  Checks Performed:
      1. Empty table guard       -- all four tables must have rows
      2. NULL value checks       -- key identifiers must not be null
      3. Negative value checks   -- numeric metrics must be >= 0
      4. Referential integrity   -- DeliveryIDs in sales and
                                    exceptions must exist in
                                    staging_deliveries
      5. Date range sanity       -- dates must be within plausible
                                    bounds and correctly ordered

  Behavior:
      - SET XACT_ABORT ON ensures any open transaction is rolled
        back if THROW fires inside a transaction context.
      - @FailureCount accumulates across all checks.
      - If @FailureCount > 0 the script calls THROW, which halts
        execution and cancels the downstream clean-layer load.
      - Set @EnableStagingValidation = 0 to skip all checks
        (e.g. during initial schema setup before data is loaded).
        NOTE: For pipeline automation, prefer controlling this
        via a SQL Agent job step parameter or a config table
        rather than editing the script directly.

  Pipeline Flow:
      staging -> [THIS SCRIPT] -> clean -> dw -> reporting

  Change Log:
      v2.0 - Replaced RAISERROR severity 10 (informational) with
             a THROW-based failure gate. Severity 10 does not
             halt execution; the pipeline previously continued
             regardless of check results.
           - Added SET XACT_ABORT ON and SET LOCK_TIMEOUT.
           - Added Check 1: empty table guard. Without it, an
             empty table after a failed bulk load causes all
             downstream NULL checks to pass vacuously.
           - Consolidated each table's NULL checks into a single
             SELECT pass (was 2-3 separate queries per table).
           - Added Check 3: negative/zero value checks on
             UnitsSold, SalesAmount, PlannedStops, ActualStops,
             PlannedHours, ActualHours, ResolutionTimeHours.
           - Added Check 4: referential integrity for DeliveryID
             across staging_sales and staging_exceptions vs
             staging_deliveries.
           - Added Check 5: date range and chronology checks
             (out-of-range dates, future delivery dates,
             ResolvedDate before DateReported).
           - @EnableStagingValidation documented as a script-level
             toggle; noted that pipeline automation should use a
             config table or job parameter instead.
=============================================================*/

SET XACT_ABORT ON;

-- Abort any scan blocked longer than 30 seconds.
-- Adjust or remove if long-running queries are expected.
SET LOCK_TIMEOUT 30000;

/*=============================================================
  VALIDATION TOGGLE
  Set to 0 to skip all checks (e.g. during schema setup before
  data is loaded). Leave at 1 for normal pipeline execution.
=============================================================*/
DECLARE @EnableStagingValidation BIT = 1;

IF @EnableStagingValidation = 0
BEGIN
    PRINT 'STAGING VALIDATION SKIPPED (CONFIG DISABLED)';
    RETURN;
END

/*=============================================================
  CONTROL VARIABLES
=============================================================*/
DECLARE @FailureCount INT       = 0;
DECLARE @CheckName    NVARCHAR(200);
DECLARE @BadRows      INT;

PRINT '=====================================';
PRINT 'RUNNING STAGING LAYER VALIDATION';
PRINT '=====================================';


/*=============================================================
  CHECK 1: EMPTY TABLE GUARD
  Purpose:
      A zero-row table almost always means the bulk load
      failed silently. All downstream NULL and range checks
      would pass vacuously on an empty table, giving a false
      all-clear. Fail immediately if any table is empty.
=============================================================*/
PRINT '--- CHECK 1: EMPTY TABLE GUARD ---';

SET @CheckName = 'staging_sales — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM staging.staging_sales;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_deliveries — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM staging.staging_deliveries;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_routes — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM staging.staging_routes;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_exceptions — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM staging.staging_exceptions;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- Informational row count summary (always printed, not a gate check)
PRINT 'Row counts:';
SELECT 'staging_sales'       AS TableName, COUNT(*) AS RowsCount FROM staging.staging_sales
UNION ALL
SELECT 'staging_deliveries',               COUNT(*) FROM staging.staging_deliveries
UNION ALL
SELECT 'staging_routes',                   COUNT(*) FROM staging.staging_routes
UNION ALL
SELECT 'staging_exceptions',               COUNT(*) FROM staging.staging_exceptions;


/*=============================================================
  CHECK 2: NULL VALUE CHECKS
  Purpose:
      Ensure primary identifiers and required fields are
      populated. Each table is scanned once using conditional
      aggregation to avoid multiple passes.
=============================================================*/
PRINT '--- CHECK 2: NULL VALUE CHECKS ---';

-- Sales: all key fields in one pass
SET @CheckName = 'staging_sales — no NULL key fields';
SELECT @BadRows =
    SUM(CASE WHEN SalesID    IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN DateKey    IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN UnitsSold  IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN SalesAmount IS NULL THEN 1 ELSE 0 END)
FROM staging.staging_sales;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad field instances = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- NULL detail for investigation (only printed when failures exist)
IF @BadRows > 0
    SELECT
        'staging_sales'                                                          AS TableName,
        SUM(CASE WHEN SalesID    IS NULL THEN 1 ELSE 0 END)                     AS NullSalesID,
        SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END)                     AS NullDeliveryID,
        SUM(CASE WHEN DateKey    IS NULL THEN 1 ELSE 0 END)                     AS NullDateKey,
        SUM(CASE WHEN UnitsSold  IS NULL THEN 1 ELSE 0 END)                     AS NullUnitsSold,
        SUM(CASE WHEN SalesAmount IS NULL THEN 1 ELSE 0 END)                    AS NullSalesAmount
    FROM staging.staging_sales;

-- Deliveries
SET @CheckName = 'staging_deliveries — no NULL key fields';
SELECT @BadRows =
    SUM(CASE WHEN DeliveryID           IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN RouteID              IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN DriverID             IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN DeliveryDate         IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN DeliveryStatus       IS NULL THEN 1 ELSE 0 END)
FROM staging.staging_deliveries;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad field instances = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

IF @BadRows > 0
    SELECT
        'staging_deliveries'                                                     AS TableName,
        SUM(CASE WHEN DeliveryID           IS NULL THEN 1 ELSE 0 END)           AS NullDeliveryID,
        SUM(CASE WHEN RouteID              IS NULL THEN 1 ELSE 0 END)           AS NullRouteID,
        SUM(CASE WHEN DriverID             IS NULL THEN 1 ELSE 0 END)           AS NullDriverID,
        SUM(CASE WHEN DeliveryDate         IS NULL THEN 1 ELSE 0 END)           AS NullDeliveryDate,
        SUM(CASE WHEN DeliveryStatus       IS NULL THEN 1 ELSE 0 END)           AS NullDeliveryStatus
    FROM staging.staging_deliveries;

-- Routes
SET @CheckName = 'staging_routes — no NULL key fields';
SELECT @BadRows =
    SUM(CASE WHEN RouteID  IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN DriverID IS NULL THEN 1 ELSE 0 END)
FROM staging.staging_routes;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad field instances = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

IF @BadRows > 0
    SELECT
        'staging_routes'                                                         AS TableName,
        SUM(CASE WHEN RouteID  IS NULL THEN 1 ELSE 0 END)                       AS NullRouteID,
        SUM(CASE WHEN DriverID IS NULL THEN 1 ELSE 0 END)                       AS NullDriverID
    FROM staging.staging_routes;

-- Exceptions
SET @CheckName = 'staging_exceptions — no NULL key fields';
SELECT @BadRows =
    SUM(CASE WHEN ExceptionID  IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN DeliveryID   IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN DateReported IS NULL THEN 1 ELSE 0 END)
FROM staging.staging_exceptions;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad field instances = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

IF @BadRows > 0
    SELECT
        'staging_exceptions'                                                     AS TableName,
        SUM(CASE WHEN ExceptionID  IS NULL THEN 1 ELSE 0 END)                   AS NullExceptionID,
        SUM(CASE WHEN DeliveryID   IS NULL THEN 1 ELSE 0 END)                   AS NullDeliveryID,
        SUM(CASE WHEN DateReported IS NULL THEN 1 ELSE 0 END)                   AS NullDateReported
    FROM staging.staging_exceptions;


/*=============================================================
  CHECK 3: NEGATIVE AND ZERO VALUE CHECKS
  Purpose:
      Negative or zero values in numeric operational columns
      indicate CSV parsing errors or upstream data problems.
      A negative unit count or negative route hours is never
      a legitimate source value.
=============================================================*/
PRINT '--- CHECK 3: NEGATIVE / ZERO VALUE CHECKS ---';

SET @CheckName = 'staging_sales — UnitsSold and SalesAmount > 0';
SELECT @BadRows = COUNT(*)
FROM staging.staging_sales
WHERE UnitsSold  <= 0
   OR SalesAmount <= 0;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_routes — stops and hours > 0';
SELECT @BadRows = COUNT(*)
FROM staging.staging_routes
WHERE PlannedStops  <= 0
   OR ActualStops   <= 0
   OR PlannedHours  <= 0
   OR ActualHours   <= 0;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_exceptions — ResolutionTimeHours >= 0 when present';
SELECT @BadRows = COUNT(*)
FROM staging.staging_exceptions
WHERE ResolutionTimeHours IS NOT NULL
  AND ResolutionTimeHours < 0;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 4: REFERENTIAL INTEGRITY
  Purpose:
      DeliveryIDs in staging_sales and staging_exceptions must
      have a matching row in staging_deliveries. Orphaned IDs
      will silently lose rows during all downstream joins.
=============================================================*/
PRINT '--- CHECK 4: REFERENTIAL INTEGRITY ---';

SET @CheckName = 'staging_sales — DeliveryID exists in staging_deliveries';
SELECT @BadRows = COUNT(*)
FROM staging.staging_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM staging.staging_deliveries d
    WHERE d.DeliveryID = s.DeliveryID
);
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_exceptions — DeliveryID exists in staging_deliveries';
SELECT @BadRows = COUNT(*)
FROM staging.staging_exceptions e
WHERE NOT EXISTS (
    SELECT 1
    FROM staging.staging_deliveries d
    WHERE d.DeliveryID = e.DeliveryID
);
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 5: DATE RANGE AND CHRONOLOGY CHECKS
  Purpose:
      Catch date formatting errors from BULK INSERT (e.g. a
      year parsed as 0001 or 9999) and logical errors like
      a resolution date before the reported date.
=============================================================*/
PRINT '--- CHECK 5: DATE RANGE AND CHRONOLOGY ---';

SET @CheckName = 'staging_deliveries — DeliveryDate within plausible range';
SELECT @BadRows = COUNT(*)
FROM staging.staging_deliveries
WHERE DeliveryDate < '2000-01-01'
   OR DeliveryDate > CAST(GETDATE() AS DATE);
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_deliveries — ExpectedDeliveryDate within plausible range';
SELECT @BadRows = COUNT(*)
FROM staging.staging_deliveries
WHERE ExpectedDeliveryDate IS NOT NULL
  AND (ExpectedDeliveryDate < '2000-01-01'
       OR ExpectedDeliveryDate > DATEADD(YEAR, 1, CAST(GETDATE() AS DATE)));
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_sales — DateKey within plausible range';
SELECT @BadRows = COUNT(*)
FROM staging.staging_sales
WHERE DateKey < '2000-01-01'
   OR DateKey > CAST(GETDATE() AS DATE);
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'staging_exceptions — ResolvedDate not before DateReported';
SELECT @BadRows = COUNT(*)
FROM staging.staging_exceptions
WHERE ResolvedDate IS NOT NULL
  AND ResolvedDate < DateReported;
IF @BadRows > 0
BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  FINAL GATE
  Halt the pipeline if any check failed. The THROW will
  propagate to the SQL Agent job step or orchestration layer
  and prevent the clean-layer load from running.
=============================================================*/
IF @FailureCount > 0
BEGIN
    PRINT '=====================================';
    PRINT 'STAGING VALIDATION FAILED: ' + CAST(@FailureCount AS VARCHAR) + ' check(s) failed.';
    PRINT 'Clean layer load cancelled.';
    PRINT '=====================================';
    THROW 51001, 'Staging Layer Validation Failed. Clean Layer Load Cancelled.', 1;
END
ELSE
BEGIN
    PRINT '=====================================';
    PRINT 'STAGING VALIDATION PASSED — all checks passed.';
    PRINT 'Proceeding to clean layer.';
    PRINT '=====================================';
END;
