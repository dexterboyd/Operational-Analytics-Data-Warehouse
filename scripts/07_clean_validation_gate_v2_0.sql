/*==============================================================
  CLEAN LAYER VALIDATION GATE
  Schema: clean
  Version: 2.0

  PURPOSE
  -------
  Production data quality gate before DW load. Executes a
  series of checks and calls THROW to abort the pipeline if
  any check fails. Designed to be called from a SQL Agent
  job step or pipeline orchestrator.

  BEHAVIOR
  --------
  - Each check sets @BadRows and logs PASS or FAIL
  - @FailureCount accumulates across all checks
  - If @FailureCount > 0 at the end, THROW halts execution
    and the DW load step should not proceed
  - SET XACT_ABORT ON ensures any open transaction is rolled
    back if the THROW fires inside a transaction context

  CHECKS PERFORMED
  ----------------
  1.  Empty table guard   — all four clean views must have rows
  2.  Sales required fields
  3.  Delivery required fields
  4.  Late delivery business rule
  5.  PriorityFlag normalization
  6.  Route hours validity  (checked against staging to avoid
                              vacuous pass from view filter)
  7.  Referential integrity — vw_sales.DeliveryID
  8.  Referential integrity — vw_exceptions.DeliveryID

  PIPELINE STAGE
  --------------
  staging -> clean -> [THIS GATE] -> dw -> reporting -> Power BI

  CHANGE LOG
  ----------
  v2.0 - Added SET XACT_ABORT ON for safe transactional use.
       - Added SET LOCK_TIMEOUT to prevent silent pipeline stall
         on blocked scans.
       - Added Check 1: empty-table guard for all four clean
         views. An empty view means a silent bulk load failure;
         without this, all downstream null checks pass vacuously.
       - Fixed Check 4: changed 'Late' to 'LATE' to match the
         value produced by vw_deliveries (critical bug fix —
         the old value caused every run to fail).
       - Fixed Check 5 (was Check 4): route hours check now
         queries staging.staging_routes directly instead of
         vw_routes. The view's own WHERE already excludes
         invalid hours, so checking the view was a vacuous pass.
       - Added Check 6: referential integrity for vw_sales.
       - Added Check 7: referential integrity for vw_exceptions.
       - Removed duplicate commented block (versioning artifact).
==============================================================*/

SET XACT_ABORT ON;

-- Abort if any single scan blocks for more than 30 seconds.
-- Adjust or remove this line if long-running queries are expected.
SET LOCK_TIMEOUT 30000;

PRINT '===== CLEAN VALIDATION GATE START =====';

/*==============================================================
  CONTROL VARIABLES
==============================================================*/
DECLARE @FailureCount INT       = 0;
DECLARE @CheckName    NVARCHAR(200);
DECLARE @BadRows      INT;


/*==============================================================
  CHECK 1: EMPTY TABLE GUARD
  Purpose:
      If any clean view is empty the bulk load likely failed
      silently. All downstream null and rule checks would pass
      vacuously on an empty dataset, giving a false all-clear.
      Fail immediately if any view returns zero rows.
==============================================================*/
SET @CheckName = 'vw_sales — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM clean.vw_sales;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | View contains zero rows — bulk load may have failed';
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'vw_deliveries — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM clean.vw_deliveries;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | View contains zero rows — bulk load may have failed';
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'vw_exceptions — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM clean.vw_exceptions;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | View contains zero rows — bulk load may have failed';
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'vw_routes — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM clean.vw_routes;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | View contains zero rows — bulk load may have failed';
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 2: SALES REQUIRED FIELDS
  Purpose:
      Ensure DateKey, SalesAmount, and UnitsSold are populated
      on every row that passed the view filter.
==============================================================*/
SET @CheckName = 'vw_sales — required fields not null';

SELECT @BadRows = COUNT(*)
FROM clean.vw_sales
WHERE DateKey     IS NULL
   OR SalesAmount IS NULL
   OR UnitsSold   IS NULL;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 3: DELIVERY REQUIRED FIELDS
  Purpose:
      Ensure key delivery identifiers are populated on every
      row that passed the view filter.
==============================================================*/
SET @CheckName = 'vw_deliveries — required fields not null';

SELECT @BadRows = COUNT(*)
FROM clean.vw_deliveries
WHERE DeliveryID IS NULL
   OR RouteID    IS NULL
   OR DriverID   IS NULL;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 4: LATE DELIVERY BUSINESS RULE
  Purpose:
      Ensure every delivery where DeliveryDate > ExpectedDeliveryDate
      has DeliveryStatus = 'LATE'.
      IMPORTANT: compare against 'LATE' (all caps) — that is the
      exact value produced by vw_deliveries.
==============================================================*/
SET @CheckName = 'vw_deliveries — late delivery flag is LATE';

SELECT @BadRows = COUNT(*)
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'LATE';

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 5: PRIORITY FLAG NORMALIZATION
  Purpose:
      Ensure PriorityFlag contains only 0 or 1 after
      normalization in vw_deliveries.
==============================================================*/
SET @CheckName = 'vw_deliveries — PriorityFlag is 0 or 1';

SELECT @BadRows = COUNT(*)
FROM clean.vw_deliveries
WHERE PriorityFlag NOT IN (0, 1);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 6: ROUTE HOURS VALIDITY (CHECKED AGAINST STAGING)
  Purpose:
      Verify that no routes with zero or negative hours exist
      in the source data BEFORE the view filter removes them.
      Querying vw_routes for this condition would always return
      0 because the view already excludes those rows — a
      vacuous pass that guards nothing.

      A non-zero count here means bad source rows were silently
      dropped by the view filter and were never investigated.
      Decide whether to treat this as a hard failure or an
      informational warning based on your data SLA.
==============================================================*/
SET @CheckName = 'staging_routes — invalid hours in source data';

SELECT @BadRows = COUNT(*)
FROM staging.staging_routes
WHERE RouteID  IS NOT NULL
  AND DriverID IS NOT NULL
  AND (PlannedHours <= 0 OR ActualHours <= 0
       OR PlannedStops <= 0 OR ActualStops <= 0);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Rows with invalid metrics = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 7: REFERENTIAL INTEGRITY — vw_sales.DeliveryID
  Purpose:
      Every DeliveryID in vw_sales must have a matching row
      in vw_deliveries. Orphaned IDs cause silent row loss
      during DW fact table joins.
==============================================================*/
SET @CheckName = 'vw_sales — DeliveryID exists in vw_deliveries';

SELECT @BadRows = COUNT(*)
FROM clean.vw_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.vw_deliveries d
    WHERE d.DeliveryID = s.DeliveryID
);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Orphaned DeliveryID count = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 8: REFERENTIAL INTEGRITY — vw_exceptions.DeliveryID
  Purpose:
      Every DeliveryID in vw_exceptions must have a matching
      row in vw_deliveries. Same orphan risk as Check 7.
==============================================================*/
SET @CheckName = 'vw_exceptions — DeliveryID exists in vw_deliveries';

SELECT @BadRows = COUNT(*)
FROM clean.vw_exceptions e
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.vw_deliveries d
    WHERE d.DeliveryID = e.DeliveryID
);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Orphaned DeliveryID count = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;


/*==============================================================
  FINAL PIPELINE DECISION
  Purpose:
      Halt the pipeline if any check failed. The THROW will
      propagate to the calling SQL Agent job step or
      orchestration layer and cancel the DW load.
==============================================================*/
IF @FailureCount > 0
BEGIN
    PRINT '===== CLEAN VALIDATION FAILED: ' + CAST(@FailureCount AS VARCHAR) + ' check(s) failed =====';
    THROW 51000, 'Clean Layer Validation Failed. DW Load Cancelled.', 1;
END
ELSE
BEGIN
    PRINT '===== CLEAN VALIDATION PASSED — all ' +
          CAST(8 AS VARCHAR) + ' checks passed =====';
END;
