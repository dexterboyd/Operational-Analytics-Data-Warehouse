/*=============================================================
  CLEAN LAYER VALIDATION GATE
  Database: Fedex_Ops_Database
  Version:  3.0

  Purpose:
      Production data quality gate before DW load. Executes a
      series of checks and calls THROW to abort the pipeline
      if any check fails.

  Behavior:
      - Each check sets @BadRows and logs PASS or FAIL
      - @FailureCount accumulates across all checks
      - If @FailureCount > 0 at the end, THROW halts execution
        and the DW load step should not proceed
      - SET XACT_ABORT ON ensures any open transaction is
        rolled back if THROW fires inside a transaction context

  Run Order:
      1. etl_staging_setup_v5.sql          -- build schemas/tables
      2. load_staging.py                   -- load CSV data
      3. staging_layer_validation_v2.sql   -- validate staging
      4. clean_layer_v1.sql                -- build clean views
      5. clean_layer_data_profiling        -- profile clean data
      6. clean_layer_validation            -- human review
      7. THIS SCRIPT                       -- pipeline gate

  Clean Layer Views Referenced:
      clean.clean_sales        -- standardized sales transactions
      clean.clean_deliveries   -- standardized delivery records
      clean.clean_routes       -- route performance with surrogate key
      clean.clean_exceptions   -- standardized exception records
=============================================================*/

SET XACT_ABORT ON;

-- Abort if any single scan blocks for more than 30 seconds.
-- Adjust or remove if long-running queries are expected in
-- your environment.
SET LOCK_TIMEOUT 30000;

USE Fedex_Ops_Database;
GO

PRINT '===== CLEAN VALIDATION GATE START =====';

/*=============================================================
  CONTROL VARIABLES
=============================================================*/

DECLARE @FailureCount INT        = 0;
DECLARE @CheckName    NVARCHAR(200);
DECLARE @BadRows      INT;

/*=============================================================
  CHECK 1: EMPTY TABLE GUARD
  Purpose:
      If any clean view returns zero rows the bulk load likely
      failed silently. All downstream null and rule checks
      would pass vacuously on an empty dataset, producing a
      false all-clear. Fail immediately if any view is empty.
=============================================================*/

SET @CheckName = 'clean_sales — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM clean.clean_sales;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | View contains zero rows -- bulk load may have failed';
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'clean_deliveries — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM clean.clean_deliveries;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | View contains zero rows -- bulk load may have failed';
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'clean_exceptions — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM clean.clean_exceptions;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | View contains zero rows -- bulk load may have failed';
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'clean_routes — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM clean.clean_routes;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | View contains zero rows -- bulk load may have failed';
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 2: SALES REQUIRED FIELDS
  Purpose:
      Ensure DateKey, SalesAmount, and UnitsSold are populated
      on every row. NULL values in these columns would break
      fact table aggregations downstream.
=============================================================*/

SET @CheckName = 'clean_sales — required fields not null';

SELECT @BadRows = COUNT(*)
FROM clean.clean_sales
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

/*=============================================================
  CHECK 3: DELIVERY REQUIRED FIELDS
  Purpose:
      Ensure key delivery identifiers and dates are populated.
      DriverID is excluded -- the view guarantees 'Unknown'
      not NULL, so a null check would always pass vacuously.
=============================================================*/

SET @CheckName = 'clean_deliveries — required fields not null';

SELECT @BadRows = COUNT(*)
FROM clean.clean_deliveries
WHERE DeliveryID     IS NULL
   OR RouteID        IS NULL
   OR DeliveryDate   IS NULL
   OR DeliveryStatus IS NULL;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 4: ISLATE FLAG — LATE AND EXCEPTION DELIVERIES
  Purpose:
      Every delivery with DeliveryStatus of 'Late' or
      'Exception' must have IsLate = 1. Any row returned
      here means the binary flag was not set correctly.

      IMPORTANT: DeliveryStatus values use mixed case exactly
      as they appear in staging -- 'Late' not 'LATE'.
=============================================================*/

SET @CheckName = 'clean_deliveries — IsLate = 1 for Late and Exception rows';

SELECT @BadRows = COUNT(*)
FROM clean.clean_deliveries
WHERE DeliveryStatus IN ('Late', 'Exception')
  AND IsLate <> 1;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 5: ISLATE FLAG — ON-TIME DELIVERIES
  Purpose:
      Every delivery with DeliveryStatus 'On-Time' must have
      IsLate = 0. This check catches false positives that
      Check 4 cannot -- a row flagged as late when it should
      not be.
=============================================================*/

SET @CheckName = 'clean_deliveries — IsLate = 0 for On-Time rows';

SELECT @BadRows = COUNT(*)
FROM clean.clean_deliveries
WHERE DeliveryStatus = 'On-Time'
  AND IsLate <> 0;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 6: PRIORITYFLAG NORMALIZATION
  Purpose:
      PriorityFlag must contain only 0 or 1 after loading
      from the BIT source column. Any other value indicates
      a type conversion issue during the Python load.
=============================================================*/

SET @CheckName = 'clean_deliveries — PriorityFlag is 0 or 1';

SELECT @BadRows = COUNT(*)
FROM clean.clean_deliveries
WHERE PriorityFlag NOT IN (0, 1);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 7: ISRESOLVED FLAG ACCURACY
  Purpose:
      Every exception with a populated ResolvedDate must have
      IsResolved = 1. Rows returned here mean the binary flag
      was not set correctly by the clean layer view.
=============================================================*/

SET @CheckName = 'clean_exceptions — IsResolved = 1 where ResolvedDate is populated';

SELECT @BadRows = COUNT(*)
FROM clean.clean_exceptions
WHERE ResolvedDate IS NOT NULL
  AND IsResolved <> 1;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 8: TRUNCATION CHECK
  Purpose:
      Confirm that no abbreviated values remain in the clean
      layer after transformation. The clean layer views expand
      all truncated values (e.g. 'L.' -> 'Large Package').
      Any row returned here means a mapping was missed.
=============================================================*/

SET @CheckName = 'clean layer — no truncated values remain';

SELECT @BadRows =
    (SELECT COUNT(*) FROM clean.clean_sales      WHERE ProductType   LIKE '_.')
  + (SELECT COUNT(*) FROM clean.clean_sales      WHERE Region        LIKE '_.')
  + (SELECT COUNT(*) FROM clean.clean_deliveries WHERE ShipmentType  LIKE '_.')
  + (SELECT COUNT(*) FROM clean.clean_deliveries WHERE Region        LIKE '_.')
  + (SELECT COUNT(*) FROM clean.clean_exceptions WHERE ExceptionType LIKE '_.')
  + (SELECT COUNT(*) FROM clean.clean_routes     WHERE Region        LIKE '_.');

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Truncated value rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 9: ROUTE SOURCE DATA VALIDITY
  Purpose:
      Verify that no routes with zero or negative stops/hours
      exist in staging before the clean layer view processes
      them. Checking clean_routes for this condition would
      always return 0 if the view filters those rows out --
      a vacuous pass that guards nothing. Querying staging
      directly ensures bad source rows are flagged even if
      the view silently dropped them.
=============================================================*/

SET @CheckName = 'staging_routes — no invalid stops or hours in source data';

SELECT @BadRows = COUNT(*)
FROM staging.staging_routes
WHERE RouteID  IS NOT NULL
  AND DriverID IS NOT NULL
  AND (   PlannedStops <= 0
       OR ActualStops  <= 0
       OR PlannedHours <= 0
       OR ActualHours  <= 0);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Rows with invalid metrics = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 10: REFERENTIAL INTEGRITY — clean_sales.DeliveryID
  Purpose:
      Every DeliveryID in clean_sales must have a matching
      row in clean_deliveries. Orphaned DeliveryIDs cause
      silent row loss during DW fact table joins.
=============================================================*/

SET @CheckName = 'clean_sales — DeliveryID exists in clean_deliveries';

SELECT @BadRows = COUNT(*)
FROM clean.clean_sales s
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.clean_deliveries d
    WHERE d.DeliveryID = s.DeliveryID
);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Orphaned DeliveryID count = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  CHECK 11: REFERENTIAL INTEGRITY — clean_exceptions.DeliveryID
  Purpose:
      Every DeliveryID in clean_exceptions must have a matching
      row in clean_deliveries. Same orphan risk as Check 10.
=============================================================*/

SET @CheckName = 'clean_exceptions — DeliveryID exists in clean_deliveries';

SELECT @BadRows = COUNT(*)
FROM clean.clean_exceptions e
WHERE NOT EXISTS (
    SELECT 1
    FROM clean.clean_deliveries d
    WHERE d.DeliveryID = e.DeliveryID
);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Orphaned DeliveryID count = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*=============================================================
  FINAL PIPELINE DECISION
  Purpose:
      Halt the pipeline if any check failed. The THROW will
      propagate to the calling SQL Agent job step or
      orchestration layer and cancel the DW load. All 11
      checks must pass before the DW load is permitted.
=============================================================*/

IF @FailureCount > 0
BEGIN
    PRINT '===== CLEAN VALIDATION FAILED: '
        + CAST(@FailureCount AS VARCHAR)
        + ' of 11 check(s) failed -- DW Load Cancelled =====';
    THROW 51000, 'Clean Layer Validation Failed. DW Load Cancelled.', 1;
END
ELSE
BEGIN
    PRINT '===== CLEAN VALIDATION PASSED — all 11 checks passed =====';
    PRINT 'Pipeline cleared to proceed with DW load.';
END;
GO
