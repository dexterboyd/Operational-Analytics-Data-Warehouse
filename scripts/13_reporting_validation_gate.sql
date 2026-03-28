/*=============================================================
  REPORTING LAYER VALIDATION GATE
  Database: Fedex_Ops_Database
  Version:  1.0

  Purpose:
      Final pipeline gate after the reporting layer is built.
      Executes a series of checks and calls THROW to halt
      execution if any check fails. A passing gate confirms
      all six reporting views are accessible, populated, and
      producing metrics consistent with the DW source data.

  Behavior:
      - Each check sets @BadRows and logs PASS or FAIL
      - @FailureCount accumulates across all checks
      - If @FailureCount > 0 at the end, THROW halts execution
      - SET XACT_ABORT ON ensures any open transaction is
        rolled back if THROW fires inside a transaction context

  Run Order:
      1. etl_staging_setup.sql          -- build schemas/tables
      2. load_staging.py                -- load CSV data
      3. staging_layer_validation.sql   -- validate staging
      4. clean_layer.sql                -- build clean views
      5. clean_validation_gate.sql      -- clean layer gate
      6. dw_layer.sql                   -- build DW tables
      7. dw_validation.sql              -- DW gate
      8. reporting_layer.sql            -- build reporting views
      9. THIS SCRIPT                    -- reporting gate

  Reporting Views Referenced:
      reporting.rpt_delivery_performance
      reporting.rpt_sales_performance
      reporting.rpt_exception_analysis
      reporting.rpt_driver_performance
      reporting.rpt_route_efficiency
      reporting.rpt_executive_summary

  Checks Performed:
      1.  Empty view guard         -- all six views must return rows
      2.  Delivery totals match    -- rpt_delivery_performance sums
                                      back to fact_deliveries count
      3.  Sales totals match       -- rpt_sales_performance revenue
                                      matches fact_sales total
      4.  Exception totals match   -- rpt_exception_analysis sums
                                      back to fact_exceptions count
      5.  Driver coverage          -- all named drivers appear in
                                      rpt_driver_performance
      6.  Route coverage           -- all 5 routes appear in
                                      rpt_route_efficiency
      7.  Rate range checks        -- all rate columns are 0-100
      8.  No negative metrics      -- revenue, units, counts >= 0
      9.  Executive summary sanity -- single row, no NULL KPIs
      10. Resolved + open = total  -- ResolvedCount + OpenCount
                                      equals TotalExceptions

=============================================================*/

SET XACT_ABORT ON;

-- Abort if any single scan blocks for more than 30 seconds.
-- Adjust or remove if long-running queries are expected.
SET LOCK_TIMEOUT 30000;

USE Fedex_Ops_Database;
GO

PRINT '===== REPORTING VALIDATION GATE START =====';

DECLARE @FailureCount INT        = 0;
DECLARE @CheckName    NVARCHAR(200);
DECLARE @BadRows      INT;


/*=============================================================
  CHECK 1: EMPTY VIEW GUARD
  All six reporting views must return rows. An empty view
  means the underlying DW tables are empty or the view
  definition is broken. All downstream metric checks would
  pass vacuously on empty views.
=============================================================*/

PRINT '--- CHECK 1: EMPTY VIEW GUARD ---';

SET @CheckName = 'rpt_delivery_performance — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM reporting.rpt_delivery_performance;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'rpt_sales_performance — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM reporting.rpt_sales_performance;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'rpt_exception_analysis — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM reporting.rpt_exception_analysis;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'rpt_driver_performance — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM reporting.rpt_driver_performance;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'rpt_route_efficiency — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM reporting.rpt_route_efficiency;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'rpt_executive_summary — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM reporting.rpt_executive_summary;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 2: DELIVERY TOTALS MATCH FACT TABLE
  The sum of TotalDeliveries across all rows in
  rpt_delivery_performance must equal the row count of
  fact_deliveries. A mismatch means rows were lost or
  double-counted during the GROUP BY aggregation.
=============================================================*/

PRINT '--- CHECK 2: DELIVERY TOTALS MATCH FACT TABLE ---';

SET @CheckName = 'rpt_delivery_performance — TotalDeliveries sums to fact_deliveries count';

DECLARE @RptDeliveries  INT;
DECLARE @FactDeliveries INT;

SELECT @RptDeliveries  = SUM(TotalDeliveries) FROM reporting.rpt_delivery_performance;
SELECT @FactDeliveries = COUNT(*)             FROM dw.fact_deliveries;

SELECT @BadRows = ABS(@RptDeliveries - @FactDeliveries);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName
        + ' | rpt=' + CAST(@RptDeliveries AS VARCHAR)
        + ' fact=' + CAST(@FactDeliveries AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 3: SALES REVENUE MATCHES FACT TABLE
  Total revenue in rpt_sales_performance must equal the sum
  of SalesAmount in fact_sales. A mismatch means revenue was
  lost or duplicated in the aggregation.
  A tolerance of 0.01 is applied to account for rounding
  differences between individual ROUND() calls and a single
  SUM() across all rows.
=============================================================*/

PRINT '--- CHECK 3: SALES REVENUE MATCHES FACT TABLE ---';

SET @CheckName = 'rpt_sales_performance — TotalRevenue matches fact_sales SalesAmount sum';

DECLARE @RptRevenue  DECIMAL(18,2);
DECLARE @FactRevenue DECIMAL(18,2);

SELECT @RptRevenue  = SUM(TotalRevenue) FROM reporting.rpt_sales_performance;
SELECT @FactRevenue = ROUND(SUM(SalesAmount), 2) FROM dw.fact_sales;

-- Allow up to 0.01 tolerance for rounding differences
SELECT @BadRows = CASE WHEN ABS(@RptRevenue - @FactRevenue) > 0.01 THEN 1 ELSE 0 END;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName
        + ' | rpt=' + CAST(@RptRevenue AS VARCHAR)
        + ' fact=' + CAST(@FactRevenue AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 4: EXCEPTION TOTALS MATCH FACT TABLE
  The sum of TotalExceptions across all rows in
  rpt_exception_analysis must equal the row count of
  fact_exceptions. A mismatch means exceptions were lost
  or double-counted in the aggregation.
=============================================================*/

PRINT '--- CHECK 4: EXCEPTION TOTALS MATCH FACT TABLE ---';

SET @CheckName = 'rpt_exception_analysis — TotalExceptions sums to fact_exceptions count';

DECLARE @RptExceptions  INT;
DECLARE @FactExceptions INT;

SELECT @RptExceptions  = SUM(TotalExceptions) FROM reporting.rpt_exception_analysis;
SELECT @FactExceptions = COUNT(*)             FROM dw.fact_exceptions;

SELECT @BadRows = ABS(@RptExceptions - @FactExceptions);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName
        + ' | rpt=' + CAST(@RptExceptions AS VARCHAR)
        + ' fact=' + CAST(@FactExceptions AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 5: DRIVER COVERAGE
  All 20 named drivers must appear in rpt_driver_performance.
  The Unknown driver row is also expected (IsUnknown = 1).
  A shortfall means at least one driver was dropped during
  the dim_driver to fact_deliveries join in the view.
=============================================================*/

PRINT '--- CHECK 5: DRIVER COVERAGE ---';

SET @CheckName = 'rpt_driver_performance — all 20 named drivers present';

-- Count named (non-Unknown) drivers in the reporting view
SELECT @BadRows = CASE WHEN COUNT(*) < 20 THEN 20 - COUNT(*) ELSE 0 END
FROM reporting.rpt_driver_performance
WHERE IsUnknown = 0;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Missing drivers = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'rpt_driver_performance — Unknown driver row present';

SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM reporting.rpt_driver_performance
WHERE IsUnknown = 1;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Unknown driver row missing';
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 6: ROUTE COVERAGE
  All 5 routes (R001-R005) must appear in rpt_route_efficiency.
  A shortfall means at least one route was dropped during the
  clean_routes to dim_route join in the view.
=============================================================*/

PRINT '--- CHECK 6: ROUTE COVERAGE ---';

SET @CheckName = 'rpt_route_efficiency — all 5 routes present (R001-R005)';

SELECT @BadRows = CASE WHEN COUNT(DISTINCT RouteID) < 5 THEN 5 - COUNT(DISTINCT RouteID) ELSE 0 END
FROM reporting.rpt_route_efficiency;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Missing routes = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 7: RATE COLUMN RANGE CHECKS
  All rate and percentage columns must be between 0 and 100.
  A value outside this range indicates a calculation error
  in the view definition.
=============================================================*/

PRINT '--- CHECK 7: RATE COLUMN RANGE CHECKS ---';

-- rpt_delivery_performance rate columns
SET @CheckName = 'rpt_delivery_performance — OnTimeRate, LateRate, ExceptionRate between 0 and 100';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_delivery_performance
WHERE OnTimeRate    < 0 OR OnTimeRate    > 100
   OR LateRate      < 0 OR LateRate      > 100
   OR ExceptionRate < 0 OR ExceptionRate > 100;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- rpt_exception_analysis resolution rate
SET @CheckName = 'rpt_exception_analysis — ResolutionRate between 0 and 100';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_exception_analysis
WHERE ResolutionRate < 0 OR ResolutionRate > 100;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- rpt_driver_performance rate columns
SET @CheckName = 'rpt_driver_performance — OnTimeRate, LateRate, ExceptionRate, PriorityRate between 0 and 100';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_driver_performance
WHERE OnTimeRate    < 0 OR OnTimeRate    > 100
   OR LateRate      < 0 OR LateRate      > 100
   OR ExceptionRate < 0 OR ExceptionRate > 100
   OR PriorityRate  < 0 OR PriorityRate  > 100;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 8: NO NEGATIVE METRICS
  Revenue, unit counts, and delivery counts must all be
  non-negative. A negative value indicates a calculation
  error or unexpected source data that slipped through.
=============================================================*/

PRINT '--- CHECK 8: NO NEGATIVE METRICS ---';

-- Sales revenue and units
SET @CheckName = 'rpt_sales_performance — TotalRevenue and TotalUnitsSold >= 0';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_sales_performance
WHERE TotalRevenue   < 0
   OR TotalUnitsSold < 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- Delivery counts
SET @CheckName = 'rpt_delivery_performance — TotalDeliveries, OnTimeCount, LateCount, ExceptionCount >= 0';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_delivery_performance
WHERE TotalDeliveries < 0
   OR OnTimeCount     < 0
   OR LateCount       < 0
   OR ExceptionCount  < 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- Exception counts and resolution hours
SET @CheckName = 'rpt_exception_analysis — TotalExceptions, ResolvedCount, AvgResolutionHours >= 0';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_exception_analysis
WHERE TotalExceptions    < 0
   OR ResolvedCount      < 0
   OR (AvgResolutionHours IS NOT NULL AND AvgResolutionHours < 0);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- Route stops and hours
SET @CheckName = 'rpt_route_efficiency — TotalPlannedStops, TotalActualStops, TotalPlannedHours, TotalActualHours >= 0';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_route_efficiency
WHERE TotalPlannedStops < 0
   OR TotalActualStops  < 0
   OR TotalPlannedHours < 0
   OR TotalActualHours  < 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 9: EXECUTIVE SUMMARY SANITY
  rpt_executive_summary must return exactly one row with no
  NULL values in any KPI column. A NULL KPI means the
  underlying subquery returned no rows, which indicates
  a broken DW table or an empty fact table.
=============================================================*/

PRINT '--- CHECK 9: EXECUTIVE SUMMARY SANITY ---';

-- Must return exactly one row
SET @CheckName = 'rpt_executive_summary — returns exactly one row';
SELECT @BadRows = CASE WHEN COUNT(*) <> 1 THEN 1 ELSE 0 END
FROM reporting.rpt_executive_summary;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- No NULL KPI columns
SET @CheckName = 'rpt_executive_summary — no NULL KPI values';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_executive_summary
WHERE TotalDeliveries        IS NULL
   OR OnTimeRate             IS NULL
   OR LateRate               IS NULL
   OR ExceptionRate          IS NULL
   OR PriorityDeliveries     IS NULL
   OR TotalSalesTransactions IS NULL
   OR TotalRevenue           IS NULL
   OR AvgRevenuePerTx        IS NULL
   OR TotalUnitsSold         IS NULL
   OR TotalExceptions        IS NULL
   OR ResolvedExceptions     IS NULL
   OR OpenExceptions         IS NULL
   OR OverallResolutionRate  IS NULL
   OR TotalRouteRuns         IS NULL
   OR AvgStopEfficiency      IS NULL
   OR AvgHourEfficiency      IS NULL;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | One or more KPI columns returned NULL';
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;

-- Delivery rates must sum to 100 (within 0.1 tolerance for rounding)
SET @CheckName = 'rpt_executive_summary — OnTimeRate + LateRate + ExceptionRate = 100';
SELECT @BadRows = CASE
    WHEN ABS((OnTimeRate + LateRate + ExceptionRate) - 100.0) > 0.1 THEN 1
    ELSE 0
END
FROM reporting.rpt_executive_summary;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Rates do not sum to 100';
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 10: RESOLVED + OPEN = TOTAL EXCEPTIONS
  For every row in rpt_exception_analysis, ResolvedCount +
  OpenCount must equal TotalExceptions. A mismatch means
  the CASE logic in the view is miscounting one category.
=============================================================*/

PRINT '--- CHECK 10: RESOLVED + OPEN = TOTAL EXCEPTIONS ---';

SET @CheckName = 'rpt_exception_analysis — ResolvedCount + OpenCount = TotalExceptions';
SELECT @BadRows = COUNT(*) FROM reporting.rpt_exception_analysis
WHERE (ResolvedCount + OpenCount) <> TotalExceptions;
IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  INFORMATIONAL: REPORTING LAYER SUMMARY
  Printed after all gate checks. Not a gate check itself --
  provides a quick view of row counts and key KPIs for
  human review before handing off to Power BI.
=============================================================*/

PRINT '--- INFORMATIONAL: REPORTING VIEW ROW COUNTS ---';

SELECT 'rpt_delivery_performance' AS ViewName, COUNT(*) AS RowsCount
FROM reporting.rpt_delivery_performance
UNION ALL
SELECT 'rpt_sales_performance',    COUNT(*)
FROM reporting.rpt_sales_performance
UNION ALL
SELECT 'rpt_exception_analysis',   COUNT(*)
FROM reporting.rpt_exception_analysis
UNION ALL
SELECT 'rpt_driver_performance',   COUNT(*)
FROM reporting.rpt_driver_performance
UNION ALL
SELECT 'rpt_route_efficiency',     COUNT(*)
FROM reporting.rpt_route_efficiency
UNION ALL
SELECT 'rpt_executive_summary',    COUNT(*)
FROM reporting.rpt_executive_summary;

-- Executive summary KPI snapshot for final review
PRINT '--- INFORMATIONAL: EXECUTIVE SUMMARY KPIs ---';
SELECT * FROM reporting.rpt_executive_summary;


/*=============================================================
  FINAL PIPELINE DECISION
  Halt execution if any check failed. THROW propagates to the
  calling SQL Agent job or orchestration layer. A passing gate
  confirms the reporting layer is ready for Power BI consumption.
=============================================================*/

IF @FailureCount > 0
BEGIN
    PRINT '===== REPORTING VALIDATION FAILED: '
        + CAST(@FailureCount AS VARCHAR)
        + ' check(s) failed -- Pipeline halted =====';
    THROW 51003, 'Reporting Layer Validation Failed. Pipeline halted.', 1;
END
ELSE
BEGIN
    PRINT '===== REPORTING VALIDATION PASSED — all 10 checks passed =====';
    PRINT 'Pipeline complete. Reporting layer is ready for Power BI consumption.';
END;
GO
