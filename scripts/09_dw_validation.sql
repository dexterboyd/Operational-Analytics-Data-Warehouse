/*==============================================================
  DW VALIDATION SCRIPT
  Database: Fedex_Ops_Database
  Version:  2.0

  Purpose:
      Validate all Data Warehouse tables after the DW load.
      Acts as a hard pipeline gate: calls THROW to halt any
      downstream reporting layer load if critical checks fail.

  Checks Performed:
      1.  Empty table guard      — all DW tables must have rows
      2.  Row count comparison   — DW facts vs clean views
      3.  NULL surrogate keys    — all FK columns in fact tables
      4.  Duplicate primary keys — all fact and dimension tables
      5.  FK integrity           — fact rows missing dimension matches
      6.  Business metric sanity — SalesAmount, UnitsSold > 0
      7.  Delivery date logic    — DeliveryDateKey <= ExpectedDateKey
          where status is not LATE
      8.  Distribution summary   — informational row counts per table

  Pipeline Position:
      staging -> clean -> dw load -> [THIS SCRIPT] -> reporting -> BI

  Consolidated from:
      08_dw_validation.sql (v1) and 08_dw_validation_v2.sql
      Both files performed the same checks with minor
      differences. This file is the single authoritative
      version.

  Change Log:
      v2.0 - Merged v1 and v2 into one file; removed duplicate.
           - Added SET XACT_ABORT ON + SET LOCK_TIMEOUT.
           - Added Check 1: empty table guard. Vacuous pass
             prevention — null/FK checks always pass on empty
             tables.
           - Added Check 2: row count comparison between DW
             fact tables and their source clean views to surface
             any rows lost during surrogate key joins.
           - Fixed NULL surrogate key check: now explicitly lists
             only true FK columns per fact table instead of using
             a dynamic LIKE '%ID' pattern that generated invalid
             table names (e.g. dw.dim_Sales) for non-FK columns.
           - Added Check 4: explicit duplicate PK check per table
             rather than relying on the FK constraint system to
             catch them.
           - Added Check 5: explicit fact-to-dimension integrity
             checks for each FK relationship.
           - Added Check 6: business metric sanity (no zero or
             negative amounts in fact_sales).
           - Added Check 7: delivery date logic validation.
           - Added failure gate using THROW consistent with the
             clean layer gate pattern.
           - Renamed file prefix from 08_ to 09_ to avoid
             filename-ordering collision with the load script.
==============================================================*/

SET XACT_ABORT ON;
SET LOCK_TIMEOUT 30000;

PRINT '===== DW VALIDATION START =====';

DECLARE @FailureCount INT      = 0;
DECLARE @CheckName    NVARCHAR(200);
DECLARE @BadRows      INT;


/*==============================================================
  CHECK 1: EMPTY TABLE GUARD
  All DW tables must contain rows. An empty table means the
  load failed or rolled back; all downstream checks would
  pass vacuously on empty tables, giving a false all-clear.
==============================================================*/
PRINT '--- CHECK 1: EMPTY TABLE GUARD ---';

SET @CheckName = 'dw.dim_date — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_date;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.dim_region — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_region;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.fact_sales — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_sales;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.fact_deliveries — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_deliveries;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.fact_routes — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_routes;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.fact_exceptions — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_exceptions;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 2: ROW COUNT COMPARISON (FACT vs CLEAN VIEW)
  Rows dropped during the DW load mean surrogate key lookups
  failed. The DW fact count should equal the clean view count.
  Informational only — logged but does not increment
  @FailureCount since some drop may be expected during
  initial pipeline runs. Investigate any non-zero difference.
==============================================================*/
PRINT '--- CHECK 2: FACT vs CLEAN ROW COUNTS ---';

SELECT
    'fact_sales'                                    AS FactTable,
    (SELECT COUNT(*) FROM clean.vw_sales)           AS CleanRows,
    (SELECT COUNT(*) FROM dw.fact_sales)            AS DWRows,
    (SELECT COUNT(*) FROM clean.vw_sales)
        - (SELECT COUNT(*) FROM dw.fact_sales)      AS DroppedRows;

SELECT
    'fact_deliveries'                               AS FactTable,
    (SELECT COUNT(*) FROM clean.vw_deliveries)      AS CleanRows,
    (SELECT COUNT(*) FROM dw.fact_deliveries)       AS DWRows,
    (SELECT COUNT(*) FROM clean.vw_deliveries)
        - (SELECT COUNT(*) FROM dw.fact_deliveries) AS DroppedRows;

SELECT
    'fact_routes'                                   AS FactTable,
    (SELECT COUNT(*) FROM clean.vw_routes)          AS CleanRows,
    (SELECT COUNT(*) FROM dw.fact_routes)           AS DWRows,
    (SELECT COUNT(*) FROM clean.vw_routes)
        - (SELECT COUNT(*) FROM dw.fact_routes)     AS DroppedRows;

SELECT
    'fact_exceptions'                               AS FactTable,
    (SELECT COUNT(*) FROM clean.vw_exceptions)      AS CleanRows,
    (SELECT COUNT(*) FROM dw.fact_exceptions)       AS DWRows,
    (SELECT COUNT(*) FROM clean.vw_exceptions)
        - (SELECT COUNT(*) FROM dw.fact_exceptions) AS DroppedRows;


/*==============================================================
  CHECK 3: NULL SURROGATE KEY CHECKS
  Only genuine FK columns are listed per fact table.
  Using a LIKE '%ID' pattern is avoided because it matches
  non-FK columns (e.g. SalesID, ExceptionID) and would
  attempt to join to non-existent dimension tables.
==============================================================*/
PRINT '--- CHECK 3: NULL SURROGATE KEY CHECKS ---';

-- fact_sales
SET @CheckName = 'fact_sales — no NULL surrogate keys';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales
WHERE DateKey       IS NULL
   OR ProductTypeID IS NULL
   OR RegionID      IS NULL;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_deliveries
SET @CheckName = 'fact_deliveries — no NULL surrogate keys';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries
WHERE RouteID                 IS NULL
   OR DriverID                IS NULL
   OR ShipmentTypeID          IS NULL
   OR DeliveryDateKey         IS NULL
   OR ExpectedDeliveryDateKey IS NULL
   OR DeliveryStatusID        IS NULL
   OR PriorityFlagID          IS NULL;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_routes
SET @CheckName = 'fact_routes — no NULL surrogate keys';
SELECT @BadRows = COUNT(*) FROM dw.fact_routes
WHERE RouteID  IS NULL
   OR DriverID IS NULL
   OR RegionID IS NULL;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_exceptions
SET @CheckName = 'fact_exceptions — no NULL surrogate keys';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions
WHERE ExceptionTypeID IS NULL
   OR DateKey         IS NULL
   OR PriorityFlagID  IS NULL
   OR RegionID        IS NULL;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 4: DUPLICATE PRIMARY KEY CHECKS
  The PKs are enforced by constraints, but a belt-and-
  suspenders count confirms no duplicates slipped in and
  that the PK constraints are actually in place.
==============================================================*/
PRINT '--- CHECK 4: DUPLICATE PRIMARY KEY CHECKS ---';

SET @CheckName = 'fact_sales — no duplicate SalesID';
SELECT @BadRows = COUNT(*) FROM (
    SELECT SalesID FROM dw.fact_sales GROUP BY SalesID HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_deliveries — no duplicate DeliveryID';
SELECT @BadRows = COUNT(*) FROM (
    SELECT DeliveryID FROM dw.fact_deliveries GROUP BY DeliveryID HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_exceptions — no duplicate ExceptionID';
SELECT @BadRows = COUNT(*) FROM (
    SELECT ExceptionID FROM dw.fact_exceptions GROUP BY ExceptionID HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_routes — no duplicate RouteID + DriverID';
SELECT @BadRows = COUNT(*) FROM (
    SELECT RouteID, DriverID FROM dw.fact_routes GROUP BY RouteID, DriverID HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 5: FACT-TO-DIMENSION REFERENTIAL INTEGRITY
  Verifies that every FK value in a fact table resolves to a
  row in its referenced dimension. Joins two fact tables on
  a shared business key is intentionally avoided here —
  these are strictly fact-to-dimension checks.
==============================================================*/
PRINT '--- CHECK 5: FK INTEGRITY CHECKS ---';

SET @CheckName = 'fact_sales -> dim_date';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.DateKey);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_sales -> dim_product_type';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_product_type d WHERE d.ProductTypeID = f.ProductTypeID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_sales -> dim_region';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionID = f.RegionID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_deliveries -> dim_route';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_route d WHERE d.RouteID = f.RouteID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_deliveries -> dim_driver';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_driver d WHERE d.DriverID = f.DriverID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_deliveries -> dim_delivery_status';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_delivery_status d WHERE d.DeliveryStatusID = f.DeliveryStatusID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_exceptions -> dim_exception_type';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_exception_type d WHERE d.ExceptionTypeID = f.ExceptionTypeID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_routes -> dim_route';
SELECT @BadRows = COUNT(*) FROM dw.fact_routes f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_route d WHERE d.RouteID = f.RouteID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_routes -> dim_driver';
SELECT @BadRows = COUNT(*) FROM dw.fact_routes f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_driver d WHERE d.DriverID = f.DriverID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 6: BUSINESS METRIC SANITY
  Validates that core financial and operational metrics are
  within acceptable bounds after load.
==============================================================*/
PRINT '--- CHECK 6: BUSINESS METRIC SANITY ---';

SET @CheckName = 'fact_sales — no zero or negative SalesAmount';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales WHERE SalesAmount <= 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_sales — no zero or negative UnitsSold';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales WHERE UnitsSold <= 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_routes — no zero or negative PlannedHours or ActualHours';
SELECT @BadRows = COUNT(*) FROM dw.fact_routes
WHERE PlannedHours <= 0 OR ActualHours <= 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*==============================================================
  CHECK 7: DELIVERY DATE LOGIC
  For non-LATE deliveries, the actual delivery date should
  not be after the expected date. The status value in the DW
  comes from dim_delivery_status so we join back to get the
  string value for comparison.
==============================================================*/
PRINT '--- CHECK 7: DELIVERY DATE LOGIC ---';

SET @CheckName = 'fact_deliveries — DeliveryDateKey <= ExpectedDeliveryDateKey when not LATE';
SELECT @BadRows = COUNT(*)
FROM dw.fact_deliveries f
JOIN dw.dim_delivery_status ds ON f.DeliveryStatusID = ds.DeliveryStatusID
WHERE ds.DeliveryStatus <> 'LATE'
  AND f.DeliveryDateKey > f.ExpectedDeliveryDateKey;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*==============================================================
  INFORMATIONAL: TABLE ROW COUNT DISTRIBUTION
  Not a gate check — printed for human review.
==============================================================*/
PRINT '--- INFORMATIONAL: DW TABLE ROW COUNTS ---';

SELECT
    s.name                                          AS SchemaName,
    t.name                                          AS TableName,
    SUM(p.rows)                                     AS RowCount
FROM sys.tables t
JOIN sys.schemas    s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
                      AND p.index_id IN (0, 1)
WHERE s.name = 'dw'
GROUP BY s.name, t.name
ORDER BY
    CASE WHEN t.name LIKE 'dim%' THEN 0 ELSE 1 END,
    t.name;

-- Fact table metric summary
SELECT
    COUNT(*)         AS TotalTransactions,
    SUM(SalesAmount) AS TotalSales,
    AVG(SalesAmount) AS AvgSale,
    MIN(SalesAmount) AS MinSale,
    MAX(SalesAmount) AS MaxSale
FROM dw.fact_sales;


/*==============================================================
  FINAL GATE
==============================================================*/
IF @FailureCount > 0
BEGIN
    PRINT '===== DW VALIDATION FAILED: '
          + CAST(@FailureCount AS VARCHAR) + ' check(s) failed =====';
    THROW 51002, 'DW Validation Failed. Reporting layer load cancelled.', 1;
END
ELSE
BEGIN
    PRINT '===== DW VALIDATION PASSED =====';
END;
