/*=============================================================
  DW VALIDATION GATE
  Database: Fedex_Ops_Database
  Version:  3.0

  Purpose:
      Hard pipeline gate after the DW load. Executes checks
      and calls THROW to halt any downstream reporting layer
      load if critical checks fail. Designed to be called
      from a SQL Agent job step or pipeline orchestrator.

  Behavior:
      - Each check sets @BadRows and logs PASS or FAIL
      - @FailureCount accumulates across all checks
      - If @FailureCount > 0 at the end, THROW halts execution
        and the reporting layer load should not proceed
      - SET XACT_ABORT ON ensures any open transaction is
        rolled back if THROW fires inside a transaction context

  Run Order:
      1. etl_staging_setup.sql          -- build schemas/tables
      2. load_staging.py                   -- load CSV data
      3. staging_layer_validation.sql   -- validate staging
      4. clean_layer.sql                -- build clean views
      5. 07_clean_validation_gate.0     -- clean layer gate
      6. dw_layer.sql                   -- build DW tables
      7. THIS SCRIPT                       -- DW gate

  DW Schema Referenced:
      Dimensions:
          dw.dim_date            dw.dim_driver
          dw.dim_region          dw.dim_product
          dw.dim_shipment_type   dw.dim_exception_type
          dw.dim_route
      Facts:
          dw.fact_deliveries     dw.fact_sales
          dw.fact_exceptions

  Checks Performed:
      1.  Empty table guard      -- all 10 DW tables must have rows
      2.  Row count comparison   -- DW facts vs clean views
      3.  NULL surrogate keys    -- FK columns in all fact tables
      4.  Duplicate primary keys -- all fact and dimension tables
      5.  FK integrity           -- fact rows missing dim matches
      6.  Business metric sanity -- SalesAmount, UnitsSold > 0
      7.  Delivery status logic  -- IsLate flag vs DeliveryStatus
      8.  Exception flag logic   -- IsResolved vs ResolvedDateKey

=============================================================*/

SET XACT_ABORT ON;

-- Abort if any single scan blocks for more than 30 seconds.
-- Adjust or remove if long-running queries are expected.
SET LOCK_TIMEOUT 30000;

USE Fedex_Ops_Database;
GO

PRINT '===== DW VALIDATION GATE START =====';

DECLARE @FailureCount INT        = 0;
DECLARE @CheckName    NVARCHAR(200);
DECLARE @BadRows      INT;


/*=============================================================
  CHECK 1: EMPTY TABLE GUARD
  All 10 DW tables (7 dimensions + 3 facts) must contain rows.
  An empty table means the load failed or rolled back silently.
  All downstream null and FK checks pass vacuously on empty
  tables, producing a false all-clear. Fail immediately if
  any table is empty.
=============================================================*/

PRINT '--- CHECK 1: EMPTY TABLE GUARD ---';

-- Dimension tables
SET @CheckName = 'dw.dim_date — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_date;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 1096 rows (2023-2025 date spine)'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.dim_driver — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_driver;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 21 rows (20 named + Unknown)'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.dim_region — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_region;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 7 rows'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.dim_product — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_product;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 4 rows'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.dim_shipment_type — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_shipment_type;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 3 rows'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.dim_exception_type — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_exception_type;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 4 rows'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.dim_route — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.dim_route;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 5 rows (R001-R005)'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- Fact tables
SET @CheckName = 'dw.fact_deliveries — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_deliveries;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 5000 rows'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.fact_sales — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_sales;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 4000 rows'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dw.fact_exceptions — not empty';
SELECT @BadRows = CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM dw.fact_exceptions;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Expected 1000 rows'; SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 2: ROW COUNT COMPARISON (FACT vs CLEAN VIEW)
  DW fact row counts must match their source clean view counts.
  Any discrepancy means rows were dropped during surrogate key
  joins in the DW load -- typically caused by a dimension
  member missing from a dim table.
  Informational only -- logged but does not increment
  @FailureCount since the empty table guard in Check 1 already
  catches the worst case. Investigate any non-zero DroppedRows.

  NOTE: View names use clean.clean_* naming convention from
  clean_layer_v1.sql, not the vw_* naming in earlier versions.
=============================================================*/

PRINT '--- CHECK 2: FACT vs CLEAN ROW COUNTS (INFORMATIONAL) ---';

SELECT
    'fact_deliveries'                                       AS FactTable,
    (SELECT COUNT(*) FROM clean.clean_deliveries)           AS CleanRows,
    (SELECT COUNT(*) FROM dw.fact_deliveries)               AS DWRows,
    (SELECT COUNT(*) FROM clean.clean_deliveries)
        - (SELECT COUNT(*) FROM dw.fact_deliveries)         AS DroppedRows;

SELECT
    'fact_sales'                                            AS FactTable,
    (SELECT COUNT(*) FROM clean.clean_sales)                AS CleanRows,
    (SELECT COUNT(*) FROM dw.fact_sales)                    AS DWRows,
    (SELECT COUNT(*) FROM clean.clean_sales)
        - (SELECT COUNT(*) FROM dw.fact_sales)              AS DroppedRows;

SELECT
    'fact_exceptions'                                       AS FactTable,
    (SELECT COUNT(*) FROM clean.clean_exceptions)           AS CleanRows,
    (SELECT COUNT(*) FROM dw.fact_exceptions)               AS DWRows,
    (SELECT COUNT(*) FROM clean.clean_exceptions)
        - (SELECT COUNT(*) FROM dw.fact_exceptions)         AS DroppedRows;


/*=============================================================
  CHECK 3: NULL SURROGATE KEY CHECKS
  Every FK surrogate key column in every fact table must be
  populated. A NULL surrogate key means the dim lookup JOIN
  failed during the DW load -- the row loaded but without a
  valid dimension reference.

  Only true FK columns are listed per fact table. Natural key
  columns (SalesID, DeliveryID, ExceptionID) and non-FK
  measures (UnitsSold, SalesAmount etc.) are excluded.

  NOTE: DeliveryStatus and PriorityFlag on fact_deliveries are
  NOT surrogate keys -- they are denormalised NVARCHAR and BIT
  columns copied directly from the clean layer. They are
  checked in Check 6 and 7 instead.
=============================================================*/

PRINT '--- CHECK 3: NULL SURROGATE KEY CHECKS ---';

-- fact_sales FK columns: DateKey, ProductSK, RegionSK
SET @CheckName = 'fact_sales — no NULL surrogate keys';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales
WHERE DateKey    IS NULL
   OR ProductSK  IS NULL
   OR RegionSK   IS NULL;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_deliveries FK columns: DateKey, DriverSK, RegionSK, ShipmentTypeSK, RouteSK
SET @CheckName = 'fact_deliveries — no NULL surrogate keys';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries
WHERE DateKey        IS NULL
   OR DriverSK       IS NULL
   OR RegionSK       IS NULL
   OR ShipmentTypeSK IS NULL
   OR RouteSK        IS NULL;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_exceptions FK columns: DateReportedKey, ExceptionTypeSK, RegionSK
-- ResolvedDateKey is intentionally excluded: NULL means open exception
SET @CheckName = 'fact_exceptions — no NULL surrogate keys on required FK columns';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions
WHERE DateReportedKey  IS NULL
   OR ExceptionTypeSK  IS NULL
   OR RegionSK         IS NULL;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 4: DUPLICATE PRIMARY KEY CHECKS
  Every fact and dimension table must have unique primary keys.
  Duplicates would cause double-counting in all aggregations.
=============================================================*/

PRINT '--- CHECK 4: DUPLICATE PRIMARY KEY CHECKS ---';

-- Dimension tables
SET @CheckName = 'dim_date — DateKey is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT DateKey FROM dw.dim_date GROUP BY DateKey HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dim_driver — DriverSK is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT DriverSK FROM dw.dim_driver GROUP BY DriverSK HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dim_region — RegionSK is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT RegionSK FROM dw.dim_region GROUP BY RegionSK HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dim_product — ProductSK is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT ProductSK FROM dw.dim_product GROUP BY ProductSK HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dim_shipment_type — ShipmentTypeSK is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT ShipmentTypeSK FROM dw.dim_shipment_type GROUP BY ShipmentTypeSK HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dim_exception_type — ExceptionTypeSK is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT ExceptionTypeSK FROM dw.dim_exception_type GROUP BY ExceptionTypeSK HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'dim_route — RouteSK is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT RouteSK FROM dw.dim_route GROUP BY RouteSK HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- Fact tables
SET @CheckName = 'fact_deliveries — DeliveryID is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT DeliveryID FROM dw.fact_deliveries GROUP BY DeliveryID HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_sales — SalesID is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT SalesID FROM dw.fact_sales GROUP BY SalesID HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

SET @CheckName = 'fact_exceptions — ExceptionID is unique';
SELECT @BadRows = COUNT(*) FROM (
    SELECT ExceptionID FROM dw.fact_exceptions GROUP BY ExceptionID HAVING COUNT(*) > 1
) x;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Duplicate PKs = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 5: FACT-TO-DIMENSION FK INTEGRITY
  Verifies that every FK surrogate key value in a fact table
  resolves to a row in its referenced dimension table.
  Orphaned FK values cause silent row loss during reporting
  joins even when the FK constraint exists with NOCHECK.

  NOTE: fact_sales also references fact_deliveries via
  DeliveryID. That fact-to-fact check is included here
  because a sales row with no matching delivery would lose
  all delivery context in reporting joins.
=============================================================*/

PRINT '--- CHECK 5: FK INTEGRITY CHECKS ---';

-- fact_sales -> dim_date
SET @CheckName = 'fact_sales -> dim_date';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.DateKey);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_sales -> dim_product
SET @CheckName = 'fact_sales -> dim_product';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_product d WHERE d.ProductSK = f.ProductSK);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_sales -> dim_region
SET @CheckName = 'fact_sales -> dim_region';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionSK = f.RegionSK);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_sales -> fact_deliveries (fact-to-fact: all sales must have a delivery)
SET @CheckName = 'fact_sales -> fact_deliveries (DeliveryID)';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.fact_deliveries d WHERE d.DeliveryID = f.DeliveryID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_deliveries -> dim_date
SET @CheckName = 'fact_deliveries -> dim_date';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.DateKey);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_deliveries -> dim_driver
SET @CheckName = 'fact_deliveries -> dim_driver';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_driver d WHERE d.DriverSK = f.DriverSK);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_deliveries -> dim_region
SET @CheckName = 'fact_deliveries -> dim_region';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionSK = f.RegionSK);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_deliveries -> dim_shipment_type
SET @CheckName = 'fact_deliveries -> dim_shipment_type';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_shipment_type d WHERE d.ShipmentTypeSK = f.ShipmentTypeSK);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_deliveries -> dim_route
SET @CheckName = 'fact_deliveries -> dim_route';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_route d WHERE d.RouteSK = f.RouteSK);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_exceptions -> dim_date (DateReportedKey)
SET @CheckName = 'fact_exceptions -> dim_date (DateReportedKey)';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.DateReportedKey);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_exceptions -> dim_date (ResolvedDateKey) -- NULL allowed for open exceptions
SET @CheckName = 'fact_exceptions -> dim_date (ResolvedDateKey where not NULL)';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions f
WHERE ResolvedDateKey IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.ResolvedDateKey);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_exceptions -> dim_exception_type
SET @CheckName = 'fact_exceptions -> dim_exception_type';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_exception_type d WHERE d.ExceptionTypeSK = f.ExceptionTypeSK);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_exceptions -> dim_region
SET @CheckName = 'fact_exceptions -> dim_region';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionSK = f.RegionSK);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- fact_exceptions -> fact_deliveries (all exceptions must have a delivery)
SET @CheckName = 'fact_exceptions -> fact_deliveries (DeliveryID)';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.fact_deliveries d WHERE d.DeliveryID = f.DeliveryID);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Orphaned rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 6: BUSINESS METRIC SANITY
  Core financial and operational measures must be within
  acceptable bounds. These checks confirm that no invalid
  values survived the clean layer and DW load.
=============================================================*/

PRINT '--- CHECK 6: BUSINESS METRIC SANITY ---';

-- No zero or negative SalesAmount
SET @CheckName = 'fact_sales — SalesAmount > 0';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales WHERE SalesAmount <= 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- No zero or negative UnitsSold
SET @CheckName = 'fact_sales — UnitsSold > 0';
SELECT @BadRows = COUNT(*) FROM dw.fact_sales WHERE UnitsSold <= 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- ResolutionTimeHours non-negative where populated
SET @CheckName = 'fact_exceptions — ResolutionTimeHours >= 0 where not NULL';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions
WHERE ResolutionTimeHours IS NOT NULL AND ResolutionTimeHours < 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- PriorityFlag is 0 or 1 only
SET @CheckName = 'fact_deliveries — PriorityFlag is 0 or 1';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries WHERE PriorityFlag NOT IN (0, 1);
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 7: ISLATE FLAG vs DELIVERY STATUS CONSISTENCY
  IsLate is a denormalised BIT column on fact_deliveries
  derived from DeliveryStatus in the clean layer. Both columns
  are stored on the fact table so they can be cross-checked
  directly without a dimension join.

  NOTE: DeliveryStatus casing matches source data exactly --
  'Late', 'On-Time', 'Exception' -- not 'LATE'. The original
  v2.0 script used 'LATE' which caused every run to fail.
=============================================================*/

PRINT '--- CHECK 7: ISLATE FLAG vs DELIVERY STATUS ---';

-- Late and Exception deliveries must have IsLate = 1
SET @CheckName = 'fact_deliveries — IsLate = 1 for Late and Exception rows';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries
WHERE DeliveryStatus IN ('Late', 'Exception')
  AND IsLate <> 1;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- On-Time deliveries must have IsLate = 0
SET @CheckName = 'fact_deliveries — IsLate = 0 for On-Time rows';
SELECT @BadRows = COUNT(*) FROM dw.fact_deliveries
WHERE DeliveryStatus = 'On-Time'
  AND IsLate <> 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  CHECK 8: ISRESOLVED FLAG vs RESOLVEDATEKEY CONSISTENCY
  IsResolved is a denormalised BIT column on fact_exceptions.
  ResolvedDateKey is NULL for open exceptions. Both columns
  must be consistent with each other.
=============================================================*/

PRINT '--- CHECK 8: ISRESOLVED FLAG vs RESOLVEDATEKEY ---';

-- Rows with a ResolvedDateKey must have IsResolved = 1
SET @CheckName = 'fact_exceptions — IsResolved = 1 where ResolvedDateKey is populated';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions
WHERE ResolvedDateKey IS NOT NULL AND IsResolved <> 1;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;

-- Rows with no ResolvedDateKey must have IsResolved = 0
SET @CheckName = 'fact_exceptions — IsResolved = 0 where ResolvedDateKey is NULL';
SELECT @BadRows = COUNT(*) FROM dw.fact_exceptions
WHERE ResolvedDateKey IS NULL AND IsResolved <> 0;
IF @BadRows > 0 BEGIN PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR); SET @FailureCount += 1; END
ELSE PRINT 'PASS: ' + @CheckName;


/*=============================================================
  INFORMATIONAL: DW TABLE ROW COUNT SUMMARY
  Not a gate check -- printed for human review after all
  gate checks pass. Shows actual row counts vs expected.

  Expected counts:
      dim_date           1,096    dim_driver            21
      dim_region             7    dim_product            4
      dim_shipment_type      3    dim_exception_type     4
      dim_route              5    fact_deliveries    5,000
      fact_sales         4,000    fact_exceptions    1,000
=============================================================*/

PRINT '--- INFORMATIONAL: DW TABLE ROW COUNTS ---';

SELECT 'dim_date'           AS TableName, COUNT(*) AS RowsCount FROM dw.dim_date
UNION ALL
SELECT 'dim_driver'         AS TableName, COUNT(*) AS RowsCount FROM dw.dim_driver
UNION ALL
SELECT 'dim_region'         AS TableName, COUNT(*) AS RowsCount FROM dw.dim_region
UNION ALL
SELECT 'dim_product'        AS TableName, COUNT(*) AS RowsCount FROM dw.dim_product
UNION ALL
SELECT 'dim_shipment_type'  AS TableName, COUNT(*) AS RowsCount FROM dw.dim_shipment_type
UNION ALL
SELECT 'dim_exception_type' AS TableName, COUNT(*) AS RowsCount FROM dw.dim_exception_type
UNION ALL
SELECT 'dim_route'          AS TableName, COUNT(*) AS RowsCount FROM dw.dim_route
UNION ALL
SELECT 'fact_deliveries'    AS TableName, COUNT(*) AS RowsCount FROM dw.fact_deliveries
UNION ALL
SELECT 'fact_sales'         AS TableName, COUNT(*) AS RowsCount FROM dw.fact_sales
UNION ALL
SELECT 'fact_exceptions'    AS TableName, COUNT(*) AS RowsCount FROM dw.fact_exceptions;

-- fact_sales summary metrics
SELECT
    COUNT(*)                                            AS TotalTransactions,
    SUM(SalesAmount)                                    AS TotalSales,
    ROUND(AVG(SalesAmount), 2)                          AS AvgSale,
    MIN(SalesAmount)                                    AS MinSale,
    MAX(SalesAmount)                                    AS MaxSale
FROM dw.fact_sales;


/*=============================================================
  FINAL PIPELINE DECISION
  Halt the pipeline if any check failed. THROW propagates to
  the calling SQL Agent job or orchestration layer and cancels
  the reporting layer load.
=============================================================*/

IF @FailureCount > 0
BEGIN
    PRINT '===== DW VALIDATION FAILED: '
        + CAST(@FailureCount AS VARCHAR)
        + ' check(s) failed -- Reporting layer load cancelled =====';
    THROW 51002, 'DW Validation Failed. Reporting layer load cancelled.', 1;
END
ELSE
BEGIN
    PRINT '===== DW VALIDATION PASSED — all checks passed =====';
    PRINT 'Pipeline cleared to proceed with reporting layer load.';
END;
GO
