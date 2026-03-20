/*=============================================================
  STAGING LAYER VALIDATION
  Database: Fedex_Ops_Database
  Version:  3.0

  Purpose:
      Validate staging tables after each load from load_staging.py.
      Outputs a PASS/FAIL summary for every check. Review all
      FAIL and WARN results before proceeding to the clean layer.

  Run Order:
      1. etl_staging_setup_v5.sql        -- build schemas and tables
      2. load_staging.py                 -- load CSV data
      3. THIS SCRIPT                     -- validate staging data

  Changes from v1.0:
      - Row count checks are now dynamic. Expected values are
        pulled from staging.load_log (written by load_staging.py)
        instead of being hardcoded. Validation automatically
        adapts when source file sizes change.

  Validation Categories:
      1. Row Counts        -- current counts match last load
      2. Null Checks       -- required columns are populated
      3. Duplicate Checks  -- primary keys are unique
      4. Referential       -- foreign keys exist in parent tables
      5. Value Ranges      -- numeric values are sensible
      6. Date Sanity       -- dates are logical and in range
      7. Data Quality      -- known issues flagged for clean layer

=============================================================*/

USE Fedex_Ops_Database;
GO

/*=============================================================
  SETUP: RESULTS TABLE
  Stores every check result so the final summary can be
  printed in one clean output at the end.
=============================================================*/

IF OBJECT_ID('tempdb..#ValidationResults', 'U') IS NOT NULL
    DROP TABLE #ValidationResults;

CREATE TABLE #ValidationResults (
    CheckID   INT IDENTITY(1,1),
    Category  NVARCHAR(50),
    TableName NVARCHAR(100),
    CheckName NVARCHAR(200),
    Expected  NVARCHAR(100),
    Actual    NVARCHAR(100),
    Status    NVARCHAR(10)    -- PASS, FAIL, WARN
);
GO

/*=============================================================
  CATEGORY 1: ROW COUNTS
  Compares current table counts against the most recent load
  recorded in staging.load_log. No hardcoded values -- adapts
  automatically when source file sizes change.
=============================================================*/

DECLARE @Expected INT;
DECLARE @Count    INT;

-- staging_sales
SELECT @Expected = RowsLoaded
FROM staging.load_log
WHERE TableName = 'staging.staging_sales'
  AND LoadID = (
      SELECT MAX(LoadID) FROM staging.load_log
      WHERE TableName = 'staging.staging_sales'
  );

SELECT @Count = COUNT(*) FROM staging.staging_sales;

INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Row Count', 'staging_sales', 'Row count matches last load',
        CAST(@Expected AS NVARCHAR), CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = @Expected THEN 'PASS' ELSE 'FAIL' END);

-- staging_deliveries
SELECT @Expected = RowsLoaded
FROM staging.load_log
WHERE TableName = 'staging.staging_deliveries'
  AND LoadID = (
      SELECT MAX(LoadID) FROM staging.load_log
      WHERE TableName = 'staging.staging_deliveries'
  );

SELECT @Count = COUNT(*) FROM staging.staging_deliveries;

INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Row Count', 'staging_deliveries', 'Row count matches last load',
        CAST(@Expected AS NVARCHAR), CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = @Expected THEN 'PASS' ELSE 'FAIL' END);

-- staging_routes
SELECT @Expected = RowsLoaded
FROM staging.load_log
WHERE TableName = 'staging.staging_routes'
  AND LoadID = (
      SELECT MAX(LoadID) FROM staging.load_log
      WHERE TableName = 'staging.staging_routes'
  );

SELECT @Count = COUNT(*) FROM staging.staging_routes;

INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Row Count', 'staging_routes', 'Row count matches last load',
        CAST(@Expected AS NVARCHAR), CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = @Expected THEN 'PASS' ELSE 'FAIL' END);

-- staging_exceptions
SELECT @Expected = RowsLoaded
FROM staging.load_log
WHERE TableName = 'staging.staging_exceptions'
  AND LoadID = (
      SELECT MAX(LoadID) FROM staging.load_log
      WHERE TableName = 'staging.staging_exceptions'
  );

SELECT @Count = COUNT(*) FROM staging.staging_exceptions;

INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Row Count', 'staging_exceptions', 'Row count matches last load',
        CAST(@Expected AS NVARCHAR), CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = @Expected THEN 'PASS' ELSE 'FAIL' END);

/*=============================================================
  CATEGORY 2: NULL CHECKS
  Confirms required (NOT NULL) columns contain no nulls.
  Nullable columns (DriverID, ResolvedDate etc.) are excluded.
=============================================================*/

-- staging_sales: all columns NOT NULL
SELECT @Count = COUNT(*) FROM staging.staging_sales
WHERE SalesID IS NULL OR DeliveryID IS NULL OR DateKey IS NULL
   OR ProductType IS NULL OR Region IS NULL
   OR UnitsSold IS NULL OR SalesAmount IS NULL;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Null Check', 'staging_sales', 'No NULLs in required columns',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_deliveries: required columns only (DriverID excluded - nullable by design)
SELECT @Count = COUNT(*) FROM staging.staging_deliveries
WHERE DeliveryID IS NULL OR RouteID IS NULL OR Region IS NULL
   OR ShipmentType IS NULL OR DeliveryDate IS NULL
   OR DeliveryStatus IS NULL OR PriorityFlag IS NULL;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Null Check', 'staging_deliveries', 'No NULLs in required columns',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_deliveries: report NULL DriverID count (expected ~1,010, nullable by design)
SELECT @Count = COUNT(*) FROM staging.staging_deliveries WHERE DriverID IS NULL;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Null Check', 'staging_deliveries',
        'NULL DriverID count (expected ~1010, nullable by design)',
        '~1010', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count BETWEEN 900 AND 1100 THEN 'PASS' ELSE 'WARN' END);

-- staging_routes: all columns NOT NULL
SELECT @Count = COUNT(*) FROM staging.staging_routes
WHERE RouteID IS NULL OR PlannedStops IS NULL
   OR ActualStops IS NULL OR PlannedHours IS NULL
   OR ActualHours IS NULL OR Region IS NULL;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Null Check', 'staging_routes', 'No NULLs in required columns',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_exceptions: required columns only (ResolvedDate, ResolutionTimeHours excluded)
SELECT @Count = COUNT(*) FROM staging.staging_exceptions
WHERE ExceptionID IS NULL OR DeliveryID IS NULL OR ExceptionType IS NULL
   OR DateReported IS NULL OR PriorityFlag IS NULL OR Region IS NULL;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Null Check', 'staging_exceptions', 'No NULLs in required columns',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_exceptions: open exceptions with NULL ResolvedDate (expected ~30)
SELECT @Count = COUNT(*) FROM staging.staging_exceptions WHERE ResolvedDate IS NULL;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Null Check', 'staging_exceptions',
        'Open exceptions with NULL ResolvedDate (expected ~30)',
        '~30', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count BETWEEN 20 AND 40 THEN 'PASS' ELSE 'WARN' END);

/*=============================================================
  CATEGORY 3: DUPLICATE CHECKS
  Confirms primary key columns contain no duplicate values.
  Routes uses a composite PK (RouteID + DriverID).
  NOTE: 202 duplicate RouteID+DriverID combinations are known
  in the source CSV -- flagged as WARN for investigation in
  the clean layer.
=============================================================*/

-- staging_sales: SalesID unique
SELECT @Count = COUNT(*) FROM (
    SELECT SalesID FROM staging.staging_sales
    GROUP BY SalesID HAVING COUNT(*) > 1
) d;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Duplicate Check', 'staging_sales', 'SalesID is unique',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_deliveries: DeliveryID unique
SELECT @Count = COUNT(*) FROM (
    SELECT DeliveryID FROM staging.staging_deliveries
    GROUP BY DeliveryID HAVING COUNT(*) > 1
) d;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Duplicate Check', 'staging_deliveries', 'DeliveryID is unique',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_exceptions: ExceptionID unique
SELECT @Count = COUNT(*) FROM (
    SELECT ExceptionID FROM staging.staging_exceptions
    GROUP BY ExceptionID HAVING COUNT(*) > 1
) d;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Duplicate Check', 'staging_exceptions', 'ExceptionID is unique',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_routes: composite PK (RouteID + DriverID)
-- 202 duplicates are known in source CSV -- flagged as WARN
SELECT @Count = COUNT(*) FROM (
    SELECT RouteID, DriverID FROM staging.staging_routes
    GROUP BY RouteID, DriverID HAVING COUNT(*) > 1
) d;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Duplicate Check', 'staging_routes',
        'Duplicate RouteID+DriverID combinations (202 known in source - investigate in clean layer)',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0    THEN 'PASS'
             WHEN @Count <= 202 THEN 'WARN'
             ELSE 'FAIL' END);

/*=============================================================
  CATEGORY 4: REFERENTIAL INTEGRITY
  Confirms foreign key values in child tables exist in their
  parent table (staging_deliveries).
=============================================================*/

-- staging_sales: every DeliveryID exists in staging_deliveries
SELECT @Count = COUNT(*) FROM staging.staging_sales s
WHERE NOT EXISTS (
    SELECT 1 FROM staging.staging_deliveries d
    WHERE d.DeliveryID = s.DeliveryID
);
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Referential Integrity', 'staging_sales',
        'All DeliveryIDs exist in staging_deliveries',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_exceptions: every DeliveryID exists in staging_deliveries
SELECT @Count = COUNT(*) FROM staging.staging_exceptions e
WHERE NOT EXISTS (
    SELECT 1 FROM staging.staging_deliveries d
    WHERE d.DeliveryID = e.DeliveryID
);
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Referential Integrity', 'staging_exceptions',
        'All DeliveryIDs exist in staging_deliveries',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

/*=============================================================
  CATEGORY 5: VALUE RANGE CHECKS
  Confirms numeric columns contain sensible values.
=============================================================*/

-- staging_sales: SalesAmount >= 0
SELECT @Count = COUNT(*) FROM staging.staging_sales WHERE SalesAmount < 0;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Value Range', 'staging_sales', 'SalesAmount is non-negative',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_sales: UnitsSold > 0
SELECT @Count = COUNT(*) FROM staging.staging_sales WHERE UnitsSold <= 0;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Value Range', 'staging_sales', 'UnitsSold is greater than zero',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_routes: PlannedStops and ActualStops >= 0
SELECT @Count = COUNT(*) FROM staging.staging_routes
WHERE PlannedStops < 0 OR ActualStops < 0;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Value Range', 'staging_routes', 'PlannedStops and ActualStops are non-negative',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_routes: PlannedHours and ActualHours >= 0
SELECT @Count = COUNT(*) FROM staging.staging_routes
WHERE PlannedHours < 0 OR ActualHours < 0;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Value Range', 'staging_routes', 'PlannedHours and ActualHours are non-negative',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

-- staging_exceptions: ResolutionTimeHours >= 0 where not null
SELECT @Count = COUNT(*) FROM staging.staging_exceptions
WHERE ResolutionTimeHours IS NOT NULL AND ResolutionTimeHours < 0;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Value Range', 'staging_exceptions',
        'ResolutionTimeHours is non-negative where populated',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

/*=============================================================
  CATEGORY 6: DATE SANITY CHECKS
  Confirms dates are logical and within expected range.
  Source data spans 2023-2025 based on CSV inspection.
=============================================================*/

-- staging_sales: DateKey within expected range
SELECT @Count = COUNT(*) FROM staging.staging_sales
WHERE CAST(DateKey AS DATE) < '2023-01-01'
   OR CAST(DateKey AS DATE) > '2025-12-31';
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Date Sanity', 'staging_sales',
        'DateKey is within expected range (2023-2025)',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'WARN' END);

-- staging_deliveries: DeliveryDate within expected range
SELECT @Count = COUNT(*) FROM staging.staging_deliveries
WHERE CAST(DeliveryDate AS DATE) < '2023-01-01'
   OR CAST(DeliveryDate AS DATE) > '2025-12-31';
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Date Sanity', 'staging_deliveries',
        'DeliveryDate is within expected range (2023-2025)',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'WARN' END);

-- staging_exceptions: ResolvedDate is never before DateReported
SELECT @Count = COUNT(*) FROM staging.staging_exceptions
WHERE ResolvedDate IS NOT NULL AND ResolvedDate < DateReported;
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Date Sanity', 'staging_exceptions',
        'ResolvedDate is never before DateReported',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'FAIL' END);

/*=============================================================
  CATEGORY 7: DATA QUALITY FLAGS
  Known issues in the source CSV that will need to be handled
  in the clean layer. Flagged as WARN so they are visible and
  documented before clean layer work begins.
=============================================================*/

-- staging_sales: truncated ProductType values (e.g. 'L.' 'F.' 'S.')
SELECT @Count = COUNT(*) FROM staging.staging_sales
WHERE ProductType LIKE '_.';
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Data Quality', 'staging_sales',
        'Truncated ProductType values ending in ''.'' (e.g. ''L.'' ''F.'') - fix in clean layer',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'WARN' END);

-- staging_sales: truncated Region values (e.g. 'M.' 'S.' 'N.')
SELECT @Count = COUNT(*) FROM staging.staging_sales
WHERE Region LIKE '_.';
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Data Quality', 'staging_sales',
        'Truncated Region values ending in ''.'' (e.g. ''M.'' ''S.'' ''N.'') - fix in clean layer',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'WARN' END);

-- staging_deliveries: truncated ShipmentType values (e.g. 'E.' 'P.' 'S.')
SELECT @Count = COUNT(*) FROM staging.staging_deliveries
WHERE ShipmentType LIKE '_.';
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Data Quality', 'staging_deliveries',
        'Truncated ShipmentType values ending in ''.'' (e.g. ''E.'' ''P.'') - fix in clean layer',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'WARN' END);

-- staging_deliveries: truncated Region values
SELECT @Count = COUNT(*) FROM staging.staging_deliveries
WHERE Region LIKE '_.';
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Data Quality', 'staging_deliveries',
        'Truncated Region values ending in ''.'' - fix in clean layer',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'WARN' END);

-- staging_exceptions: truncated ExceptionType values
SELECT @Count = COUNT(*) FROM staging.staging_exceptions
WHERE ExceptionType LIKE '_.';
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Data Quality', 'staging_exceptions',
        'Truncated ExceptionType values ending in ''.'' - fix in clean layer',
        '0', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count = 0 THEN 'PASS' ELSE 'WARN' END);

-- staging_routes: NULL or empty DriverID values (59 known in source)
SELECT @Count = COUNT(*) FROM staging.staging_routes
WHERE DriverID IS NULL OR DriverID = '';
INSERT INTO #ValidationResults (Category, TableName, CheckName, Expected, Actual, Status)
VALUES ('Data Quality', 'staging_routes',
        'NULL or empty DriverID values (59 known in source) - fix in clean layer',
        '~59', CAST(@Count AS NVARCHAR),
        CASE WHEN @Count BETWEEN 50 AND 70 THEN 'WARN' ELSE 'FAIL' END);

GO

/*=============================================================
  FINAL SUMMARY
  Prints full results table sorted by severity then a
  counts summary. Review all FAIL results before proceeding
  to the clean layer. WARN results are known issues to
  address in the clean layer.
=============================================================*/

PRINT '--- STAGING VALIDATION RESULTS ---';

SELECT
    CheckID,
    Category,
    TableName,
    CheckName,
    Expected,
    Actual,
    Status
FROM #ValidationResults
ORDER BY
    CASE Status
        WHEN 'FAIL' THEN 1
        WHEN 'WARN' THEN 2
        WHEN 'PASS' THEN 3
    END,
    Category,
    TableName;

-- Summary counts
PRINT '--- SUMMARY ---';

SELECT
    Status,
    COUNT(*) AS CheckCount
FROM #ValidationResults
GROUP BY Status
ORDER BY
    CASE Status
        WHEN 'FAIL' THEN 1
        WHEN 'WARN' THEN 2
        WHEN 'PASS' THEN 3
    END;

-- Overall result
DECLARE @FailCount INT;
SELECT @FailCount = COUNT(*) FROM #ValidationResults WHERE Status = 'FAIL';

IF @FailCount = 0
    PRINT 'OVERALL: PASS — No failures detected. Review WARNs before proceeding to clean layer.';
ELSE
    PRINT 'OVERALL: FAIL — ' + CAST(@FailCount AS NVARCHAR)
        + ' failure(s) detected. Do not proceed to clean layer until resolved.';

PRINT '--- END OF VALIDATION ---';

DROP TABLE #ValidationResults;
GO
