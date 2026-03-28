/*=============================================================
  DW COLUMN PROFILER
  Database: Fedex_Ops_Database
  Version:  3.0

  Purpose:
      Deep column-level data quality profiling across all base
      tables in the warehouse. Informational only -- does not
      halt the pipeline. Run after a DW load to investigate
      data quality, completeness, and distribution.

  Run Order:
      This script runs independently after the pipeline has
      completed. It is not part of the linear load sequence.
      Run it manually when investigating data quality issues
      or after a schema change.

  Steps:
      1. Table row counts        (staging and dw base tables)
      2. Column NULL profiling   (actual NULL counts + %)
      3. Data quality scorecard  (per-table NULL summary)
      4. Critical NULL flags     (columns > 20% NULL)
      5. Dimension member counts (cardinality per dimension)
      6. Fact table distributions (key metric summaries)

  Schemas Scanned:
      staging, dw
      clean and reporting contain views only -- NULL profiling
      against views is valid but expensive and is already
      covered by the clean layer profiling script. Base tables
      in staging and dw are the focus here.

  Expected NULLs (not errors):
      staging_deliveries.DriverID		   ~1,010 rows (nullable by design)
      staging_routes.DriverID              ~59 rows (nullable by design)
      staging_exceptions.ResolvedDate      ~30 rows (open exceptions)
      fact_exceptions.ResolvedDateKey      ~30 rows (open exceptions)
      fact_exceptions.ResolutionTimeHours  ~30 rows (open exceptions)
      fact_exceptions.ResolutionDays       ~30 rows (open exceptions)
      fact_deliveries.DaysVariance			varies (NULL if no expected date)

=============================================================*/

USE Fedex_Ops_Database;
GO

PRINT '===== COLUMN PROFILER START =====';


/*=============================================================
  STEP 1: TABLE ROW COUNTS
  Row counts are stored in #TableRowCounts and reused in
  Step 2 to avoid a redundant full-scan COUNT(*) per column.
  This is particularly important for large staging tables.
=============================================================*/

PRINT '--- STEP 1: TABLE ROW COUNTS ---';

IF OBJECT_ID('tempdb..#TableRowCounts') IS NOT NULL
    DROP TABLE #TableRowCounts;

CREATE TABLE #TableRowCounts (
    SchemaName  NVARCHAR(128),
    TableName   NVARCHAR(128),
    [RowCount]  BIGINT
);

INSERT INTO #TableRowCounts (SchemaName, TableName, [RowCount])
SELECT
    s.name,
    t.name,
    SUM(p.rows)
FROM sys.tables     t
JOIN sys.schemas    s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
                      AND p.index_id IN (0, 1)
WHERE s.name IN ('staging', 'dw')
GROUP BY s.name, t.name;

SELECT *
FROM #TableRowCounts
ORDER BY SchemaName,
         CASE WHEN TableName LIKE 'dim%'  THEN 0
              WHEN TableName LIKE 'fact%' THEN 1
              ELSE 2
         END,
         TableName;

/*=============================================================
  STEP 2: COLUMN NULL PROFILING
  For every column in every base table in the scoped schemas,
  counts actual NULLs and computes the NULL percentage.
  Row counts are sourced from #TableRowCounts (Step 1) to
  avoid a redundant COUNT(*) per column.

  QUOTENAME() ensures reserved-word column names are safe
  (e.g. [Year], [Month], [Quarter] in dim_date).

  Only BASE TABLE objects are profiled -- views are excluded
  by the JOIN to INFORMATION_SCHEMA.TABLES on TABLE_TYPE.
=============================================================*/

PRINT '--- STEP 2: COLUMN NULL PROFILING ---';

IF OBJECT_ID('tempdb..#ColumnProfile') IS NOT NULL
    DROP TABLE #ColumnProfile;

CREATE TABLE #ColumnProfile (
    SchemaName  NVARCHAR(128),
    TableName   NVARCHAR(128),
    ColumnName  NVARCHAR(128),
    NullCount   BIGINT,
    TotalRows   BIGINT,
    NullPct     DECIMAL(10,2)
);

DECLARE @profileSQL NVARCHAR(MAX) = '';

SELECT @profileSQL = @profileSQL + '
INSERT INTO #ColumnProfile
SELECT
    ''' + c.TABLE_SCHEMA + ''',
    ''' + c.TABLE_NAME   + ''',
    ''' + c.COLUMN_NAME  + ''',
    SUM(CASE WHEN ' + QUOTENAME(c.COLUMN_NAME) + ' IS NULL THEN 1 ELSE 0 END),
    rc.[RowCount],
    CAST(
        SUM(CASE WHEN ' + QUOTENAME(c.COLUMN_NAME) + ' IS NULL THEN 1 ELSE 0 END)
        * 100.0 / NULLIF(rc.[RowCount], 0)
    AS DECIMAL(10,2))
FROM ' + QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME) + '
CROSS JOIN (
    SELECT [RowCount]
    FROM #TableRowCounts
    WHERE SchemaName = ''' + c.TABLE_SCHEMA + '''
      AND TableName  = ''' + c.TABLE_NAME   + '''
) rc
GROUP BY rc.[RowCount];'
FROM INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.TABLES  t
    ON  c.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND c.TABLE_NAME   = t.TABLE_NAME
    AND t.TABLE_TYPE   = 'BASE TABLE'   -- exclude views
WHERE c.TABLE_SCHEMA IN ('staging', 'dw');

EXEC sp_executesql @profileSQL;

-- Full profiling output sorted by NULL % descending
SELECT *
FROM #ColumnProfile
ORDER BY NullPct DESC, SchemaName, TableName, ColumnName;

/*=============================================================
  STEP 3: DATA QUALITY SCORECARD
  Summarises NULL distribution per table: how many columns
  have any NULLs, and the average NULL % across all columns.
  Tables with unexpectedly high AvgNullPct warrant investigation.

  Expected NULLs are documented in the file header above.
  Any table not listed there with a non-zero AvgNullPct
  should be investigated before the next pipeline run.
=============================================================*/

PRINT '--- STEP 3: DATA QUALITY SCORECARD ---';

SELECT
    SchemaName,
    TableName,
    COUNT(*)                                            AS ColumnsChecked,
    SUM(CASE WHEN NullPct > 0 THEN 1 ELSE 0 END)       AS ColumnsWithNulls,
    ROUND(AVG(NullPct), 2)                              AS AvgNullPct,
    MAX(NullPct)                                        AS MaxNullPct
FROM #ColumnProfile
GROUP BY SchemaName, TableName
ORDER BY AvgNullPct DESC;

/*=============================================================
  STEP 4: CRITICAL NULL FLAGS
  Lists every column where more than 20% of rows are NULL.
  These may indicate a load failure, a missing source join,
  or a column that was added after the initial data load.

  NOTE: The following columns are expected to have high NULL %
  and are not errors. They are documented here for reference:
      staging_deliveries.DriverID     ~20%  (nullable by design)
      staging_routes.DriverID         ~20%  (nullable by design)
  All other columns above 20% NULL should be investigated.
=============================================================*/

PRINT '--- STEP 4: CRITICAL NULL FLAGS (> 20%) ---';

SELECT
    SchemaName,
    TableName,
    ColumnName,
    NullCount,
    TotalRows,
    NullPct
FROM #ColumnProfile
WHERE NullPct > 20
ORDER BY NullPct DESC;

DROP TABLE #ColumnProfile;
DROP TABLE #TableRowCounts;

/*=============================================================
  STEP 5: DIMENSION MEMBER CARDINALITY
  Confirms the expected number of members loaded into each
  dimension table. A count mismatch means a dimension member
  was added to or removed from the source data without the
  dimension table being updated.

  Expected cardinality:
      dim_date            1,096  (2023-01-01 to 2025-12-31)
      dim_driver             21  (20 named drivers + Unknown)
      dim_region              7  (MW N NE NW S SE SW)
      dim_product             4  (Freight Large Medium Small)
      dim_shipment_type       3  (Express Priority Standard)
      dim_exception_type      4  (Address Issue Customer
                                  Not Available Mechanical Weather)
      dim_route               5  (R001 R002 R003 R004 R005)
=============================================================*/

PRINT '--- STEP 5: DIMENSION MEMBER CARDINALITY ---';

SELECT 'dim_date'           AS DimTable, COUNT(*) AS MemberCount FROM dw.dim_date
UNION ALL
SELECT 'dim_driver'         AS DimTable, COUNT(*) AS MemberCount FROM dw.dim_driver
UNION ALL
SELECT 'dim_region'         AS DimTable, COUNT(*) AS MemberCount FROM dw.dim_region
UNION ALL
SELECT 'dim_product'        AS DimTable, COUNT(*) AS MemberCount FROM dw.dim_product
UNION ALL
SELECT 'dim_shipment_type'  AS DimTable, COUNT(*) AS MemberCount FROM dw.dim_shipment_type
UNION ALL
SELECT 'dim_exception_type' AS DimTable, COUNT(*) AS MemberCount FROM dw.dim_exception_type
UNION ALL
SELECT 'dim_route'          AS DimTable, COUNT(*) AS MemberCount FROM dw.dim_route;

-- dim_driver member list: confirm Unknown row exists and all
-- 20 named drivers loaded correctly
SELECT
    DriverSK,
    DriverCode,
    DriverLabel,
    IsUnknown
FROM dw.dim_driver
ORDER BY IsUnknown DESC, DriverCode;

-- dim_region member list: confirm all 7 regions loaded
SELECT RegionSK, RegionCode, RegionName
FROM dw.dim_region
ORDER BY RegionCode;

-- dim_product member list
SELECT ProductSK, ProductType
FROM dw.dim_product
ORDER BY ProductType;

-- dim_shipment_type member list
SELECT ShipmentTypeSK, ShipmentType
FROM dw.dim_shipment_type
ORDER BY ShipmentType;

-- dim_exception_type member list
SELECT ExceptionTypeSK, ExceptionType
FROM dw.dim_exception_type
ORDER BY ExceptionType;

-- dim_route member list
SELECT RouteSK, RouteID
FROM dw.dim_route
ORDER BY RouteID;

/*=============================================================
  STEP 6: FACT TABLE METRIC DISTRIBUTIONS
  Quick statistical summaries for key measures in each fact
  table. Review for unexpected outliers or distributions
  that differ from previous runs.
=============================================================*/

PRINT '--- STEP 6: FACT TABLE DISTRIBUTIONS ---';

-- fact_sales: revenue and volume metrics
SELECT
    COUNT(*)                                            AS TotalTransactions,
    SUM(SalesAmount)                                    AS TotalRevenue,
    ROUND(AVG(SalesAmount), 2)                          AS AvgSaleAmount,
    MIN(SalesAmount)                                    AS MinSaleAmount,
    MAX(SalesAmount)                                    AS MaxSaleAmount,
    SUM(UnitsSold)                                      AS TotalUnitsSold,
    ROUND(AVG(CAST(UnitsSold AS DECIMAL(10,2))), 2)     AS AvgUnitsSold
FROM dw.fact_sales;

-- fact_sales: revenue by region
SELECT
    r.RegionCode,
    r.RegionName,
    COUNT(*)                                            AS Transactions,
    SUM(f.SalesAmount)                                  AS TotalRevenue,
    ROUND(AVG(f.SalesAmount), 2)                        AS AvgSale
FROM dw.fact_sales      f
JOIN dw.dim_region      r ON r.RegionSK = f.RegionSK
GROUP BY r.RegionCode, r.RegionName
ORDER BY TotalRevenue DESC;

-- fact_deliveries: on-time vs late distribution
SELECT
    DeliveryStatus,
    COUNT(*)                                            AS DeliveryCount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)  AS PctOfTotal
FROM dw.fact_deliveries
GROUP BY DeliveryStatus
ORDER BY DeliveryCount DESC;

-- fact_deliveries: DaysVariance distribution
-- Positive = delivered late, Negative = early, NULL = no expected date
SELECT
    MIN(DaysVariance)                                   AS MinDaysVariance,
    MAX(DaysVariance)                                   AS MaxDaysVariance,
    ROUND(AVG(CAST(DaysVariance AS DECIMAL(10,2))), 2)  AS AvgDaysVariance,
    COUNT(CASE WHEN DaysVariance > 0 THEN 1 END)        AS LateDeliveries,
    COUNT(CASE WHEN DaysVariance <= 0 THEN 1 END)       AS OnTimeOrEarly,
    COUNT(CASE WHEN DaysVariance IS NULL THEN 1 END)    AS NoExpectedDate
FROM dw.fact_deliveries;

-- fact_exceptions: resolution metrics
SELECT
    COUNT(*)                                            AS TotalExceptions,
    SUM(CAST(IsResolved AS INT))                         AS Resolved,
    SUM(CASE WHEN IsResolved = 0 THEN 1 ELSE 0 END)     AS OpenExceptions,
    ROUND(AVG(CASE WHEN ResolutionTimeHours IS NOT NULL
                   THEN ResolutionTimeHours END), 2)    AS AvgResolutionHours,
    MIN(ResolutionTimeHours)                            AS MinResolutionHours,
    MAX(ResolutionTimeHours)                            AS MaxResolutionHours
FROM dw.fact_exceptions;

-- fact_exceptions: distribution by exception type
SELECT
    et.ExceptionType,
    COUNT(*)                                            AS ExceptionCount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)  AS PctOfTotal,
    ROUND(AVG(f.ResolutionTimeHours), 2)                AS AvgResolutionHours
FROM dw.fact_exceptions     f
JOIN dw.dim_exception_type  et ON et.ExceptionTypeSK = f.ExceptionTypeSK
GROUP BY et.ExceptionType
ORDER BY ExceptionCount DESC;

PRINT '===== COLUMN PROFILER COMPLETE =====';
GO
