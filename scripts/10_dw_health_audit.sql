/*=============================================================
  DW HEALTH AUDIT
  Database: Fedex_Ops_Database
  Version:  3.0

  Purpose:
      Ongoing production monitoring of the Data Warehouse.
      Informational only -- does not halt the pipeline.
      Run on a schedule (e.g. daily SQL Agent job) to detect
      drift, growth anomalies, and structural issues.

  Run Order:
      This script runs independently on a schedule after the
      pipeline has completed. It is not part of the linear
      load sequence.

  Checks:
      1. Table row counts          (staging and dw schemas)
      2. Duplicate primary keys    (all staging and dw tables)
      3. Actual NULL value counts  (per column, per table)
      4. Table storage size        (KB allocated and used)
      5. Data freshness            (note on LoadDate column)
      6. Fact table row counts     (focused fact-only check)
      7. Orphaned fact rows        (fact-to-dimension joins)

  Schemas Monitored:
      staging, dw
      clean and reporting contain views only -- row counts
      and storage metrics are not applicable to views.

=============================================================*/

USE Fedex_Ops_Database;
GO

PRINT '===== DATA WAREHOUSE HEALTH AUDIT START =====';


/*=============================================================
  STEP 1: TABLE ROW COUNTS
  Reports row counts for all base tables in staging and dw.
  Clean and reporting schemas contain views only and are
  excluded -- view row counts are reported by the clean layer
  profiling script instead.
=============================================================*/

PRINT '--- STEP 1: TABLE ROW COUNTS ---';

SELECT
    s.name                                              AS SchemaName,
    t.name                                              AS TableName,
    SUM(p.rows)                                         AS RowsCount
FROM sys.tables     t
JOIN sys.schemas    s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
                      AND p.index_id IN (0, 1)
WHERE s.name IN ('staging', 'dw')
GROUP BY s.name, t.name
ORDER BY
    s.name,
    CASE WHEN t.name LIKE 'dim%'  THEN 0
         WHEN t.name LIKE 'fact%' THEN 1
         ELSE 2
    END,
    t.name;


/*=============================================================
  STEP 2: DUPLICATE PRIMARY KEY DETECTION
  Dynamically checks every single-column primary key in
  staging and dw schemas. The composite PK on staging_routes
  (RouteID + DriverID) is checked separately below the
  dynamic block as it cannot be detected generically.

  QUOTENAME() wraps all identifiers to handle reserved-word
  column names such as [Year], [Month], [Day] in dim_date.
=============================================================*/

PRINT '--- STEP 2: DUPLICATE PRIMARY KEY CHECK ---';

DECLARE @dupSQL NVARCHAR(MAX) = '';

SELECT @dupSQL = @dupSQL + '
SELECT
    ''' + s.name + '.' + t.name + ''' AS TableName,
    ''' + c.name                      + ''' AS PKColumn,
    COUNT(*)                               AS DuplicateGroupCount
FROM ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + '
GROUP BY ' + QUOTENAME(c.name) + '
HAVING COUNT(*) > 1;'
FROM sys.tables        t
JOIN sys.schemas        s  ON t.schema_id    = s.schema_id
JOIN sys.indexes        i  ON t.object_id    = i.object_id
                           AND i.is_primary_key = 1
JOIN sys.index_columns  ic ON i.object_id    = ic.object_id
                           AND i.index_id    = ic.index_id
                           AND ic.key_ordinal = 1       -- single-column PKs only
JOIN sys.columns        c  ON ic.object_id   = c.object_id
                           AND ic.column_id  = c.column_id
WHERE s.name IN ('staging', 'dw');

IF LEN(@dupSQL) > 0
    EXEC sp_executesql @dupSQL;

-- Composite PK check for staging_routes (RouteID + DriverID)
-- dw.fact_routes does not exist -- route data is accessed via
-- dim_route joined to fact_deliveries using RouteSK.
SELECT
    'staging.staging_routes'    AS TableName,
    'RouteID + DriverID'        AS PKColumns,
    COUNT(*)                    AS DuplicateGroupCount
FROM staging.staging_routes
GROUP BY RouteID, DriverID
HAVING COUNT(*) > 1;


/*=============================================================
  STEP 3: NULL VALUE COUNTS PER COLUMN
  Counts actual NULLs in every column of every base table in
  staging and dw. Dynamic SQL is used so all columns are
  covered automatically without manually listing them.

  QUOTENAME() wraps all identifiers to handle reserved-word
  column names such as [Year], [Month], [Day] in dim_date.
  Only columns with at least one NULL are shown to reduce noise.
  A second result set flags columns above 20% NULL threshold.
=============================================================*/

PRINT '--- STEP 3: NULL VALUE COUNTS ---';

IF OBJECT_ID('tempdb..#NullCounts') IS NOT NULL
    DROP TABLE #NullCounts;

CREATE TABLE #NullCounts (
    SchemaName  NVARCHAR(128),
    TableName   NVARCHAR(128),
    ColumnName  NVARCHAR(128),
    NullCount   BIGINT,
    TotalRows   BIGINT,
    NullPct     DECIMAL(10,2)
);

DECLARE @nullSQL NVARCHAR(MAX) = '';

SELECT @nullSQL = @nullSQL + '
INSERT INTO #NullCounts
SELECT
    ''' + c.TABLE_SCHEMA + ''',
    ''' + c.TABLE_NAME   + ''',
    ''' + c.COLUMN_NAME  + ''',
    SUM(CASE WHEN ' + QUOTENAME(c.COLUMN_NAME) + ' IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    CAST(
        SUM(CASE WHEN ' + QUOTENAME(c.COLUMN_NAME) + ' IS NULL THEN 1 ELSE 0 END)
        * 100.0 / NULLIF(COUNT(*), 0)
    AS DECIMAL(10,2))
FROM ' + QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME) + ';'
FROM INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.TABLES  t
    ON  c.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND c.TABLE_NAME   = t.TABLE_NAME
    AND t.TABLE_TYPE   = 'BASE TABLE'   -- exclude views
WHERE c.TABLE_SCHEMA IN ('staging', 'dw');

EXEC sp_executesql @nullSQL;

-- Show only columns that actually contain NULLs
-- Expected NULLs: staging_deliveries.DriverID (~1010),
-- staging_routes.DriverID (~59), staging_exceptions.ResolvedDate (~30),
-- fact_exceptions.ResolvedDateKey (~30), fact_exceptions.ResolutionTimeHours (~30),
-- fact_exceptions.ResolutionDays (~30), fact_deliveries.DaysVariance (small number)
SELECT *
FROM #NullCounts
WHERE NullCount > 0
ORDER BY NullPct DESC, SchemaName, TableName, ColumnName;

-- Columns above 20% NULL threshold warrant investigation
PRINT 'Columns with > 20% NULLs:';
SELECT *
FROM #NullCounts
WHERE NullPct > 20
ORDER BY NullPct DESC;

DROP TABLE #NullCounts;


/*=============================================================
  STEP 4: TABLE STORAGE SIZE
  Reports allocated and used storage per table in KB.
  Useful for tracking growth trends over time.
=============================================================*/

PRINT '--- STEP 4: TABLE STORAGE SIZE ---';

SELECT
    s.name                                              AS SchemaName,
    t.name                                              AS TableName,
    SUM(a.total_pages) * 8                              AS TotalSpaceKB,
    SUM(a.used_pages)  * 8                              AS UsedSpaceKB
FROM sys.tables          t
JOIN sys.schemas          s  ON t.schema_id     = s.schema_id
JOIN sys.indexes          i  ON t.object_id     = i.object_id
JOIN sys.partitions       p  ON i.object_id     = p.object_id
                             AND i.index_id     = p.index_id
JOIN sys.allocation_units a  ON p.partition_id  = a.container_id
WHERE s.name IN ('staging', 'dw')
GROUP BY s.name, t.name
ORDER BY TotalSpaceKB DESC;


/*=============================================================
  STEP 5: DATA FRESHNESS
  Reports when DW fact tables were last structurally modified.

  NOTE: sys.tables.modify_date reflects DDL changes (schema
  modifications) not DML activity (row inserts). It is used
  here as a proxy only.

  To track true data freshness, add a LoadDate column to each
  fact table in dw_layer_v1.sql:

      LoadDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()

  Then query MAX(LoadDate) per fact table:

      SELECT 'fact_deliveries', MAX(LoadDate) FROM dw.fact_deliveries
      UNION ALL
      SELECT 'fact_sales',      MAX(LoadDate) FROM dw.fact_sales
      UNION ALL
      SELECT 'fact_exceptions', MAX(LoadDate) FROM dw.fact_exceptions;
=============================================================*/

PRINT '--- STEP 5: DATA FRESHNESS (DDL PROXY) ---';

PRINT 'NOTE: modify_date reflects schema changes not row inserts.';
PRINT 'Add LoadDate DEFAULT SYSUTCDATETIME() to fact tables for true DML freshness.';

SELECT
    name                                                AS TableName,
    create_date                                         AS CreatedDate,
    modify_date                                         AS LastDDLChange
FROM sys.tables
WHERE OBJECT_SCHEMA_NAME(object_id) = 'dw'
ORDER BY modify_date DESC;


/*=============================================================
  STEP 6: FACT TABLE ROW COUNT FOCUS
  Focused check on fact tables only to detect abnormal growth
  or unexpectedly empty loads. Expected counts are documented
  inline for quick comparison.

  Expected:
      fact_deliveries  5,000
      fact_sales       4,000
      fact_exceptions  1,000
=============================================================*/

PRINT '--- STEP 6: FACT TABLE ROW COUNTS ---';

SELECT
    s.name                                              AS SchemaName,
    t.name                                              AS FactTable,
    SUM(p.rows)                                         AS RowsCount
FROM sys.tables     t
JOIN sys.schemas    s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
                      AND p.index_id IN (0, 1)
WHERE s.name = 'dw'
  AND t.name LIKE 'fact%'
GROUP BY s.name, t.name
ORDER BY RowsCount DESC;


/*=============================================================
  STEP 7: ORPHANED FACT ROW DETECTION
  Detects fact rows with FK values that have no matching row
  in their referenced dimension or parent fact table.
  These represent referential integrity violations that should
  not exist after a clean load but are worth monitoring as a
  safeguard against future load issues.

  All joins use surrogate key columns (DriverSK, RegionSK etc.)
  not natural keys. dim_driver is joined via DriverSK not
  DriverCode. All counts should be 0.

  NOTE: fact_routes does not exist in this pipeline. Route
  context is accessed by joining fact_deliveries to dim_route
  via RouteSK. No separate route orphan check is needed.
=============================================================*/

PRINT '--- STEP 7: ORPHANED KEY DETECTION ---';

-- fact_sales orphan checks
SELECT 'fact_sales -> dim_date'              AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.DateKey)

UNION ALL
SELECT 'fact_sales -> dim_product'           AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_product d WHERE d.ProductSK = f.ProductSK)

UNION ALL
SELECT 'fact_sales -> dim_region'            AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionSK = f.RegionSK)

UNION ALL
SELECT 'fact_sales -> fact_deliveries'       AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.fact_deliveries d WHERE d.DeliveryID = f.DeliveryID)

-- fact_deliveries orphan checks
UNION ALL
SELECT 'fact_deliveries -> dim_date'         AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.DateKey)

UNION ALL
SELECT 'fact_deliveries -> dim_driver'       AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_driver d WHERE d.DriverSK = f.DriverSK)

UNION ALL
SELECT 'fact_deliveries -> dim_region'       AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionSK = f.RegionSK)

UNION ALL
SELECT 'fact_deliveries -> dim_shipment_type' AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_shipment_type d WHERE d.ShipmentTypeSK = f.ShipmentTypeSK)

UNION ALL
SELECT 'fact_deliveries -> dim_route'        AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_route d WHERE d.RouteSK = f.RouteSK)

-- fact_exceptions orphan checks
UNION ALL
SELECT 'fact_exceptions -> dim_date (DateReportedKey)' AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.DateReportedKey)

UNION ALL
SELECT 'fact_exceptions -> dim_date (ResolvedDateKey)' AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_exceptions f
WHERE ResolvedDateKey IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.ResolvedDateKey)

UNION ALL
SELECT 'fact_exceptions -> dim_exception_type' AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_exception_type d WHERE d.ExceptionTypeSK = f.ExceptionTypeSK)

UNION ALL
SELECT 'fact_exceptions -> dim_region'       AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionSK = f.RegionSK)

UNION ALL
SELECT 'fact_exceptions -> fact_deliveries'  AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.fact_deliveries d WHERE d.DeliveryID = f.DeliveryID)

ORDER BY OrphanCount DESC;

PRINT '===== DATA WAREHOUSE HEALTH AUDIT COMPLETE =====';
GO
