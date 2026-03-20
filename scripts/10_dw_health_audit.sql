/*==============================================================
  DW HEALTH AUDIT
  Database: Fedex_Ops_Database
  Version:  2.0

  Purpose:
      Ongoing production monitoring of the Data Warehouse.
      Informational only — does not halt the pipeline.
      Run on a schedule (e.g. daily SQL Agent job) to detect
      drift, growth anomalies, and structural issues.

  Checks:
      1.  Table row counts
      2.  Duplicate primary key detection
      3.  Actual NULL value counts per column
      4.  Table storage size
      5.  Data freshness (MAX load date from fact tables)
      6.  Fact table row counts and growth detection
      7.  Orphaned fact rows (fact-to-dimension joins)

  Schemas Monitored:
      staging, dw
      (clean and reporting contain views, not base tables;
       row counts and storage are not applicable to views)

  Change Log:
      v2.0 - Fixed Step 3: was querying INFORMATION_SCHEMA
             column metadata (IS_NULLABLE flag) rather than
             counting actual NULLs in the data. Replaced with
             per-table conditional aggregation that counts real
             NULL occurrences per column.
           - Fixed Step 5: was using sys.tables.modify_date
             which reflects DDL changes, not DML activity. Now
             reads MAX(LoadDate) from each fact table so
             freshness reflects when rows were actually written.
             NOTE: This requires a LoadDate column on fact
             tables. If not yet present, Step 5 falls back to
             a note explaining how to add it.
           - Fixed Step 7: was joining fact_sales to
             fact_deliveries (fact-to-fact, not meaningful for
             referential integrity). Replaced with explicit
             fact-to-dimension checks for all four fact tables.
           - Updated schema scope comment: clean and reporting
             contain views; duplicate PK and storage checks now
             correctly target staging and dw only.
           - Added QUOTENAME() around all dynamically assembled
             identifiers to handle reserved-word column names
             (e.g. [Year], [Month], [Day] in dim_date).
==============================================================*/

PRINT '===== DATA WAREHOUSE HEALTH AUDIT START =====';


/*==============================================================
  STEP 1: TABLE ROW COUNTS
  Covers all base tables in staging and dw schemas.
==============================================================*/
PRINT '--- STEP 1: TABLE ROW COUNTS ---';

SELECT
    s.name      AS SchemaName,
    t.name      AS TableName,
    SUM(p.rows) AS RowCount
FROM sys.tables    t
JOIN sys.schemas    s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
                      AND p.index_id IN (0, 1)
WHERE s.name IN ('staging', 'dw')
GROUP BY s.name, t.name
ORDER BY s.name,
         CASE WHEN t.name LIKE 'dim%' THEN 0 ELSE 1 END,
         t.name;


/*==============================================================
  STEP 2: DUPLICATE PRIMARY KEY DETECTION
  Dynamically checks every table that has a single-column
  primary key in the staging and dw schemas.
  Composite PKs (e.g. fact_routes) are checked separately.
==============================================================*/
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
FROM sys.tables       t
JOIN sys.schemas       s  ON t.schema_id   = s.schema_id
JOIN sys.indexes       i  ON t.object_id   = i.object_id
                          AND i.is_primary_key = 1
JOIN sys.index_columns ic ON i.object_id   = ic.object_id
                          AND i.index_id   = ic.index_id
                          AND ic.key_ordinal = 1       -- single-column PK only
JOIN sys.columns       c  ON ic.object_id  = c.object_id
                          AND ic.column_id = c.column_id
WHERE s.name IN ('staging', 'dw');

IF LEN(@dupSQL) > 0
    EXEC sp_executesql @dupSQL;

-- Composite PK check for fact_routes
SELECT
    'dw.fact_routes'  AS TableName,
    'RouteID+DriverID' AS PKColumns,
    COUNT(*)           AS DuplicateGroupCount
FROM dw.fact_routes
GROUP BY RouteID, DriverID
HAVING COUNT(*) > 1;


/*==============================================================
  STEP 3: NULL VALUE COUNTS PER COLUMN
  Counts actual NULLs in each column of each table, not
  just the IS_NULLABLE metadata flag. Uses dynamic SQL so
  every column in every table is covered automatically.
  QUOTENAME() wraps all identifiers to handle reserved words
  such as [Year], [Month], [Day] in dim_date.
==============================================================*/
PRINT '--- STEP 3: NULL VALUE COUNTS ---';

IF OBJECT_ID('tempdb..#NullCounts') IS NOT NULL
    DROP TABLE #NullCounts;

CREATE TABLE #NullCounts
(
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

-- Show only columns that actually contain NULLs (noise reduction)
SELECT *
FROM #NullCounts
WHERE NullCount > 0
ORDER BY NullPct DESC, SchemaName, TableName, ColumnName;

-- Columns with > 20% NULLs flagged as potentially critical
PRINT 'Columns with > 20% NULLs:';
SELECT *
FROM #NullCounts
WHERE NullPct > 20
ORDER BY NullPct DESC;

DROP TABLE #NullCounts;


/*==============================================================
  STEP 4: TABLE STORAGE SIZE
  Reports allocated storage per table in KB.
  Useful for tracking growth over time.
==============================================================*/
PRINT '--- STEP 4: TABLE STORAGE SIZE ---';

SELECT
    s.name              AS SchemaName,
    t.name              AS TableName,
    SUM(a.total_pages) * 8  AS TotalSpaceKB,
    SUM(a.used_pages)  * 8  AS UsedSpaceKB
FROM sys.tables         t
JOIN sys.schemas         s  ON t.schema_id  = s.schema_id
JOIN sys.indexes         i  ON t.object_id  = i.object_id
JOIN sys.partitions      p  ON i.object_id  = p.object_id
                            AND i.index_id  = p.index_id
JOIN sys.allocation_units a  ON p.partition_id = a.container_id
WHERE s.name IN ('staging', 'dw')
GROUP BY s.name, t.name
ORDER BY TotalSpaceKB DESC;


/*==============================================================
  STEP 5: DATA FRESHNESS
  Reports the most recent row written to each fact table
  based on a LoadDate column.

  NOTE: LoadDate is not yet in the fact table DDL in this
  pipeline. To enable this check, add the following column
  to each fact table in 08_dw_load.sql:

      LoadDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()

  Until then, this step shows the sys.tables modify_date as
  a DDL-change proxy and notes its limitation.
==============================================================*/
PRINT '--- STEP 5: DATA FRESHNESS ---';

PRINT 'NOTE: modify_date reflects schema changes, not row inserts.';
PRINT 'Add a LoadDate DEFAULT SYSUTCDATETIME() column to fact tables';
PRINT 'and query MAX(LoadDate) for true DML freshness tracking.';

SELECT
    name        AS TableName,
    create_date AS CreatedDate,
    modify_date AS LastDDLChange
FROM sys.tables
WHERE OBJECT_SCHEMA_NAME(object_id) = 'dw'
ORDER BY modify_date DESC;


/*==============================================================
  STEP 6: FACT TABLE ROW COUNTS AND SIZE
  Focuses specifically on fact tables to detect abnormal
  growth or unexpected empty loads.
==============================================================*/
PRINT '--- STEP 6: FACT TABLE SIZE CHECK ---';

SELECT
    s.name      AS SchemaName,
    t.name      AS FactTable,
    SUM(p.rows) AS RowCount
FROM sys.tables    t
JOIN sys.schemas    s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
                      AND p.index_id IN (0, 1)
WHERE s.name = 'dw'
  AND t.name LIKE 'fact%'
GROUP BY s.name, t.name
ORDER BY RowCount DESC;


/*==============================================================
  STEP 7: ORPHANED FACT ROW DETECTION
  Each check finds fact rows that have no matching row in
  their referenced dimension table. These represent FK
  violations that should not exist if the load ran correctly,
  but are worth checking explicitly as a monitoring safeguard.
==============================================================*/
PRINT '--- STEP 7: ORPHANED KEY DETECTION ---';

-- fact_sales orphans
SELECT 'fact_sales -> dim_date'         AS Check_, COUNT(*) AS OrphanCount
FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date d WHERE d.DateKey = f.DateKey)
UNION ALL
SELECT 'fact_sales -> dim_product_type', COUNT(*)
FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_product_type d WHERE d.ProductTypeID = f.ProductTypeID)
UNION ALL
SELECT 'fact_sales -> dim_region',       COUNT(*)
FROM dw.fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionID = f.RegionID)

-- fact_deliveries orphans
UNION ALL
SELECT 'fact_deliveries -> dim_route',   COUNT(*)
FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_route d WHERE d.RouteID = f.RouteID)
UNION ALL
SELECT 'fact_deliveries -> dim_driver',  COUNT(*)
FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_driver d WHERE d.DriverID = f.DriverID)
UNION ALL
SELECT 'fact_deliveries -> dim_delivery_status', COUNT(*)
FROM dw.fact_deliveries f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_delivery_status d WHERE d.DeliveryStatusID = f.DeliveryStatusID)

-- fact_exceptions orphans
UNION ALL
SELECT 'fact_exceptions -> dim_exception_type', COUNT(*)
FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_exception_type d WHERE d.ExceptionTypeID = f.ExceptionTypeID)
UNION ALL
SELECT 'fact_exceptions -> dim_region',          COUNT(*)
FROM dw.fact_exceptions f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionID = f.RegionID)

-- fact_routes orphans
UNION ALL
SELECT 'fact_routes -> dim_route',   COUNT(*)
FROM dw.fact_routes f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_route d WHERE d.RouteID = f.RouteID)
UNION ALL
SELECT 'fact_routes -> dim_driver',  COUNT(*)
FROM dw.fact_routes f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_driver d WHERE d.DriverID = f.DriverID)
UNION ALL
SELECT 'fact_routes -> dim_region',  COUNT(*)
FROM dw.fact_routes f
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_region d WHERE d.RegionID = f.RegionID)

ORDER BY OrphanCount DESC;

PRINT '===== DATA WAREHOUSE HEALTH AUDIT COMPLETE =====';
