/*==============================================================
  DW COLUMN PROFILER
  Database: Fedex_Ops_Database
  Version:  2.0

  Purpose:
      Deep column-level data quality profiling across all
      base tables in the warehouse. Informational only —
      does not halt the pipeline. Run after a DW load to
      investigate data quality, completeness, and distribution.

  Steps:
      1.  Table row counts
      2.  Column NULL profiling  (actual NULL counts + %)
      3.  Data quality scorecard (per-table NULL summary)
      4.  Critical NULL flags    (columns > 20% NULL)

  Schemas Scanned:
      staging, dw
      (clean and reporting contain views — NULL profiling
       against views is valid but expensive; excluded here
       since their columns are already validated by the clean
       and DW validation gate scripts)

  Change Log:
      v2.0 - Scoped profiling to BASE TABLE only by joining
             INFORMATION_SCHEMA.TABLES on TABLE_TYPE. The
             original queried INFORMATION_SCHEMA.COLUMNS
             alone which includes view columns; if a view
             had been dropped, the dynamic SQL would fail
             mid-loop with no temp table cleanup.
           - Added QUOTENAME() around all dynamically
             assembled column and table identifiers. The
             original used bare string concatenation, which
             fails on reserved-word column names such as
             [Year], [Month], [Day] in dim_date.
           - Row counts from Step 1 are stored in a temp
             table and reused in Step 2's percentage
             calculation, eliminating the double full-scan
             per column that the original performed.
           - Added DROP TABLE IF EXISTS guard on temp tables
             so the script is safe to re-run in the same
             session.
           - Renamed file prefix from 08_ to 11_ to avoid
             filename-ordering collision with the load and
             validation scripts.
==============================================================*/

PRINT '===== COLUMN PROFILER START =====';


/*==============================================================
  STEP 1: TABLE ROW COUNTS
  Row counts are stored in #TableRowCounts so Step 2 can
  reuse them without re-scanning every table.
==============================================================*/
PRINT '--- STEP 1: TABLE ROW COUNTS ---';

IF OBJECT_ID('tempdb..#TableRowCounts') IS NOT NULL
    DROP TABLE #TableRowCounts;

CREATE TABLE #TableRowCounts
(
    SchemaName NVARCHAR(128),
    TableName  NVARCHAR(128),
    RowCount   BIGINT
);

INSERT INTO #TableRowCounts (SchemaName, TableName, RowCount)
SELECT
    s.name,
    t.name,
    SUM(p.rows)
FROM sys.tables    t
JOIN sys.schemas    s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
                      AND p.index_id IN (0, 1)
WHERE s.name IN ('staging', 'dw')
GROUP BY s.name, t.name;

SELECT *
FROM #TableRowCounts
ORDER BY SchemaName, TableName;


/*==============================================================
  STEP 2: COLUMN NULL PROFILING
  For every column in every base table in the scoped schemas,
  count actual NULLs and compute the NULL percentage.
  Row counts are sourced from #TableRowCounts (pre-computed
  in Step 1) to avoid a redundant COUNT(*) per column.
  QUOTENAME() ensures reserved-word column names are safe.
==============================================================*/
PRINT '--- STEP 2: COLUMN NULL PROFILING ---';

IF OBJECT_ID('tempdb..#ColumnProfile') IS NOT NULL
    DROP TABLE #ColumnProfile;

CREATE TABLE #ColumnProfile
(
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
    rc.RowCount,
    CAST(
        SUM(CASE WHEN ' + QUOTENAME(c.COLUMN_NAME) + ' IS NULL THEN 1 ELSE 0 END)
        * 100.0 / NULLIF(rc.RowCount, 0)
    AS DECIMAL(10,2))
FROM ' + QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME) + '
CROSS JOIN (
    SELECT RowCount FROM #TableRowCounts
    WHERE SchemaName = ''' + c.TABLE_SCHEMA + '''
      AND TableName  = ''' + c.TABLE_NAME   + '''
) rc
GROUP BY rc.RowCount;'
FROM INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.TABLES  t
    ON  c.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND c.TABLE_NAME   = t.TABLE_NAME
    AND t.TABLE_TYPE   = 'BASE TABLE'  -- exclude views
WHERE c.TABLE_SCHEMA IN ('staging', 'dw');

EXEC sp_executesql @profileSQL;

SELECT *
FROM #ColumnProfile
ORDER BY NullPct DESC, SchemaName, TableName, ColumnName;


/*==============================================================
  STEP 3: DATA QUALITY SCORECARD
  Summarises NULL distribution per table: how many columns
  have any NULLs, and the average NULL % across all columns.
  Tables with high AvgNullPct warrant investigation.
==============================================================*/
PRINT '--- STEP 3: DATA QUALITY SCORECARD ---';

SELECT
    SchemaName,
    TableName,
    COUNT(*)                                            AS ColumnsChecked,
    SUM(CASE WHEN NullPct > 0 THEN 1 ELSE 0 END)       AS ColumnsWithNulls,
    AVG(NullPct)                                        AS AvgNullPct,
    MAX(NullPct)                                        AS MaxNullPct
FROM #ColumnProfile
GROUP BY SchemaName, TableName
ORDER BY AvgNullPct DESC;


/*==============================================================
  STEP 4: CRITICAL NULL FLAGS
  Lists every column where more than 20% of rows are NULL.
  These may indicate a load failure, a missing source join,
  or a column that was added after the initial data load.
==============================================================*/
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

PRINT '===== COLUMN PROFILER COMPLETE =====';
