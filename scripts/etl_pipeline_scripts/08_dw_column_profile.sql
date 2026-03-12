/*
============================================================
ENTERPRISE DATA QUALITY PROFILING

Purpose:
    Profile data quality across all warehouse tables.

Checks:
    1. Table row counts
    2. Column NULL counts
    3. Column NULL percentages
    4. Data quality scorecard

Schemas scanned:
    staging
    clean
    dw
    reporting
============================================================
*/

PRINT '===== ENTERPRISE DATA PROFILING START =====';

------------------------------------------------------------
-- 1. TABLE ROW COUNTS
------------------------------------------------------------

PRINT 'STEP 1: TABLE ROW COUNTS';

SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    SUM(p.rows) AS RowCounts
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
WHERE s.name IN ('staging','clean','dw','reporting')
AND p.index_id IN (0,1)
GROUP BY s.name, t.name
ORDER BY s.name, t.name;

------------------------------------------------------------
-- 2. COLUMN NULL PROFILING (SET-BASED)
------------------------------------------------------------

PRINT 'STEP 2: COLUMN NULL PROFILING';

CREATE TABLE #ColumnProfile
(
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    ColumnName NVARCHAR(128),
    NullCount BIGINT,
    TotalRows BIGINT,
    NullPercent DECIMAL(10,2)
);

DECLARE @sql NVARCHAR(MAX) = '';

SELECT
    @sql = @sql + '
    INSERT INTO #ColumnProfile
    SELECT
        ''' + TABLE_SCHEMA + ''',
        ''' + TABLE_NAME + ''',
        ''' + COLUMN_NAME + ''',
        SUM(CASE WHEN [' + COLUMN_NAME + '] IS NULL THEN 1 ELSE 0 END),
        COUNT(*),
        CAST(SUM(CASE WHEN [' + COLUMN_NAME + '] IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(10,2))
    FROM [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '];'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('staging','clean','dw','reporting');

EXEC sp_executesql @sql;

SELECT *
FROM #ColumnProfile
ORDER BY
    NullPercent DESC;

------------------------------------------------------------
-- 3. DATA QUALITY SCORECARD
------------------------------------------------------------

PRINT 'STEP 3: DATA QUALITY SCORECARD';

SELECT
    SchemaName,
    TableName,
    COUNT(*) AS ColumnsChecked,
    SUM(CASE WHEN NullPercent > 0 THEN 1 ELSE 0 END) AS ColumnsWithNulls,
    AVG(NullPercent) AS AvgNullPercent
FROM #ColumnProfile
GROUP BY
    SchemaName,
    TableName
ORDER BY
    AvgNullPercent DESC;

------------------------------------------------------------
-- 4. CRITICAL DATA QUALITY FLAGS
------------------------------------------------------------

PRINT 'STEP 4: CRITICAL DATA QUALITY FLAGS';

SELECT
    SchemaName,
    TableName,
    ColumnName,
    NullPercent
FROM #ColumnProfile
WHERE NullPercent > 20
ORDER BY
    NullPercent DESC;

DROP TABLE #ColumnProfile;

PRINT '===== ENTERPRISE DATA PROFILING COMPLETE =====';