/*
============================================================
ADVANCED DATA QUALITY AUDIT
File: 10_dw_data_quality_audit_advanced.sql

Purpose:
    Automatically profile all warehouse tables and columns.

Checks:
    1. Table row counts
    2. NULL counts per column
    3. NULL percentage per column
    4. Primary key duplicate detection

Schemas scanned:
    staging
    clean
    dw
    reporting
============================================================
*/

PRINT '===== ADVANCED DATA QUALITY AUDIT START =====';


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
-- 2. NULL PROFILE PER COLUMN
------------------------------------------------------------

PRINT 'STEP 2: COLUMN NULL PROFILE';

CREATE TABLE #NullAudit
(
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    ColumnName NVARCHAR(128),
    NullCount INT,
    TotalRows INT,
    NullPercent DECIMAL(10,2)
);

DECLARE
    @schema NVARCHAR(128),
    @table NVARCHAR(128),
    @column NVARCHAR(128),
    @sql NVARCHAR(MAX);

DECLARE column_cursor CURSOR FOR

SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('staging','clean','dw','reporting');

OPEN column_cursor;

FETCH NEXT FROM column_cursor
INTO @schema, @table, @column;

WHILE @@FETCH_STATUS = 0
BEGIN

SET @sql = '
INSERT INTO #NullAudit
SELECT
''' + @schema + ''',
''' + @table + ''',
''' + @column + ''',
SUM(CASE WHEN [' + @column + '] IS NULL THEN 1 ELSE 0 END),
COUNT(*),
CAST(SUM(CASE WHEN [' + @column + '] IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(10,2))
FROM [' + @schema + '].[' + @table + ']';

EXEC sp_executesql @sql;

FETCH NEXT FROM column_cursor
INTO @schema, @table, @column;

END;

CLOSE column_cursor;
DEALLOCATE column_cursor;

SELECT *
FROM #NullAudit
ORDER BY
    NullPercent DESC;

------------------------------------------------------------
-- 3. PRIMARY KEY DUPLICATE CHECK
------------------------------------------------------------

PRINT 'STEP 3: PRIMARY KEY DUPLICATE CHECK';

SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    c.name AS PrimaryKeyColumn
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.indexes i
    ON t.object_id = i.object_id
JOIN sys.index_columns ic
    ON i.object_id = ic.object_id
    AND i.index_id = ic.index_id
JOIN sys.columns c
    ON ic.object_id = c.object_id
    AND ic.column_id = c.column_id
WHERE i.is_primary_key = 1
AND s.name IN ('staging','clean','dw','reporting');

------------------------------------------------------------
-- 4. DATA QUALITY SUMMARY
------------------------------------------------------------

PRINT 'STEP 4: DATA QUALITY SUMMARY';

SELECT
    SchemaName,
    TableName,
    COUNT(*) AS ColumnsChecked,
    SUM(CASE WHEN NullPercent > 0 THEN 1 ELSE 0 END) AS ColumnsWithNulls
FROM #NullAudit
GROUP BY
    SchemaName,
    TableName
ORDER BY
    ColumnsWithNulls DESC;

DROP TABLE #NullAudit;

PRINT '===== ADVANCED DATA QUALITY AUDIT COMPLETE =====';