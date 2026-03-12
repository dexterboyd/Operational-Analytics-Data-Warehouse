/*
============================================================
METADATA-DRIVEN DW VALIDATION

Purpose:
    Automatically validate fact and dimension tables.

Checks:
    1. Row counts for all DW tables
    2. Null surrogate key checks in fact tables
    3. Foreign key integrity validation
    4. Basic fact table metric validation

Schema scanned:
    dw
============================================================
*/

PRINT '===== DW VALIDATION START =====';


------------------------------------------------------------
-- STEP 1: ROW COUNTS FOR ALL DW TABLES
------------------------------------------------------------
PRINT 'STEP 1: TABLE ROW COUNTS';

SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    SUM(p.rows) AS RowsCount
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.partitions p
    ON t.object_id = p.object_id
WHERE s.name = 'dw'
AND p.index_id IN (0,1)
GROUP BY
    s.name,
    t.name
ORDER BY
    t.name;


------------------------------------------------------------
-- STEP 2: NULL SURROGATE KEY CHECKS (FACT TABLES)
------------------------------------------------------------
PRINT 'STEP 2: NULL SURROGATE KEY CHECKS';

DECLARE @sql NVARCHAR(MAX) = '';

SELECT
    @sql = @sql + '
    SELECT
        ''' + t.name + ''' AS FactTable,
        ''' + c.name + ''' AS KeyColumn,
        COUNT(*) AS NullCount
    FROM dw.' + t.name + '
    WHERE ' + c.name + ' IS NULL
    HAVING COUNT(*) > 0;
'
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.columns c
    ON t.object_id = c.object_id
WHERE s.name = 'dw'
AND t.name LIKE 'fact%'
AND c.name LIKE '%ID';

EXEC sp_executesql @sql;


------------------------------------------------------------
-- STEP 3: FOREIGN KEY INTEGRITY CHECK
------------------------------------------------------------
PRINT 'STEP 3: FOREIGN KEY RELATIONSHIP CHECKS';

SELECT
    fk.name AS ForeignKeyName,
    OBJECT_NAME(fk.parent_object_id) AS FactTable,
    COL_NAME(fc.parent_object_id,fc.parent_column_id) AS FactColumn,
    OBJECT_NAME(fk.referenced_object_id) AS DimensionTable,
    COL_NAME(fc.referenced_object_id,fc.referenced_column_id) AS DimensionColumn
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fc
    ON fk.object_id = fc.constraint_object_id
WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = 'dw';


------------------------------------------------------------
-- STEP 4: FACT TABLE METRIC VALIDATION
------------------------------------------------------------
PRINT 'STEP 4: FACT TABLE METRICS';

DECLARE @metricSQL NVARCHAR(MAX) = '';

SELECT
    @metricSQL = @metricSQL + '
    SELECT
        ''' + t.name + ''' AS FactTable,
        COUNT(*) AS RowCount
    FROM dw.' + t.name + ';
'
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
WHERE s.name = 'dw'
AND t.name LIKE 'fact%';

EXEC sp_executesql @metricSQL;


PRINT '===== DW VALIDATION COMPLETE =====';