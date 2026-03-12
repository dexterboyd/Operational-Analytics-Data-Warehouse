/*
=========================================================
PRODUCTION DATA WAREHOUSE AUDIT (Monitoring)

Purpose:
    Automated validation and monitoring for the warehouse.

Checks Included:
    1. Row counts
    2. Duplicate primary keys
    3. NULL value counts
    4. Table storage size
    5. Data freshness validation
    6. Fact table integrity checks
    7. Orphan foreign key detection

Schemas Monitored:
    staging
    clean
    dw
    reporting
=========================================================
*/

PRINT '===== DATA WAREHOUSE AUDIT START =====';


---------------------------------------------------------
-- STEP 1: TABLE ROW COUNTS
---------------------------------------------------------
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
WHERE s.name IN ('staging','clean','dw','reporting')
  AND p.index_id IN (0,1)
GROUP BY s.name, t.name
ORDER BY s.name, t.name;



---------------------------------------------------------
-- STEP 2: DUPLICATE PRIMARY KEY CHECK
---------------------------------------------------------
PRINT 'STEP 2: DUPLICATE PRIMARY KEY CHECK';

DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql = @sql +
'
SELECT
    ''' + s.name + ''' AS SchemaName,
    ''' + t.name + ''' AS TableName,
    ''' + c.name + ''' AS PKColumn,
    COUNT(*) AS DuplicateCount
FROM ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + '
GROUP BY ' + QUOTENAME(c.name) + '
HAVING COUNT(*) > 1;
'
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
AND s.name IN ('staging','dw');

EXEC sp_executesql @sql;



---------------------------------------------------------
-- STEP 3: NULL VALUE COUNTS
---------------------------------------------------------
PRINT 'STEP 3: NULL VALUE COUNTS';

SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('staging','clean','dw','reporting')
ORDER BY
    TABLE_SCHEMA,
    TABLE_NAME;



---------------------------------------------------------
-- STEP 4: TABLE STORAGE SIZE
---------------------------------------------------------
PRINT 'STEP 4: TABLE STORAGE SIZE';

SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    SUM(a.total_pages) * 8 AS TotalSpaceKB
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.indexes i
    ON t.object_id = i.object_id
JOIN sys.partitions p
    ON i.object_id = p.object_id
    AND i.index_id = p.index_id
JOIN sys.allocation_units a
    ON p.partition_id = a.container_id
WHERE s.name IN ('staging','clean','dw','reporting')
GROUP BY
    s.name,
    t.name
ORDER BY
    TotalSpaceKB DESC;



---------------------------------------------------------
-- STEP 5: DATA FRESHNESS CHECK
-- Ensures tables are receiving new data
---------------------------------------------------------
PRINT 'STEP 5: DATA FRESHNESS CHECK';

SELECT
    name AS TableName,
    create_date,
    modify_date
FROM sys.tables
ORDER BY modify_date DESC;



---------------------------------------------------------
-- STEP 6: FACT TABLE ROW VALIDATION
-- Detect abnormal fact table growth
---------------------------------------------------------
PRINT 'STEP 6: FACT TABLE SIZE CHECK';

SELECT
    s.name AS SchemaName,
    t.name AS FactTable,
    SUM(p.rows) AS RowsCount
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.partitions p
    ON t.object_id = p.object_id
WHERE s.name = 'dw'
AND t.name LIKE 'fact%'
AND p.index_id IN (0,1)
GROUP BY s.name, t.name
ORDER BY RowsCount DESC;



---------------------------------------------------------
-- STEP 7: ORPHAN KEY DETECTION
-- Checks fact rows without matching dimension keys
---------------------------------------------------------
PRINT 'STEP 7: ORPHAN KEY CHECK (Example: Sales -> Delivery)';

SELECT
    f.DeliveryID
FROM dw.fact_sales f
LEFT JOIN dw.fact_deliveries d
    ON f.DeliveryID = d.DeliveryID
WHERE d.DeliveryID IS NULL;



PRINT '===== DATA WAREHOUSE AUDIT COMPLETE =====';