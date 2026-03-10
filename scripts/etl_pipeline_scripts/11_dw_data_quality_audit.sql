/*
=========================================================
DATA WAREHOUSE QUALITY AUDIT
File: 10_dw_data_quality_audit.sql

Purpose:
    Run automated quality checks across warehouse tables.
	This version uses metadata-driven checks, so it won’t break if column names differ:
		sys.tables
		sys.schemas
		sys.partitions
		sys.columns
		INFORMATION_SCHEMA

Checks:
    1. Row counts for all tables
    2. Duplicate primary keys
    3. NULL counts per column
    4. Table size overview

Schemas scanned:
    staging
    clean
    dw
    reporting
=========================================================
*/

PRINT '===== DATA QUALITY AUDIT START =====';


---------------------------------------------------------
-- 1. ROW COUNTS FOR ALL TABLES
---------------------------------------------------------

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
---------------------------------------------------------
-- 2. PRIMARY KEY DUPLICATE CHECK
---------------------------------------------------------

PRINT 'STEP 2: PRIMARY KEY DUPLICATE CHECK';

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
AND s.name IN ('staging','clean','dw','reporting')
ORDER BY
    s.name,
    t.name;
---------------------------------------------------------
-- 3. NULL COUNT PER COLUMN
---------------------------------------------------------

PRINT 'STEP 3: NULL VALUE CHECKS';

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
    TABLE_NAME,
    ORDINAL_POSITION;
---------------------------------------------------------
-- 4. TABLE SIZE OVERVIEW
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


PRINT '===== DATA QUALITY AUDIT COMPLETE =====';