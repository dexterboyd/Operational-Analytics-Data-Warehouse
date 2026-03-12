/*==============================================================
  DW VALIDATION SCRIPT (DYNAMIC VERSION)

  Purpose
      Validate Data Warehouse tables after ETL load using
      metadata-driven checks.

  Validations
      1. Row counts for all DW tables
      2. Fact table NULL checks for surrogate keys
      3. Fact → Dimension referential integrity checks
      4. Business metric sanity checks

  Pipeline Position
      staging → clean → DW load → DW validation → reporting → BI

==============================================================*/

PRINT '--- DW VALIDATION START ---';

-----------------------------------------------------
-- STEP 1: ROW COUNT SUMMARY (ALL DW TABLES)
-----------------------------------------------------

PRINT 'STEP 1: DW TABLE ROW COUNTS';

SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    SUM(p.rows) AS Row_Count
FROM sys.tables t
JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
JOIN sys.partitions p 
    ON t.object_id = p.object_id
WHERE s.name = 'dw'
AND p.index_id IN (0,1)
GROUP BY s.name, t.name
ORDER BY t.name;


-----------------------------------------------------
-- STEP 2: FACT TABLE NULL CHECKS (SURROGATE KEYS)
-----------------------------------------------------

PRINT 'STEP 2: FACT TABLE NULL CHECKS';

DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql = @sql + '
PRINT ''Checking NULL keys in ' + t.name + ''';

SELECT 
    ''' + t.name + ''' AS FactTable,
    ''' + c.name + ''' AS ColumnName,
    COUNT(*) AS NullCount
FROM dw.' + t.name + '
WHERE ' + c.name + ' IS NULL;
'
FROM sys.tables t
JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
JOIN sys.columns c
    ON t.object_id = c.object_id
WHERE s.name = 'dw'
AND t.name LIKE 'fact_%'
AND c.name LIKE '%ID';

EXEC sp_executesql @sql;


-----------------------------------------------------
-- STEP 3: FACT → DIMENSION RELATIONSHIP CHECKS
-----------------------------------------------------

PRINT 'STEP 3: FACT → DIMENSION INTEGRITY CHECKS';

DECLARE @sql2 NVARCHAR(MAX) = '';

SELECT @sql2 = @sql2 + '
PRINT ''Checking FK relationship for ' + t.name + '.' + c.name + ''';

SELECT
    ''' + t.name + ''' AS FactTable,
    ''' + c.name + ''' AS FKColumn,
    COUNT(*) AS InvalidKeyCount
FROM dw.' + t.name + ' f
LEFT JOIN dw.dim_' + REPLACE(c.name,'ID','') + ' d
    ON f.' + c.name + ' = d.' + c.name + '
WHERE d.' + c.name + ' IS NULL;
'
FROM sys.tables t
JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
JOIN sys.columns c
    ON t.object_id = c.object_id
WHERE s.name = 'dw'
AND t.name LIKE 'fact_%'
AND c.name LIKE '%ID';

EXEC sp_executesql @sql2;


-----------------------------------------------------
-- STEP 4: FACT TABLE METRIC SUMMARY
-----------------------------------------------------

PRINT 'STEP 4: SALES METRIC SUMMARY';

IF OBJECT_ID('dw.fact_sales') IS NOT NULL
BEGIN
    SELECT
        COUNT(*) AS TotalTransactions,
        SUM(SalesAmount) AS TotalSales,
        AVG(SalesAmount) AS AvgSale,
        MIN(SalesAmount) AS MinSale,
        MAX(SalesAmount) AS MaxSale
    FROM dw.fact_sales;
END


-----------------------------------------------------
-- STEP 5: DATA DISTRIBUTION CHECK
-----------------------------------------------------

PRINT 'STEP 5: FACT TABLE DISTRIBUTION';

SELECT
    t.name AS FactTable,
    SUM(p.rows) AS RowsCount
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.partitions p
    ON t.object_id = p.object_id
WHERE s.name = 'dw'
AND t.name LIKE 'fact_%'
AND p.index_id IN (0,1)
GROUP BY t.name
ORDER BY RowsCount DESC;


PRINT '--- DW VALIDATION COMPLETE ---';