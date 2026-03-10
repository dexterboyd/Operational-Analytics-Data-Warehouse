/*==============================================================
FULL ETL SCHEMA SETUP & VALIDATION SCRIPT
Purpose: Set up ETL schemas, move tables/views into appropriate layers,
         and validate object counts with logging.

Features of this script:
1. Safe schema creation: Only creates schemas if they don’t exist.
2. Safe object transfer: Checks each table/view exists before moving.
3. Logging: PRINT messages for every action.
4. Cursor-driven transfers for DW dimensions, facts, and reporting views → easy to extend.
5. Compact validation: Single table shows counts of tables and views per schema.
==============================================================*/

-----------------------------------------------------
-- STEP 1: CREATE SCHEMAS
-----------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    EXEC('CREATE SCHEMA staging');
    PRINT 'Schema created: staging';
END
ELSE PRINT 'Schema already exists: staging';

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'clean')
BEGIN
    EXEC('CREATE SCHEMA clean');
    PRINT 'Schema created: clean';
END
ELSE PRINT 'Schema already exists: clean';

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
BEGIN
    EXEC('CREATE SCHEMA dw');
    PRINT 'Schema created: dw';
END
ELSE PRINT 'Schema already exists: dw';

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reporting')
BEGIN
    EXEC('CREATE SCHEMA reporting');
    PRINT 'Schema created: reporting';
END
ELSE PRINT 'Schema already exists: reporting';


-----------------------------------------------------
-- STEP 2: TRANSFER RAW DBO OBJECTS TO STAGING TABLES
-----------------------------------------------------

IF OBJECT_ID('dbo.staging_sales', 'U') IS NOT NULL
BEGIN
    ALTER SCHEMA staging TRANSFER dbo.staging_sales;
    PRINT 'Transferred table to staging: staging_sales';
END

IF OBJECT_ID('dbo.staging_deliveries', 'U') IS NOT NULL
BEGIN
    ALTER SCHEMA staging TRANSFER dbo.staging_deliveries;
    PRINT 'Transferred table to staging: staging_deliveries';
END

IF OBJECT_ID('dbo.staging_routes', 'U') IS NOT NULL
BEGIN
    ALTER SCHEMA staging TRANSFER dbo.staging_routes;
    PRINT 'Transferred table to staging: staging_routes';
END

IF OBJECT_ID('dbo.staging_exceptions', 'U') IS NOT NULL
BEGIN
    ALTER SCHEMA staging TRANSFER dbo.staging_exceptions;
    PRINT 'Transferred table to staging: staging_exceptions';
END


-----------------------------------------------------
-- STEP 6: COMPACT SUMMARY & VALIDATION
-----------------------------------------------------

PRINT '--- ETL SCHEMA OBJECT SUMMARY ---';

SELECT 
    s.name AS SchemaName,
    SUM(CASE WHEN o.type = 'U' THEN 1 ELSE 0 END) AS TableCount,
    SUM(CASE WHEN o.type = 'V' THEN 1 ELSE 0 END) AS ViewCount
FROM sys.schemas s
LEFT JOIN sys.objects o ON o.schema_id = s.schema_id
WHERE s.name IN ('staging','clean','dw','reporting')
GROUP BY s.name
ORDER BY 
    CASE s.name 
        WHEN 'staging' THEN 1
        WHEN 'clean' THEN 2
        WHEN 'dw' THEN 3
        WHEN 'reporting' THEN 4
    END;

PRINT '--- END OF SUMMARY ---';