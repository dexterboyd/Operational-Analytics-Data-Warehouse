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
6. ETL layer order maintained: Staging → Clean → DW → Reporting.
7. Optional staging → clean validation: Row counts and NULL checks for key columns (can be enabled/disabled via flag).
==============================================================*/

-----------------------------------------------------
-- CONFIGURATION: ENABLE/DISABLE STAGING VALIDATION
-----------------------------------------------------
DECLARE @EnableStagingValidation BIT = 1; -- Set to 1 to run validation, 0 to skip

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
-- STEP 2: TRANSFER DBO Objects TO STAGING TABLES
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
-- OPTIONAL: STAGING → CLEAN VALIDATION
-- PRE-CLEANING VALIDATION
-- Controlled by @EnableStagingValidation
-----------------------------------------------------

IF @EnableStagingValidation = 1
BEGIN
    PRINT '--- STAGING LAYER VALIDATION ---';

    -- Row counts
    SELECT 'staging_sales' AS TableName, COUNT(*) AS [RowCount]
	FROM staging.staging_sales
    UNION ALL
    SELECT 'staging_deliveries', COUNT(*) FROM staging.staging_deliveries
    UNION ALL
    SELECT 'staging_routes', COUNT(*) FROM staging.staging_routes
    UNION ALL
    SELECT 'staging_exceptions', COUNT(*) FROM staging.staging_exceptions;

    -- Key NULL checks (example for critical columns)
    SELECT 'staging_sales' AS TableName,
           SUM(CASE WHEN SalesID IS NULL THEN 1 ELSE 0 END) AS NullSalesID,
           SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID,
           SUM(CASE WHEN UnitsSold IS NULL THEN 1 ELSE 0 END) AS NullUnitsSold,
           SUM(CASE WHEN SalesAmount IS NULL THEN 1 ELSE 0 END) AS NullSalesAmount
    FROM staging.staging_sales;

    SELECT 'staging_deliveries' AS TableName,
           SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID,
           SUM(CASE WHEN RouteID IS NULL THEN 1 ELSE 0 END) AS NullRouteID,
           SUM(CASE WHEN DriverID IS NULL THEN 1 ELSE 0 END) AS NullDriverID
    FROM staging.staging_deliveries;

    SELECT 'staging_routes' AS TableName,
           SUM(CASE WHEN RouteID IS NULL THEN 1 ELSE 0 END) AS NullRouteID,
           SUM(CASE WHEN DriverID IS NULL THEN 1 ELSE 0 END) AS NullDriverID
    FROM staging.staging_routes;

    SELECT 'staging_exceptions' AS TableName,
           SUM(CASE WHEN ExceptionID IS NULL THEN 1 ELSE 0 END) AS NullExceptionID,
           SUM(CASE WHEN DeliveryID IS NULL THEN 1 ELSE 0 END) AS NullDeliveryID
    FROM staging.staging_exceptions;
END


-----------------------------------------------------
-- STEP 3: TRANSFER DW DIMENSION TABLES
-- (moving pre-existing DW tables into the dw schema)
-----------------------------------------------------

DECLARE @dimTables TABLE (Name NVARCHAR(128));
INSERT INTO @dimTables VALUES
('dim_shipment_type'), ('dim_route'), ('dim_date'), ('dim_region'), 
('dim_product'), ('dim_priority_flag'), ('dim_exception_type'), ('dim_driver');

DECLARE @t NVARCHAR(128);
DECLARE dim_cursor CURSOR FOR SELECT Name FROM @dimTables;
OPEN dim_cursor;
FETCH NEXT FROM dim_cursor INTO @t;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @t, 'U') IS NOT NULL
    BEGIN
        EXEC('ALTER SCHEMA dw TRANSFER dbo.' + @t);
        PRINT 'Transferred DW dimension table: ' + @t;
    END
    FETCH NEXT FROM dim_cursor INTO @t;
END
CLOSE dim_cursor;
DEALLOCATE dim_cursor;


-----------------------------------------------------
-- STEP 4: TRANSFER DW FACT TABLES
-- (moving pre-existing DW tables into the dw schema)
-----------------------------------------------------

DECLARE @factTables TABLE (Name NVARCHAR(128));
INSERT INTO @factTables VALUES
('fact_sales'), ('fact_routes'), ('fact_exceptions'), ('fact_deliveries');

DECLARE fact_cursor CURSOR FOR SELECT Name FROM @factTables;
OPEN fact_cursor;
FETCH NEXT FROM fact_cursor INTO @t;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @t, 'U') IS NOT NULL
    BEGIN
        EXEC('ALTER SCHEMA dw TRANSFER dbo.' + @t);
        PRINT 'Transferred DW fact table: ' + @t;
    END
    FETCH NEXT FROM fact_cursor INTO @t;
END
CLOSE fact_cursor;
DEALLOCATE fact_cursor;


-----------------------------------------------------
-- STEP 5: TRANSFER REPORTING OBJECTS
-----------------------------------------------------

DECLARE @reportViews TABLE (Name NVARCHAR(128));
INSERT INTO @reportViews VALUES
('vw_sales_summary'), ('vw_delivery_metrics');  -- Replace with your reporting views

DECLARE report_cursor CURSOR FOR SELECT Name FROM @reportViews;
OPEN report_cursor;
FETCH NEXT FROM report_cursor INTO @t;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @t, 'V') IS NOT NULL
    BEGIN
        EXEC('ALTER SCHEMA reporting TRANSFER dbo.' + @t);
        PRINT 'Transferred reporting view: ' + @t;
    END
    FETCH NEXT FROM report_cursor INTO @t;
END
CLOSE report_cursor;
DEALLOCATE report_cursor;


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