-----------------------------------------------------
-- CREATE REQUIRED ETL SCHEMAS
-- Ensures schemas used in the warehouse pipeline exist
--
-- Pipeline Layers:
-- staging   → raw imported source data
-- clean     → standardized transformation views
-- dw        → star schema (fact & dimension tables)
-- reporting → aggregated BI views
-----------------------------------------------------

DECLARE @Schemas TABLE (SchemaName NVARCHAR(100))

INSERT INTO @Schemas
VALUES
('staging'),
('clean'),
('dw'),
('reporting')

DECLARE @SQL NVARCHAR(MAX) = ''

-- =========================================
-- CREATE STAGING SCHEMA (IF NOT EXISTS)
-- =========================================
-- The staging schema stores raw imported data from source systems
-- with minimal transformation. Data types are aligned but business
-- rules are not yet applied.

SELECT @SQL = @SQL + '
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ''' + SchemaName + ''')
BEGIN
    EXEC(''CREATE SCHEMA ' + SchemaName + ''')
    PRINT ''Schema created: ' + SchemaName + '''
END
ELSE
    PRINT ''Schema already exists: ' + SchemaName + '''
'
FROM @Schemas
EXEC(@SQL)
GO

-- =========================================
-- STAGING TABLE: DELIVERIES
-- =========================================
-- Raw delivery data imported from source files.
-- Contains route, driver, delivery timing, and status information.

IF OBJECT_ID('staging.staging_deliveries', 'U') IS NULL
BEGIN
CREATE TABLE staging.staging_deliveries (
    DeliveryID INT PRIMARY KEY,          -- Unique identifier for each delivery
    RouteID NVARCHAR(10),                -- Route assigned to delivery
    DriverID NVARCHAR(50),               -- Driver responsible for delivery
    Region NVARCHAR(10),                 -- Geographic delivery region
    ShipmentType NVARCHAR(20),           -- Shipment classification (Standard, Express, etc.)
    DeliveryDate DATE,                   -- Actual delivery date
    ExpectedDeliveryDate DATE NULL,      -- Planned delivery date
    DeliveryStatus NVARCHAR(20),         -- Status (Delivered, Delayed, Failed)
    PriorityFlag BIT                     -- Indicates high priority shipment
);
END
GO

-- =========================================
-- STAGING TABLE: DELIVERY EXCEPTIONS
-- =========================================
-- Captures operational issues affecting deliveries
-- such as delays, damages, or route problems.

IF OBJECT_ID('staging.staging_exceptions', 'U') IS NULL
BEGIN
CREATE TABLE staging.staging_exceptions (
    ExceptionID INT PRIMARY KEY,         -- Unique exception record identifier
    DeliveryID INT,                      -- Related delivery
    ExceptionType NVARCHAR(50),          -- Type of exception (Delay, Damage, Weather)
    DateReported DATE,                   -- Date the issue was reported
    ResolvedDate DATE NULL,              -- Date issue was resolved
    ResolutionTimeHours INT,             -- Hours taken to resolve issue
    PriorityFlag BIT,                    -- Indicates critical exception
    Region NVARCHAR(10)                  -- Region where exception occurred
);
END
GO

-- =========================================
-- STAGING TABLE: ROUTES
-- =========================================
-- Contains planned vs actual route performance
-- metrics for delivery drivers.

IF OBJECT_ID('staging.staging_routes', 'U') IS NULL
BEGIN
CREATE TABLE staging.staging_routes (
    RouteID NVARCHAR(10),                -- Route identifier
    DriverID NVARCHAR(50),               -- Driver assigned to route
    PlannedStops INT,                    -- Planned number of stops
    ActualStops INT,                     -- Actual number of stops completed
    PlannedHours DECIMAL(5,2),           -- Estimated route duration
    ActualHours DECIMAL(5,2),            -- Actual route duration
    Region NVARCHAR(10)                  -- Operating region
);
END
GO

-- =========================================
-- STAGING TABLE: SALES
-- =========================================
-- Raw sales transactions tied to deliveries.
-- Used for building sales fact tables in the DW layer.

IF OBJECT_ID('staging.staging_sales', 'U') IS NULL
BEGIN
CREATE TABLE staging.staging_sales (
    SalesID INT PRIMARY KEY,             -- Unique sales transaction ID
    DeliveryID INT,                      -- Delivery associated with the sale
    DateKey DATE,                        -- Sales transaction date
    ProductType NVARCHAR(50),            -- Product category sold
    Region NVARCHAR(10),                 -- Sales region
    UnitsSold INT,                       -- Number of units sold
    SalesAmount DECIMAL(10,2)            -- Total revenue for the transaction
);
END
GO

-----------------------------------------------------
-- BULK INSERT DATA INTO STAGING TABLES
-----------------------------------------------------
TRUNCATE TABLE staging.staging_sales;
BULK INSERT staging.staging_sales
FROM 'C:\Operational-Analytics-Data-Warehouse\datasets\raw\sales.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO

TRUNCATE TABLE staging.staging_deliveries;
BULK INSERT staging.staging_deliveries
FROM 'C:\Operational-Analytics-Data-Warehouse\datasets\raw\deliveries.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO

TRUNCATE TABLE staging.staging_routes;
BULK INSERT staging.staging_routes
FROM 'C:\Operational-Analytics-Data-Warehouse\datasets\raw\routes.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO

TRUNCATE TABLE staging.staging_exceptions;
BULK INSERT staging.staging_exceptions
FROM 'C:\Operational-Analytics-Data-Warehouse\datasets\raw\exceptions.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO

PRINT 'CSV data loaded into staging tables';
GO

-----------------------------------------------------
-- Option 2: TRANSFER RAW IMPORTED TABLES INTO STAGING
-- Many raw imports land in dbo by default.
-- This section moves them into the staging schema.
-----------------------------------------------------
-- DECLARE @Tables TABLE (TableName NVARCHAR(100))

-- INSERT INTO @Tables
-- VALUES
-- ('staging_sales'),
-- ('staging_deliveries'),
-- ('staging_routes'),
-- ('staging_exceptions')

-- DECLARE @TransferSQL NVARCHAR(MAX) = ''

-- SELECT @TransferSQL = @TransferSQL + '
-- IF OBJECT_ID(''dbo.' + TableName + ''',''U'') IS NOT NULL
-- BEGIN
--     ALTER SCHEMA staging TRANSFER dbo.' + TableName + '
--     PRINT ''Transferred table to staging: ' + TableName + '''
-- END
-- '
-- FROM @Tables
-- EXEC(@TransferSQL)
-- GO
-----------------------------------------------------

-----------------------------------------------------
-- ETL SCHEMA OBJECT SUMMARY
-- Quick validation report showing tables & views 
-- within each warehouse layer
-----------------------------------------------------

PRINT '--- ETL SCHEMA OBJECT SUMMARY ---'

SELECT 
    s.name AS SchemaName,
    -- Count user tables
    SUM(CASE WHEN o.type = 'U' THEN 1 ELSE 0 END) AS TableCount,
    -- Count views
    SUM(CASE WHEN o.type = 'V' THEN 1 ELSE 0 END) AS ViewCount
FROM sys.schemas s
LEFT JOIN sys.objects o
    ON o.schema_id = s.schema_id
WHERE s.name IN ('staging','clean','dw','reporting')
GROUP BY s.name
ORDER BY 
    CASE s.name
        WHEN 'staging' THEN 1
        WHEN 'clean' THEN 2
        WHEN 'dw' THEN 3
        WHEN 'reporting' THEN 4
    END
PRINT '--- END OF SUMMARY ---'
GO
