-----------------------------------------------------
-- STEP 1: CREATE SCHEMAS
-- Create all necessary schemas for ETL layers
-----------------------------------------------------

-- Staging layer: raw data from source systems
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');

-- Clean layer: standardized, validated, cleansed data
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'clean')
    EXEC('CREATE SCHEMA clean');

-- Data Warehouse layer: fact and dimension tables for analytics
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');

-- Reporting layer: views or tables for BI tools
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reporting')
    EXEC('CREATE SCHEMA reporting');

-----------------------------------------------------
-- STEP 2: TRANSFER STAGING TABLES
-- Move raw tables into the staging schema if they exist
-----------------------------------------------------

IF OBJECT_ID('dbo.staging_sales', 'U') IS NOT NULL
    ALTER SCHEMA staging TRANSFER dbo.staging_sales;

IF OBJECT_ID('dbo.staging_deliveries', 'U') IS NOT NULL
    ALTER SCHEMA staging TRANSFER dbo.staging_deliveries;

IF OBJECT_ID('dbo.staging_routes', 'U') IS NOT NULL
    ALTER SCHEMA staging TRANSFER dbo.staging_routes;

IF OBJECT_ID('dbo.staging_exceptions', 'U') IS NOT NULL
    ALTER SCHEMA staging TRANSFER dbo.staging_exceptions;

-----------------------------------------------------
-- STEP 3: TRANSFER DW DIMENSION TABLES
-----------------------------------------------------

IF OBJECT_ID('dbo.dim_shipment_type', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.dim_shipment_type;

IF OBJECT_ID('dbo.dim_route', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.dim_route;

IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.dim_date;

IF OBJECT_ID('dbo.dim_region', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.dim_region;

IF OBJECT_ID('dbo.dim_product', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.dim_product;

IF OBJECT_ID('dbo.dim_priority_flag', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.dim_priority_flag;

IF OBJECT_ID('dbo.dim_exception_type', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.dim_exception_type;

IF OBJECT_ID('dbo.dim_driver', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.dim_driver;

-----------------------------------------------------
-- STEP 4: TRANSFER DW FACT TABLES
-----------------------------------------------------

IF OBJECT_ID('dbo.fact_sales', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.fact_sales;

IF OBJECT_ID('dbo.fact_routes', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.fact_routes;

IF OBJECT_ID('dbo.fact_exceptions', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.fact_exceptions;

IF OBJECT_ID('dbo.fact_deliveries', 'U') IS NOT NULL
    ALTER SCHEMA dw TRANSFER dbo.fact_deliveries;

-----------------------------------------------------
-- STEP 5: TRANSFER REPORTING OBJECTS
-- Views or reporting tables that reference DW layer
-----------------------------------------------------

-- Example reporting views
IF OBJECT_ID('dbo.vw_sales_summary', 'V') IS NOT NULL
    ALTER SCHEMA reporting TRANSFER dbo.vw_sales_summary;

IF OBJECT_ID('dbo.vw_delivery_metrics', 'V') IS NOT NULL
    ALTER SCHEMA reporting TRANSFER dbo.vw_delivery_metrics;