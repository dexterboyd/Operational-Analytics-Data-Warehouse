-- =========================================
-- CREATE STAGING SCHEMA (IF NOT EXISTS)
-- =========================================
-- The staging schema stores raw imported data from source systems
-- with minimal transformation. Data types are aligned but business
-- rules are not yet applied.

IF SCHEMA_ID('staging') IS NULL
BEGIN
    EXEC('CREATE SCHEMA staging');
    PRINT 'Schema created: staging';
END
ELSE
BEGIN
    PRINT 'Schema already exists: staging';
END
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

/*
-- =========================================
--  CREATE STAGING TABLES
-- =========================================

CREATE TABLE staging_deliveries (
    DeliveryID INT PRIMARY KEY,
    RouteID NVARCHAR(10),
    DriverID NVARCHAR(50),
    Region NVARCHAR(10),
    ShipmentType NVARCHAR(20),
    DeliveryDate DATE,
    ExpectedDeliveryDate DATE NULL,
    DeliveryStatus NVARCHAR(20),
    PriorityFlag BIT
);

CREATE TABLE staging_exceptions (
    ExceptionID INT PRIMARY KEY,
    DeliveryID INT,
    ExceptionType NVARCHAR(50),
    DateReported DATE,
    ResolvedDate DATE NULL,
    ResolutionTimeHours INT,
    PriorityFlag BIT,
    Region NVARCHAR(10)
);

CREATE TABLE staging_routes (
    RouteID NVARCHAR(10),
    DriverID NVARCHAR(50),
    PlannedStops INT,
    ActualStops INT,
    PlannedHours DECIMAL(5,2),
    ActualHours DECIMAL(5,2),
    Region NVARCHAR(10)
);

CREATE TABLE staging_sales (
    SalesID INT PRIMARY KEY,
    DeliveryID INT,
    DateKey DATE,
    ProductType NVARCHAR(50),
    Region NVARCHAR(10),
    UnitsSold INT,
    SalesAmount DECIMAL(10,2)
);
*/
