
-- =========================================
--  STAGING TABLES
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

-- =========================================
--  DIMENSION TABLES
-- =========================================

CREATE TABLE dim_date (
    DateKey INT PRIMARY KEY,
    FullDate DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    Weekday INT,
    IsWeekend BIT
);

CREATE TABLE dim_region (
    RegionKey INT PRIMARY KEY,
    RegionName NVARCHAR(10)
);

CREATE TABLE dim_driver (
    DriverKey INT PRIMARY KEY,
    DriverName NVARCHAR(50)
);

CREATE TABLE dim_route (
    RouteKey INT PRIMARY KEY,
    RouteID NVARCHAR(10)
);

CREATE TABLE dim_shipment_type (
    ShipmentTypeKey INT PRIMARY KEY,
    ShipmentType NVARCHAR(20)
);

CREATE TABLE dim_exception_type (
    ExceptionTypeKey INT PRIMARY KEY,
    ExceptionType NVARCHAR(50)
);

CREATE TABLE dim_product (
    ProductKey INT PRIMARY KEY,
    ProductType NVARCHAR(50)
);

CREATE TABLE dim_priority_flag (
    PriorityFlagKey INT PRIMARY KEY,
    PriorityFlag BIT
);

-- =========================================
--  FACT TABLES
-- =========================================

CREATE TABLE fact_deliveries (
    FactDeliveryID INT IDENTITY(1,1) PRIMARY KEY,
    DeliveryID INT,
    DateKey INT,
    RouteKey INT,
    DriverKey INT,
    RegionKey INT,
    ShipmentTypeKey INT,
    PriorityFlagKey INT,
    DeliveryStatus NVARCHAR(20),
    DeliveryDelayMinutes INT,
    OnTimeFlag BIT,
    FOREIGN KEY(DateKey) REFERENCES dim_date(DateKey),
    FOREIGN KEY(RouteKey) REFERENCES dim_route(RouteKey),
    FOREIGN KEY(DriverKey) REFERENCES dim_driver(DriverKey),
    FOREIGN KEY(RegionKey) REFERENCES dim_region(RegionKey),
    FOREIGN KEY(ShipmentTypeKey) REFERENCES dim_shipment_type(ShipmentTypeKey),
    FOREIGN KEY(PriorityFlagKey) REFERENCES dim_priority_flag(PriorityFlagKey)
);

CREATE TABLE fact_exceptions (
    FactExceptionID INT IDENTITY(1,1) PRIMARY KEY,
    ExceptionID INT,
    DeliveryID INT,
    DateReportedKey INT,
    ResolvedDateKey INT NULL,
    RegionKey INT,
    ExceptionTypeKey INT,
    PriorityFlagKey INT,
    ResolutionTimeHours INT,
    ResolvedFlag BIT,
    FOREIGN KEY(DateReportedKey) REFERENCES dim_date(DateKey),
    FOREIGN KEY(ResolvedDateKey) REFERENCES dim_date(DateKey),
    FOREIGN KEY(RegionKey) REFERENCES dim_region(RegionKey),
    FOREIGN KEY(ExceptionTypeKey) REFERENCES dim_exception_type(ExceptionTypeKey),
    FOREIGN KEY(PriorityFlagKey) REFERENCES dim_priority_flag(PriorityFlagKey)
);

CREATE TABLE fact_sales (
    FactSalesID INT IDENTITY(1,1) PRIMARY KEY,
    SalesID INT,
    DeliveryID INT,
    DateKey INT,
    ProductKey INT,
    RegionKey INT,
    UnitsSold INT,
    SalesAmount DECIMAL(10,2),
    FOREIGN KEY(DateKey) REFERENCES dim_date(DateKey),
    FOREIGN KEY(ProductKey) REFERENCES dim_product(ProductKey),
    FOREIGN KEY(RegionKey) REFERENCES dim_region(RegionKey)
);

CREATE TABLE fact_routes (
    FactRouteID INT IDENTITY(1,1) PRIMARY KEY,
    RouteKey INT,
    DriverKey INT,
    RegionKey INT,
    DateKey INT,
    PlannedStops INT,
    ActualStops INT,
    PlannedHours DECIMAL(5,2),
    ActualHours DECIMAL(5,2),
    StopsVariance AS (ActualStops - PlannedStops),
    HoursVariance AS (ActualHours - PlannedHours),
    OnTimeFlag AS (CASE WHEN ActualHours <= PlannedHours THEN 1 ELSE 0 END),
    FOREIGN KEY(RouteKey) REFERENCES dim_route(RouteKey),
    FOREIGN KEY(DriverKey) REFERENCES dim_driver(DriverKey),
    FOREIGN KEY(RegionKey) REFERENCES dim_region(RegionKey),
    FOREIGN KEY(DateKey) REFERENCES dim_date(DateKey)
);


-- Example for staging_deliveries
BULK INSERT staging_deliveries
FROM 'C:\Users\DBoyd\Documents\CSVs\staging_deliveries.csv'
WITH (
    FIRSTROW = 2,               -- skip header
    FIELDTERMINATOR = ',',       -- comma-separated
    ROWTERMINATOR = '\n',        -- line break
    TABLOCK
);

SELECT TOP 10 * FROM staging_deliveries;