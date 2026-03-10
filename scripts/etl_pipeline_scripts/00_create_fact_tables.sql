-- =====================================================
-- CREATE AND POPULATE FACT TABLES
-- Data Source: Staging Layer
-- Purpose: Load transactional metrics linked to dimensions
-- =====================================================

-- =====================================================
-- FACT TABLE: SALES
-- Stores transactional sales metrics
-- =====================================================

IF OBJECT_ID('dw.fact_sales','U') IS NULL
BEGIN

CREATE TABLE dw.fact_sales (
    SalesID INT PRIMARY KEY,          -- Natural key from source
    DeliveryID INT,
    ProductKey INT,
    RegionKey INT,
    UnitsSold INT,
    SalesAmount DECIMAL(18,2),
    DateKey INT
);

END

INSERT INTO dw.fact_sales
(
    SalesID,
    DeliveryID,
    ProductKey,
    RegionKey,
    UnitsSold,
    SalesAmount,
    DateKey
)
SELECT
    s.SalesID,
    s.DeliveryID,
    p.ProductKey,
    r.RegionKey,
    s.UnitsSold,
    s.SalesAmount,
    d.DateKey

FROM staging.staging_sales s
LEFT JOIN dw.dim_product p
    ON LTRIM(RTRIM(s.ProductType)) = p.ProductType
LEFT JOIN dw.dim_region r
    ON LTRIM(RTRIM(s.Region)) = r.RegionName
LEFT JOIN dw.dim_date d
    ON s.DateKey = d.FullDate
WHERE s.SalesID IS NOT NULL
AND NOT EXISTS
(
    SELECT 1
    FROM dw.fact_sales f
    WHERE f.SalesID = s.SalesID
);

-- =====================================================
-- FACT TABLE: ROUTE PERFORMANCE
-- Stores route operational metrics
-- =====================================================

IF OBJECT_ID('dw.fact_routes','U') IS NULL
BEGIN

CREATE TABLE dw.fact_routes (
    RouteFactID INT IDENTITY(1,1) PRIMARY KEY,
    RouteKey INT,
    DriverKey INT,
    RegionKey INT,
    PlannedStops INT,
    ActualStops INT,
    PlannedHours DECIMAL(8,2),
    ActualHours DECIMAL(8,2)
);
END

INSERT INTO dw.fact_routes
(
    RouteKey,
    DriverKey,
    RegionKey,
    PlannedStops,
    ActualStops,
    PlannedHours,
    ActualHours
)
SELECT
    dr.RouteKey,
    dv.DriverKey,
    r.RegionKey,
    s.PlannedStops,
    s.ActualStops,
    s.PlannedHours,
    s.ActualHours

FROM staging.staging_routes s
LEFT JOIN dw.dim_route dr
    ON LTRIM(RTRIM(s.RouteID)) = dr.RouteID
LEFT JOIN dw.dim_driver dv
    ON LTRIM(RTRIM(s.DriverID)) = dv.DriverName
LEFT JOIN dw.dim_region r
    ON LTRIM(RTRIM(s.Region)) = r.RegionName
WHERE s.RouteID IS NOT NULL;

-- =====================================================
-- FACT TABLE: DELIVERIES
-- Tracks delivery performance and logistics
-- =====================================================

IF OBJECT_ID('dw.fact_deliveries','U') IS NULL
BEGIN

CREATE TABLE dw.fact_deliveries (
    DeliveryFactID INT IDENTITY(1,1) PRIMARY KEY,
    DeliveryID INT,
    RouteKey INT,
    DriverKey INT,
    RegionKey INT,
    ShipmentTypeKey INT,
    DeliveryDateKey INT,
    ExpectedDeliveryDateKey INT,
    DeliveryStatusKey INT,
    PriorityFlagKey INT
);
END

INSERT INTO dw.fact_deliveries
(
    DeliveryID,
    RouteKey,
    DriverKey,
    RegionKey,
    ShipmentTypeKey,
    DeliveryDateKey,
    ExpectedDeliveryDateKey,
    DeliveryStatusKey,
    PriorityFlagKey
)
SELECT
    d.DeliveryID,
    dr.RouteKey,
    dv.DriverKey,
    r.RegionKey,
    st.ShipmentTypeKey,
    dd.DateKey,
    ed.DateKey,
    ds.DeliveryStatusKey,
    pf.PriorityFlagKey

FROM staging.staging_deliveries d
LEFT JOIN dw.dim_route dr
    ON LTRIM(RTRIM(d.RouteID)) = dr.RouteID
LEFT JOIN dw.dim_driver dv
    ON LTRIM(RTRIM(d.DriverID)) = dv.DriverName
LEFT JOIN dw.dim_region r
    ON LTRIM(RTRIM(d.Region)) = r.RegionName
LEFT JOIN dw.dim_shipment_type st
    ON LTRIM(RTRIM(d.ShipmentType)) = st.ShipmentType
LEFT JOIN dw.dim_date dd
    ON d.DeliveryDate = dd.FullDate
LEFT JOIN dw.dim_date ed
    ON d.ExpectedDeliveryDate = ed.FullDate
LEFT JOIN dw.dim_delivery_status ds
    ON LTRIM(RTRIM(d.DeliveryStatus)) = ds.DeliveryStatus
LEFT JOIN dw.dim_priority_flag pf
    ON d.PriorityFlag = pf.PriorityFlag
WHERE d.DeliveryID IS NOT NULL;

-- =====================================================
-- FACT TABLE: EXCEPTIONS
-- Tracks operational issues affecting deliveries
-- =====================================================

IF OBJECT_ID('dw.fact_exceptions','U') IS NULL
BEGIN

CREATE TABLE dw.fact_exceptions (
    ExceptionFactID INT IDENTITY(1,1) PRIMARY KEY,
    ExceptionID INT,
    DeliveryID INT,
    RegionKey INT,
    ExceptionTypeKey INT,
    DateReportedKey INT,
    ResolvedDateKey INT,
    ResolutionTimeHours DECIMAL(8,2),
    PriorityFlagKey INT
);
END

INSERT INTO dw.fact_exceptions
(
    ExceptionID,
    DeliveryID,
    RegionKey,
    ExceptionTypeKey,
    DateReportedKey,
    ResolvedDateKey,
    ResolutionTimeHours,
    PriorityFlagKey
)
SELECT
    e.ExceptionID,
    e.DeliveryID,
    r.RegionKey,
    et.ExceptionTypeKey,
    dr.DateKey,
    rr.DateKey,
    e.ResolutionTimeHours,
    pf.PriorityFlagKey

FROM staging.staging_exceptions e
LEFT JOIN dw.dim_region r
    ON LTRIM(RTRIM(e.Region)) = r.RegionName
LEFT JOIN dw.dim_exception_type et
    ON LTRIM(RTRIM(e.ExceptionType)) = et.ExceptionType
LEFT JOIN dw.dim_date dr
    ON e.DateReported = dr.FullDate
LEFT JOIN dw.dim_date rr
    ON e.ResolvedDate = rr.FullDate
LEFT JOIN dw.dim_priority_flag pf
    ON e.PriorityFlag = pf.PriorityFlag
WHERE e.ExceptionID IS NOT NULL;

/*
-- =========================================
--  CREATE FACT TABLES
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



-----------------------------------------------------
-- CREATE AND POPULATE FACT TABLES
-----------------------------------------------------

-- Create fact_sales table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_sales' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.fact_sales (
        SalesID INT PRIMARY KEY,
        DeliveryID INT,
        ProductTypeKey INT,
        RegionKey INT,
        UnitsSold INT,
        SalesAmount DECIMAL(18,2),
        DateKey INT
    );
END

-- Populate fact_sales
INSERT INTO dw.fact_sales (SalesID, DeliveryID, ProductTypeKey, RegionKey, UnitsSold, SalesAmount, DateKey)
SELECT 
    s.SalesID,
    s.DeliveryID,
    pt.ProductTypeKey,
    r.RegionKey,
    s.UnitsSold,
    s.SalesAmount,
    d.DateKey
FROM staging.staging_sales s
LEFT JOIN dw.dim_product_type pt
    ON LTRIM(RTRIM(s.ProductType)) = pt.ProductType
LEFT JOIN dw.dim_region r
    ON LTRIM(RTRIM(s.Region)) = r.Region
LEFT JOIN dw.dim_date d
    ON s.DateKey = d.DateKey
WHERE s.SalesID IS NOT NULL;
-------------------------------------------
-- Create fact_routes table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_routes' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.fact_routes (
        RouteFactID INT IDENTITY(1,1) PRIMARY KEY,
        RouteKey INT,
        DriverKey INT,
        RegionKey INT,
        PlannedStops INT,
        ActualStops INT,
        PlannedHours DECIMAL(8,2),
        ActualHours DECIMAL(8,2)
    );
END

-- Populate fact_routes
INSERT INTO dw.fact_routes (RouteKey, DriverKey, RegionKey, PlannedStops, ActualStops, PlannedHours, ActualHours)
SELECT 
    dr.RouteKey,
    dv.DriverKey,
    r.RegionKey,
    s.PlannedStops,
    s.ActualStops,
    s.PlannedHours,
    s.ActualHours
FROM staging.staging_routes s
LEFT JOIN dw.dim_route dr
    ON LTRIM(RTRIM(s.RouteID)) = dr.RouteID
LEFT JOIN dw.dim_driver dv
    ON s.DriverID = dv.DriverID
LEFT JOIN dw.dim_region r
    ON LTRIM(RTRIM(s.Region)) = r.Region
WHERE s.RouteID IS NOT NULL;
-------------------------------------------------
-- Create fact_deliveries
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_deliveries' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.fact_deliveries (
        DeliveryFactID INT IDENTITY(1,1) PRIMARY KEY,
        DeliveryID INT,
        RouteKey INT,
        DriverKey INT,
        RegionKey INT,
        ShipmentTypeKey INT,
        DeliveryDateKey INT,
        ExpectedDeliveryDateKey INT,
        DeliveryStatusKey INT,
        PriorityFlagKey INT
    );
END

-- Populate fact_deliveries
INSERT INTO dw.fact_deliveries (
    DeliveryID, RouteKey, DriverKey, RegionKey, ShipmentTypeKey, 
    DeliveryDateKey, ExpectedDeliveryDateKey, DeliveryStatusKey, PriorityFlagKey
)
SELECT 
    d.DeliveryID,
    dr.RouteKey,
    dv.DriverKey,
    r.RegionKey,
    st.ShipmentTypeKey,
    dd.DateKey AS DeliveryDateKey,
    ed.DateKey AS ExpectedDeliveryDateKey,
    ds.DeliveryStatusKey,
    pf.PriorityFlagKey
FROM staging.staging_deliveries d
LEFT JOIN dw.dim_route dr
    ON LTRIM(RTRIM(d.RouteID)) = dr.RouteID
LEFT JOIN dw.dim_driver dv
    ON d.DriverID = dv.DriverID
LEFT JOIN dw.dim_region r
    ON LTRIM(RTRIM(d.Region)) = r.Region
LEFT JOIN dw.dim_shipment_type st
    ON LTRIM(RTRIM(d.ShipmentType)) = st.ShipmentType
LEFT JOIN dw.dim_date dd
    ON d.DeliveryDate = dd.FullDate
LEFT JOIN dw.dim_date ed
    ON d.ExpectedDeliveryDate = ed.FullDate
LEFT JOIN dw.dim_delivery_status ds
    ON LTRIM(RTRIM(d.DeliveryStatus)) = ds.DeliveryStatus
LEFT JOIN dw.dim_priority_flag pf
    ON LTRIM(RTRIM(d.PriorityFlag)) = pf.PriorityFlag
WHERE d.DeliveryID IS NOT NULL;
-----------------------------------------------
-- Create fact_exceptions table if not exists
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_exceptions' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.fact_exceptions (
        ExceptionFactID INT IDENTITY(1,1) PRIMARY KEY,
        ExceptionID INT,
        DeliveryID INT,
        RegionKey INT,
        ExceptionTypeKey INT,
        DateReportedKey INT,
        ResolvedDateKey INT,
        ResolutionTimeHours DECIMAL(8,2),
        PriorityFlagKey INT
    );
END

-- Populate fact_exceptions
INSERT INTO dw.fact_exceptions (
    ExceptionID, DeliveryID, RegionKey, ExceptionTypeKey, DateReportedKey, 
    ResolvedDateKey, ResolutionTimeHours, PriorityFlagKey
)
SELECT
    e.ExceptionID,
    e.DeliveryID,
    r.RegionKey,
    et.ExceptionTypeKey,
    dr.DateKey AS DateReportedKey,
    rr.DateKey AS ResolvedDateKey,
    e.ResolutionTimeHours,
    pf.PriorityFlagKey
FROM staging.staging_exceptions e
LEFT JOIN dw.dim_region r
    ON LTRIM(RTRIM(e.Region)) = r.Region
LEFT JOIN dw.dim_exception_type et
    ON LTRIM(RTRIM(e.ExceptionType)) = et.ExceptionType
LEFT JOIN dw.dim_date dr
    ON e.DateReported = dr.FullDate
LEFT JOIN dw.dim_date rr
    ON e.ResolvedDate = rr.FullDate
LEFT JOIN dw.dim_priority_flag pf
    ON LTRIM(RTRIM(e.PriorityFlag)) = pf.PriorityFlag
WHERE e.ExceptionID IS NOT NULL;

*/