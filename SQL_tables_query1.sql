create database Fedex_Ops_DB_Prod

-- =============================================
-- DIMENSION TABLES
-- =============================================

-- dim_date
CREATE TABLE dim_date (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Year INT,
    Quarter INT,
    Month INT,
    MonthName NVARCHAR(20),
    Week INT,
    DayOfWeek NVARCHAR(20)
);

-- dim_region
CREATE TABLE dim_region (
    RegionKey INT PRIMARY KEY,
    RegionName NVARCHAR(50),
    State NVARCHAR(50)
);

-- dim_route
CREATE TABLE dim_route (
    RouteKey INT PRIMARY KEY,
    RouteID NVARCHAR(10),
    PlannedStops INT,
    PlannedHours INT,
    RegionKey INT FOREIGN KEY REFERENCES dim_region(RegionKey)
);

-- dim_driver
CREATE TABLE dim_driver (
    DriverKey INT PRIMARY KEY,
    DriverID NVARCHAR(10),
    ExperienceLevel NVARCHAR(10),
    HireDate DATE
);

-- dim_shipment_type
CREATE TABLE dim_shipment_type (
    ShipmentTypeKey INT PRIMARY KEY,
    ShipmentType NVARCHAR(20),
    SLA_Category NVARCHAR(10)
);

-- dim_exception_type
CREATE TABLE dim_exception_type (
    ExceptionTypeKey INT PRIMARY KEY,
    ExceptionType NVARCHAR(50),
    SeverityLevel NVARCHAR(10)
);

-- dim_product
CREATE TABLE dim_product (
    ProductKey INT PRIMARY KEY,
    ProductName NVARCHAR(50),
    Category NVARCHAR(20),
    BasePrice DECIMAL(10,2)
);

-- =============================================
-- FACT TABLES
-- =============================================

-- fact_deliveries
CREATE TABLE fact_deliveries (
    DeliveryID INT PRIMARY KEY,
    DateKey INT FOREIGN KEY REFERENCES dim_date(DateKey),
    RouteKey INT FOREIGN KEY REFERENCES dim_route(RouteKey),
    DriverKey INT FOREIGN KEY REFERENCES dim_driver(DriverKey),
    RegionKey INT FOREIGN KEY REFERENCES dim_region(RegionKey),
    ShipmentTypeKey INT FOREIGN KEY REFERENCES dim_shipment_type(ShipmentTypeKey),
    PriorityFlag CHAR(1),
    SLA_Minutes INT,
    DeliveryMinutes INT,
    DeliveryStatus NVARCHAR(20)
);

-- fact_exceptions
CREATE TABLE fact_exceptions (
    ExceptionID INT PRIMARY KEY,
    DeliveryID INT FOREIGN KEY REFERENCES fact_deliveries(DeliveryID),
    DateKey INT FOREIGN KEY REFERENCES dim_date(DateKey),
    ExceptionTypeKey INT FOREIGN KEY REFERENCES dim_exception_type(ExceptionTypeKey),
    ResolutionMinutes INT,
    ResolvedFlag CHAR(1),
    PriorityFlag CHAR(1)
);

-- fact_sales
CREATE TABLE fact_sales (
    SalesID INT PRIMARY KEY,
    DateKey INT FOREIGN KEY REFERENCES dim_date(DateKey),
    ProductKey INT FOREIGN KEY REFERENCES dim_product(ProductKey),
    RegionKey INT FOREIGN KEY REFERENCES dim_region(RegionKey),
    DeliveryID INT FOREIGN KEY REFERENCES fact_deliveries(DeliveryID),
    UnitsSold INT,
    SalesAmount DECIMAL(12,2)
);

-- =============================================
-- POPULATE DIMENSION TABLES
-- =============================================

-- dim_date (2023-01-01 to 2024-12-31)
WITH DateSeq AS (
    SELECT CAST('2023-01-01' AS DATE) AS DateValue
    UNION ALL
    SELECT DATEADD(DAY,1,DateValue)
    FROM DateSeq
    WHERE DateValue < '2024-12-31'
)
INSERT INTO dim_date(DateKey, Date, Year, Quarter, Month, MonthName, Week, DayOfWeek)
SELECT
    CAST(FORMAT(DateValue,'yyyyMMdd') AS INT),
    DateValue,
    YEAR(DateValue),
    DATEPART(QUARTER, DateValue),
    MONTH(DateValue),
    DATENAME(MONTH, DateValue),
    DATEPART(ISO_WEEK, DateValue),
    DATENAME(WEEKDAY, DateValue)
FROM DateSeq
OPTION (MAXRECURSION 0);

-- dim_region
INSERT INTO dim_region(RegionKey, RegionName, State)
VALUES
(1,'South','MS/AL/LA'),
(2,'Midwest','IL/OH/MI'),
(3,'Northeast','NY/NJ/PA'),
(4,'West','CA/WA/OR');

-- dim_route (12 routes)
INSERT INTO dim_route(RouteKey, RouteID, PlannedStops, PlannedHours, RegionKey)
SELECT
    v.Number,
    'R-' + RIGHT('00' + CAST(v.Number AS VARCHAR(2)),2),
    80 + ABS(CHECKSUM(NEWID()) % 80), -- PlannedStops
    7 + ABS(CHECKSUM(NEWID()) % 4),  -- PlannedHours
    1 + ABS(CHECKSUM(NEWID()) % 4)   -- RegionKey
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)) v(Number);

-- dim_driver (20 drivers)
INSERT INTO dim_driver(DriverKey, DriverID, ExperienceLevel, HireDate)
SELECT
    v.Number,
    'D-' + RIGHT('000' + CAST(v.Number AS VARCHAR(3)),3),
    CASE WHEN r <= 30 THEN 'Junior' WHEN r <= 40 THEN 'Mid' ELSE 'Senior' END,
    DATEADD(DAY, ABS(CHECKSUM(NEWID()) % 2000), '2018-01-01')
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20)) v(Number)
CROSS APPLY (SELECT ABS(CHECKSUM(NEWID()) % 100) AS r) t;

-- dim_shipment_type
INSERT INTO dim_shipment_type(ShipmentTypeKey, ShipmentType, SLA_Category)
VALUES
(1,'Standard','48hr'),
(2,'Express','24hr'),
(3,'Overnight','12hr');

-- dim_exception_type
INSERT INTO dim_exception_type(ExceptionTypeKey, ExceptionType, SeverityLevel)
VALUES
(1,'Weather Delay','Medium'),
(2,'Address Issue','Low'),
(3,'Vehicle Breakdown','High'),
(4,'Customer Unavailable','Low');

-- dim_product
INSERT INTO dim_product(ProductKey, ProductName, Category, BasePrice)
VALUES
(1,'Small Parcel','Parcel',15),
(2,'Medium Parcel','Parcel',35),
(3,'Large Parcel','Parcel',75),
(4,'Freight','Freight',180);

-- =============================================
-- POPULATE FACT TABLES
-- =============================================

-- fact_deliveries (5,000 rows)
;WITH Numbers AS (
    SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS DeliveryID
    FROM sys.objects a CROSS JOIN sys.objects b
)
INSERT INTO fact_deliveries
(
    DeliveryID,
    DateKey,
    RouteKey,
    DriverKey,
    RegionKey,
    ShipmentTypeKey,
    PriorityFlag,
    SLA_Minutes,
    DeliveryMinutes,
    DeliveryStatus
)
SELECT
    n.DeliveryID,

    -- Real DateKey from dim_date
    (SELECT TOP 1 DateKey FROM dim_date ORDER BY NEWID()),

    -- Real foreign keys
    (SELECT TOP 1 RouteKey FROM dim_route ORDER BY NEWID()),
    (SELECT TOP 1 DriverKey FROM dim_driver ORDER BY NEWID()),
    (SELECT TOP 1 RegionKey FROM dim_region ORDER BY NEWID()),
    (SELECT TOP 1 ShipmentTypeKey FROM dim_shipment_type ORDER BY NEWID()),

    -- Priority logic (30% priority)
    CASE WHEN ABS(CHECKSUM(NEWID())) % 10 < 3 THEN 'Y' ELSE 'N' END,

    -- SLA based on shipment type
    CASE 
        WHEN ABS(CHECKSUM(NEWID())) % 3 = 0 THEN 720    -- Express (12h)
        WHEN ABS(CHECKSUM(NEWID())) % 3 = 1 THEN 1440   -- Standard (24h)
        ELSE 2880                                       -- Economy (48h)
    END,

    -- Delivery time influenced by risk factors
    CASE 
        WHEN ABS(CHECKSUM(NEWID())) % 10 < 2 THEN 4000 -- extreme delays
        ELSE 600 + ABS(CHECKSUM(NEWID()) % 2200)
    END,

    -- Status logic (correlated, not random)
    CASE 
        WHEN ABS(CHECKSUM(NEWID())) % 10 < 6 THEN 'On-Time'
        WHEN ABS(CHECKSUM(NEWID())) % 10 < 8 THEN 'Late'
        ELSE 'Exception'
    END
FROM Numbers n;

-- fact_exceptions (subset of deliveries marked Exception)
INSERT INTO fact_exceptions
(
    ExceptionID,
    DeliveryID,
    ExceptionTypeKey,
    DateKey,
    ResolutionMinutes
)
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ExceptionID,
    d.DeliveryID,
    (SELECT TOP 1 ExceptionTypeKey FROM dim_exception_type ORDER BY NEWID()),
    d.DateKey,
    30 + ABS(CHECKSUM(NEWID()) % 500)
FROM fact_deliveries d
WHERE d.DeliveryStatus = 'Exception';

-- fact_sales (3,500 rows)
INSERT INTO fact_sales
(
    SalesID,
    DeliveryID,
    DateKey,
    RegionKey,
    ProductKey,
    UnitsSold,
    SalesAmount
)
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS SalesID,
    d.DeliveryID,
    d.DateKey,
    d.RegionKey,
    (SELECT TOP 1 ProductKey FROM dim_product ORDER BY NEWID()),

    -- Units sold
    1 + ABS(CHECKSUM(NEWID()) % 5),

    -- Revenue correlated with priority & SLA
    CASE 
        WHEN d.PriorityFlag = 'Y' THEN 120 + ABS(CHECKSUM(NEWID()) % 400)
        ELSE 40 + ABS(CHECKSUM(NEWID()) % 200)
    END
FROM fact_deliveries d;

-- =============================================
-- ADVANCED ENTERPRISE LOGIC
-- =============================================

-- Holiday Season Delays (Nov-Dec)
UPDATE d
SET DeliveryMinutes = DeliveryMinutes + 800
FROM fact_deliveries d
JOIN dim_date dt ON d.DateKey = dt.DateKey
WHERE dt.Month IN (11, 12);

-- Less Experienced Drivers
UPDATE d
SET DeliveryMinutes = DeliveryMinutes + 300
FROM fact_deliveries d
JOIN dim_driver dr ON d.DriverKey = dr.DriverKey
WHERE dr.ExperienceLevel = 'Junior';


/*
FACT TABLES:
fact_deliveries
fact_exceptions
fact_sales

DIM TABLES:
dim_date
dim_region
dim_route
dim_driver
dim_shipment_type
dim_product
dim_exception_type */
