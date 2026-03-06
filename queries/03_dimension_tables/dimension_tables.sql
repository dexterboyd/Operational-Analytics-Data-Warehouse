-----------------------------------------------------
-- DIM Tables (DW DATA)
-----------------------------------------------------

-- CHECK ROW COUNTS
---------------------------------------------
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dw.dim_date
UNION ALL
SELECT 'dim_delivery_status', COUNT(*) FROM dw.dim_delivery_status
UNION ALL
SELECT 'dim_driver', COUNT(*) FROM dw.dim_driver
UNION ALL
SELECT 'dim_exception_type', COUNT(*) FROM dw.dim_exception_type
UNION ALL
SELECT 'dim_priority_flag', COUNT(*) FROM dw.dim_priority_flag
UNION ALL
SELECT 'dim_product_type', COUNT(*) FROM dw.dim_product_type
UNION ALL
SELECT 'dim_region', COUNT(*) FROM dw.dim_region
UNION ALL
SELECT 'dim_route', COUNT(*) FROM dw.dim_route
UNION ALL
SELECT 'dim_shipment_type', COUNT(*) FROM dw.dim_shipment_type;

-- CHECK TABLE STRUCTURE
----------------------------------------------
SELECT * FROM dw.dim_date;
SELECT * FROM dw.dim_delivery_status;
SELECT * FROM dw.dim_driver;
SELECT * FROM dw.dim_exception_type;
SELECT * FROM dw.dim_priority_flag;
SELECT * FROM dw.dim_product_type;
SELECT * FROM dw.dim_region;
SELECT * FROM dw.dim_route;
SELECT * FROM dw.dim_shipment_type;
--------------------------------------------------------

-- Populate dim tables
--------------------------------------------
-- dw.dim_date
DECLARE @Date DATE = '2023-01-01';

WHILE @Date <= '2025-12-31'
BEGIN
    INSERT INTO dw.dim_date
    VALUES (
        CONVERT(INT, FORMAT(@Date, 'yyyyMMdd')),
        @Date,
        YEAR(@Date),
        DATEPART(QUARTER, @Date),
        MONTH(@Date),
        DAY(@Date),
        DATEPART(WEEKDAY, @Date),
        CASE WHEN DATEPART(WEEKDAY, @Date) IN (1,7) THEN 1 ELSE 0 END,
        DATENAME(MONTH, @Date),
        0,
        DATENAME(WEEKDAY, @Date),
        DATEPART(WEEK, @Date),
        DATENAME(MONTH, @Date) + ' ' + CAST(YEAR(@Date) AS VARCHAR),
        FORMAT(@Date, 'yyyy-MM'),
        YEAR(@Date)
    );
    SET @Date = DATEADD(DAY, 1, @Date);
END;
-------------------------------------------------
-- dw.dim_region
INSERT INTO dw.dim_region (RegionName)
SELECT DISTINCT
    LTRIM(RTRIM(Region))
FROM staging.staging_routes
WHERE Region IS NOT NULL;
-----------------------------------------------
-- dw.dim_driver
INSERT INTO dw.dim_driver (DriverName)
SELECT DISTINCT
    LTRIM(RTRIM(DriverID))
FROM staging.staging_deliveries
WHERE DriverID IS NOT NULL;
-------------------------------------
-- dw.dim_route
INSERT INTO dw.dim_route (RouteID, Region)
SELECT DISTINCT
    LTRIM(RTRIM(RouteID)) AS RouteID,
    LTRIM(RTRIM(Region)) AS Region
FROM staging.staging_routes
WHERE RouteID IS NOT NULL;
-----------------------------------------------
-- dim_product_type (from staging_sales)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_product_type' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.dim_product_type (
        ProductTypeKey INT IDENTITY(1,1) PRIMARY KEY,
        ProductType NVARCHAR(100) NOT NULL
    );
END

-- Populate dim_product_type
INSERT INTO dw.dim_product_type (ProductType)
SELECT DISTINCT LTRIM(RTRIM(ProductType)) AS ProductType
FROM staging.staging_sales
WHERE ProductType IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 
      FROM dw.dim_product_type dpt
      WHERE dpt.ProductType = LTRIM(RTRIM(staging.staging_sales.ProductType))
  );
------------------------------------------------
 -- dim_shipment_type (from staging_deliveries)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_shipment_type' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.dim_shipment_type (
        ShipmentTypeKey INT IDENTITY(1,1) PRIMARY KEY,
        ShipmentType NVARCHAR(100) NOT NULL
    );
END

-- Populate dim_shipment_type
INSERT INTO dw.dim_shipment_type (ShipmentType)
SELECT DISTINCT LTRIM(RTRIM(ShipmentType)) AS ShipmentType
FROM staging.staging_deliveries
WHERE ShipmentType IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 
      FROM dw.dim_shipment_type dst
      WHERE dst.ShipmentType = LTRIM(RTRIM(staging.staging_deliveries.ShipmentType))
  );
----------------------------------------------
-- Create dim_delivery_status (from staging_deliveries)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_delivery_status' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.dim_delivery_status (
        DeliveryStatusKey INT IDENTITY(1,1) PRIMARY KEY,
        DeliveryStatus NVARCHAR(100) NOT NULL
    );
END

-- Populate dim_delivery_status
INSERT INTO dw.dim_delivery_status (DeliveryStatus)
SELECT DISTINCT LTRIM(RTRIM(DeliveryStatus)) AS DeliveryStatus
FROM staging.staging_deliveries
WHERE DeliveryStatus IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dw.dim_delivery_status dds
      WHERE dds.DeliveryStatus = LTRIM(RTRIM(staging.staging_deliveries.DeliveryStatus))
  );
------------------------------------------
-- Create dim_exception_type (from staging_exceptions)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_exception_type' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.dim_exception_type (
        ExceptionTypeKey INT IDENTITY(1,1) PRIMARY KEY,
        ExceptionType NVARCHAR(100) NOT NULL
    );
END

-- Populate dim_exception_type
INSERT INTO dw.dim_exception_type (ExceptionType)
SELECT DISTINCT LTRIM(RTRIM(ExceptionType)) AS ExceptionType
FROM staging.staging_exceptions
WHERE ExceptionType IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dw.dim_exception_type det
      WHERE det.ExceptionType = LTRIM(RTRIM(staging.staging_exceptions.ExceptionType))
  );
-----------------------------------------
-- Create dim_priority_flag (from staging_deliveries & staging_exceptions)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_priority_flag' AND schema_id = SCHEMA_ID('dw'))
BEGIN
    CREATE TABLE dw.dim_priority_flag (
        PriorityFlagKey INT IDENTITY(1,1) PRIMARY KEY,
        PriorityFlag NVARCHAR(50) NOT NULL
    );
END

-- Populate dim_priority_flag
INSERT INTO dw.dim_priority_flag (PriorityFlag)
SELECT DISTINCT LTRIM(RTRIM(PriorityFlag)) AS PriorityFlag
FROM (
    SELECT PriorityFlag FROM staging.staging_deliveries
    UNION
    SELECT PriorityFlag FROM staging.staging_exceptions
) AS CombinedFlags
WHERE PriorityFlag IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dw.dim_priority_flag dpf
      WHERE dpf.PriorityFlag = LTRIM(RTRIM(CombinedFlags.PriorityFlag))
  );
-------------------------------------------

