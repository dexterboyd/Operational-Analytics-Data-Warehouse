/*==============================================================
  DW VALIDATION SCRIPT
  File: 07_dw_validation.sql

  Purpose:
      Validate Data Warehouse tables after ETL load.

  Validations performed:
      1. Row counts for dimensions and fact tables
      2. Surrogate key integrity checks
      3. Referential integrity checks
      4. Fact table NULL checks
      5. Business sanity checks

  Pipeline Position:
      staging → clean → DW load → DW validation → reporting → BI

  If any validation returns non-zero error counts,
  the ETL run should be investigated before reporting refresh.

==============================================================*/

PRINT '--- DW VALIDATION START ---';


-----------------------------------------------------
-- STEP 1: ROW COUNT SUMMARY
-----------------------------------------------------

PRINT 'STEP 1: DW TABLE ROW COUNTS';

SELECT 'dim_date' AS TableName, COUNT(*) AS Row_Count FROM dw.dim_date
UNION ALL
SELECT 'dim_product_type', COUNT(*) FROM dw.dim_product_type
UNION ALL
SELECT 'dim_region', COUNT(*) FROM dw.dim_region
UNION ALL
SELECT 'dim_driver', COUNT(*) FROM dw.dim_driver
UNION ALL
SELECT 'dim_route', COUNT(*) FROM dw.dim_route
UNION ALL
SELECT 'dim_shipment_type', COUNT(*) FROM dw.dim_shipment_type
UNION ALL
SELECT 'dim_delivery_status', COUNT(*) FROM dw.dim_delivery_status
UNION ALL
SELECT 'dim_exception_type', COUNT(*) FROM dw.dim_exception_type
UNION ALL
SELECT 'dim_priority_flag', COUNT(*) FROM dw.dim_priority_flag
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM dw.fact_sales
UNION ALL
SELECT 'fact_deliveries', COUNT(*) FROM dw.fact_deliveries
UNION ALL
SELECT 'fact_routes', COUNT(*) FROM dw.fact_routes
UNION ALL
SELECT 'fact_exceptions', COUNT(*) FROM dw.fact_exceptions;


-----------------------------------------------------
-- STEP 2: FACT TABLE NULL CHECKS
-----------------------------------------------------

PRINT 'STEP 2: FACT TABLE NULL CHECKS';

-- Sales
SELECT COUNT(*) AS NullDateKey
FROM dw.fact_sales
WHERE DateKey IS NULL;

SELECT COUNT(*) AS NullProductType
FROM dw.fact_sales
WHERE ProductTypeID IS NULL;

SELECT COUNT(*) AS NullRegion
FROM dw.fact_sales
WHERE RegionID IS NULL;

-- Deliveries
SELECT COUNT(*) AS NullRoute
FROM dw.fact_deliveries
WHERE RouteID IS NULL;

SELECT COUNT(*) AS NullDriver
FROM dw.fact_deliveries
WHERE DriverID IS NULL;

SELECT COUNT(*) AS NullShipmentType
FROM dw.fact_deliveries
WHERE ShipmentTypeID IS NULL;

SELECT COUNT(*) AS NullDeliveryStatus
FROM dw.fact_deliveries
WHERE DeliveryStatusID IS NULL;

SELECT COUNT(*) AS NullPriority
FROM dw.fact_deliveries
WHERE PriorityFlagID IS NULL;


-----------------------------------------------------
-- STEP 3: SURROGATE KEY INTEGRITY
-----------------------------------------------------

PRINT 'STEP 3: SURROGATE KEY VALIDATION';

-- ProductType
SELECT COUNT(*) AS MissingProductTypeKey
FROM dw.fact_sales f
LEFT JOIN dw.dim_product_type d
ON f.ProductTypeID = d.ProductTypeID
WHERE d.ProductTypeID IS NULL;

-- Region
SELECT COUNT(*) AS MissingRegionKey
FROM dw.fact_sales f
LEFT JOIN dw.dim_region d
ON f.RegionID = d.RegionID
WHERE d.RegionID IS NULL;

-- Date
SELECT COUNT(*) AS MissingDateKey
FROM dw.fact_sales f
LEFT JOIN dw.dim_date d
ON f.DateKey = d.DateKey
WHERE d.DateKey IS NULL;


-----------------------------------------------------
-- STEP 4: REFERENTIAL INTEGRITY CHECKS
-----------------------------------------------------

PRINT 'STEP 4: FACT → DIMENSION VALIDATION';

-- Deliveries → Route
SELECT COUNT(*) AS InvalidRouteReference
FROM dw.fact_deliveries f
LEFT JOIN dw.dim_route d
ON f.RouteID = d.RouteID
WHERE d.RouteID IS NULL;

-- Deliveries → Driver
SELECT COUNT(*) AS InvalidDriverReference
FROM dw.fact_deliveries f
LEFT JOIN dw.dim_driver d
ON f.DriverID = d.DriverID
WHERE d.DriverID IS NULL;

-- Deliveries → ShipmentType
SELECT COUNT(*) AS InvalidShipmentTypeReference
FROM dw.fact_deliveries f
LEFT JOIN dw.dim_shipment_type d
ON f.ShipmentTypeID = d.ShipmentTypeID
WHERE d.ShipmentTypeID IS NULL;


-----------------------------------------------------
-- STEP 5: BUSINESS METRIC SANITY CHECKS
-----------------------------------------------------

PRINT 'STEP 5: DATA SANITY CHECKS';

-- Sales totals
SELECT
    COUNT(*) AS TotalTransactions,
    SUM(SalesAmount) AS TotalSales,
    AVG(SalesAmount) AS AvgSale,
    MIN(SalesAmount) AS MinSale,
    MAX(SalesAmount) AS MaxSale
FROM dw.fact_sales;

-- Delivery counts by status
SELECT
    s.DeliveryStatus,
    COUNT(*) AS Deliveries
FROM dw.fact_deliveries f
JOIN dw.dim_delivery_status s
ON f.DeliveryStatusID = s.DeliveryStatusID
GROUP BY s.DeliveryStatus
ORDER BY Deliveries DESC;

-- Exceptions by type
SELECT
    e.ExceptionType,
    COUNT(*) AS ExceptionCount
FROM dw.fact_exceptions f
JOIN dw.dim_exception_type e
ON f.ExceptionTypeID = e.ExceptionTypeID
GROUP BY e.ExceptionType
ORDER BY ExceptionCount DESC;


-----------------------------------------------------
-- STEP 6: FACT TABLE DISTRIBUTIONS
-----------------------------------------------------

PRINT 'STEP 6: FACT DISTRIBUTION CHECKS';

-- Sales by region
SELECT
    r.Region,
    COUNT(*) AS Transactions,
    SUM(f.SalesAmount) AS TotalSales
FROM dw.fact_sales f
JOIN dw.dim_region r
ON f.RegionID = r.RegionID
GROUP BY r.Region
ORDER BY TotalSales DESC;

-- Routes by driver
SELECT
    d.DriverName,
    COUNT(*) AS RouteRecords
FROM dw.fact_routes f
JOIN dw.dim_driver d
ON f.DriverID = d.DriverID
GROUP BY d.DriverName
ORDER BY RouteRecords DESC;


PRINT '--- DW VALIDATION COMPLETE ---';