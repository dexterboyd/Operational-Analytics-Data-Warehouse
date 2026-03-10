/*==============================================================
  DW VALIDATION SCRIPT
  Purpose:
      Validate Data Warehouse tables after load.

  Checks:
      - Row counts
      - Surrogate key integrity
      - Foreign key consistency
      - Data completeness
==============================================================*/

PRINT '--- DW VALIDATION START ---';

-----------------------------------------------------
-- STEP 1: TABLE ROW COUNTS
-----------------------------------------------------

SELECT 'dim_product_type' AS TableName, COUNT(*) AS Row_Count FROM dw.dim_product_type
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
SELECT 'dim_date', COUNT(*) FROM dw.dim_date
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM dw.fact_sales
UNION ALL
SELECT 'fact_deliveries', COUNT(*) FROM dw.fact_deliveries
UNION ALL
SELECT 'fact_routes', COUNT(*) FROM dw.fact_routes
UNION ALL
SELECT 'fact_exceptions', COUNT(*) FROM dw.fact_exceptions;

-----------------------------------------------------
-- STEP 2: FACT TABLE NULL KEY CHECKS
-----------------------------------------------------

SELECT 'fact_sales.ProductTypeID' AS FieldName, COUNT(*) AS NullCount
FROM dw.fact_sales
WHERE ProductTypeID IS NULL

UNION ALL

SELECT 'fact_sales.RegionID', COUNT(*)
FROM dw.fact_sales
WHERE RegionID IS NULL;

-----------------------------------------------------
-- STEP 3: FOREIGN KEY INTEGRITY CHECKS
-----------------------------------------------------

-- Sales → Product
SELECT COUNT(*) AS MissingProductDimension
FROM dw.fact_sales f
LEFT JOIN dw.dim_product_type d
ON f.ProductTypeID = d.ProductTypeID
WHERE d.ProductTypeID IS NULL;

-- Sales → Region
SELECT COUNT(*) AS MissingRegionDimension
FROM dw.fact_sales f
LEFT JOIN dw.dim_region r
ON f.RegionID = r.RegionID
WHERE r.RegionID IS NULL;

-- Deliveries → Route
SELECT COUNT(*) AS MissingRouteDimension
FROM dw.fact_deliveries f
LEFT JOIN dw.dim_route r
ON f.RouteID = r.RouteID
WHERE r.RouteID IS NULL;

-----------------------------------------------------
-- STEP 4: FACT TABLE DATA QUALITY
-----------------------------------------------------

-- Negative sales values
SELECT COUNT(*) AS NegativeSales
FROM dw.fact_sales
WHERE SalesAmount < 0;

-- Invalid delivery relationships
SELECT COUNT(*) AS InvalidDeliveryRecords
FROM dw.fact_deliveries
WHERE DeliveryID IS NULL;

-----------------------------------------------------
-- STEP 5: BUSINESS METRICS CHECK
-----------------------------------------------------

SELECT
    COUNT(*) AS TotalSalesTransactions,
    SUM(SalesAmount) AS TotalSalesRevenue,
    AVG(SalesAmount) AS AvgSalesValue
FROM dw.fact_sales;

PRINT '--- DW VALIDATION COMPLETE ---';