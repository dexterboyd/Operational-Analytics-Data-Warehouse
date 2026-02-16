-----------------------------------------------------
-- DATA CLEANING (STAGING DATA)
-- MOVE FROM <STAGING> TO <CLEAN>
-----------------------------------------------------

-- CHECK ROW COUNTS
----------------------------------------------
SELECT 'staging_sales' AS table_name, COUNT(*) AS row_count FROM staging.staging_sales
UNION ALL
SELECT 'staging_deliveries', COUNT(*) FROM staging.staging_deliveries
UNION ALL
SELECT 'staging_exceptions', COUNT(*) FROM staging.staging_exceptions
UNION ALL
SELECT 'staging_routes', COUNT(*) FROM staging.staging_routes;

-- CHECK TABLE STRUCTURE
----------------------------------------------
SELECT * FROM staging.staging_sales;
SELECT * FROM staging.staging_deliveries;
SELECT * FROM staging.staging_exceptions;
SELECT * FROM staging.staging_routes;

-- CHECK NULL VALUES
--------------------------------------------------
-- COALESCE returns the first non-NULL value from a list of expressions.
SELECT *
FROM staging.staging_sales
WHERE COALESCE(DeliveryID, SalesID, UnitsSold, SalesAmount) IS NULL;

SELECT *
FROM staging.staging_deliveries
WHERE COALESCE(DeliveryID, RouteID, DriverID) IS NULL;

SELECT *
FROM staging.staging_exceptions
WHERE COALESCE(ExceptionID, DeliveryID) IS NULL;

SELECT *
FROM staging.staging_routes
WHERE COALESCE(RouteID, DriverID) IS NULL;
---------------------
-- staging_sales
SELECT *
FROM staging.staging_sales
WHERE DeliveryID IS NULL
   OR SalesID IS NULL
   OR UnitsSold IS NULL
   OR SalesAmount IS NULL;

-- staging_deliveries
SELECT *
FROM staging.staging_deliveries
WHERE DeliveryID IS NULL
   OR RouteID IS NULL
   OR DriverID IS NULL;

-- staging_exceptions
SELECT *
FROM staging.staging_exceptions
WHERE ExceptionID IS NULL
   OR DeliveryID IS NULL;

-- staging_routes
SELECT *
FROM staging.staging_routes
WHERE RouteID IS NULL
   OR DriverID IS NULL;
----------------------------------------
CREATE VIEW clean.vw_sales AS
SELECT
    SalesID,
    DeliveryID,
    CONVERT(INT, FORMAT(DateKey, 'yyyyMMdd')) AS DateKey,
    LTRIM(RTRIM(ProductType)) AS ProductType,
    LTRIM(RTRIM(Region)) AS Region,
    UnitsSold,
    SalesAmount
FROM staging.staging_sales
WHERE UnitsSold > 0
  AND SalesAmount > 0;

CREATE VIEW clean.vw_deliveries AS
SELECT
    DeliveryID,
    RouteID,
    DriverID,
    LTRIM(RTRIM(Region)) AS Region,
    ShipmentType,
    DeliveryDate,
    ExpectedDeliveryDate,
    CASE
        WHEN DeliveryDate > ExpectedDeliveryDate THEN 'Late'
        ELSE DeliveryStatus
    END AS DeliveryStatus,
    CASE
        WHEN PriorityFlag IN ('TRUE', 1) THEN 1
        ELSE 0
    END AS PriorityFlag
FROM staging.staging_deliveries;

CREATE VIEW clean.vw_exceptions AS
SELECT
    ExceptionID,
    DeliveryID,
    ExceptionType,
    DateReported,
    CASE
        WHEN ResolvedDate < DateReported THEN DateReported
        ELSE ResolvedDate
    END AS ResolvedDate,
    CASE
        WHEN ResolutionTimeHours < 0 THEN NULL
        ELSE ResolutionTimeHours
    END AS ResolutionTimeHours,
    PriorityFlag,
    Region
FROM staging.staging_exceptions;

CREATE VIEW clean.vw_routes AS
SELECT
    RouteID,
    DriverID,
    PlannedStops,
    ActualStops,
    PlannedHours,
    ActualHours,
    Region
FROM staging.staging_routes
WHERE PlannedStops >= 0
  AND ActualStops >= 0
  AND PlannedHours > 0
  AND ActualHours > 0;

-- Validate Referential Integrity
----------------------------------------------
SELECT COUNT(*)
FROM clean.vw_sales s
LEFT JOIN clean.vw_deliveries d
    ON s.DeliveryID = d.DeliveryID
WHERE d.DeliveryID IS NULL;

SELECT COUNT(*)
FROM clean.vw_deliveries d
LEFT JOIN clean.vw_routes r
    ON d.RouteID = r.RouteID
WHERE r.RouteID IS NULL;