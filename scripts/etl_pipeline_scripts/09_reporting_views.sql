/*==============================================================
  REPORTING LAYER VIEWS - PRODUCTION READY
  Purpose:
      Provide stable, business-friendly datasets for Power BI:
        - Aggregate facts from DW fact tables
        - Join relevant dimensions
        - Simplify complex warehouse structures
        - Serve as a semantic layer for dashboards
  Notes:
      - Views are read-only, used directly in reporting tools
      - All calculations like ratios and totals are pre-aggregated
==============================================================*/

-----------------------------------------------------
-- 1️. Sales Summary
-- Purpose: Give high-level sales metrics per transaction
-- Joins: Sales fact → Product type, Region, Date
-- Calculates: Total revenue per sale
-----------------------------------------------------
CREATE OR ALTER VIEW reporting.vw_sales_summary
AS
SELECT
    s.SalesID,                        -- Transaction identifier
    s.DeliveryID,                     -- Related delivery
    dd.FullDate AS SaleDate,           -- Business-friendly date
    dp.ProductType,                    -- Product dimension
    dr.Region,                         -- Region dimension
    s.UnitsSold,                       -- Quantity sold
    s.SalesAmount,                     -- Unit sales amount
    (s.UnitsSold * s.SalesAmount) AS TotalRevenue  -- Calculated metric
FROM dw.fact_sales s
JOIN dw.dim_product_type dp ON s.ProductTypeID = dp.ProductTypeID
JOIN dw.dim_region dr ON s.RegionID = dr.RegionID
JOIN dw.dim_date dd ON s.DateKey = dd.DateKey;
GO

-----------------------------------------------------
-- 2️. Delivery Performance
-- Purpose: Aggregate deliveries with priority counts
-- Joins: Deliveries fact → Route, Driver, Shipment type, Status, Priority, Date
-- Calculates: Total deliveries and priority delivery count
-----------------------------------------------------
CREATE OR ALTER VIEW reporting.vw_delivery_performance
AS
SELECT
    d.DeliveryID,                      -- Delivery identifier
    r.RouteName AS Route,              -- Route dimension
    drv.DriverName AS Driver,          -- Driver dimension
    st.ShipmentType,                   -- Shipment type dimension
    dd.FullDate AS DeliveryDate,       -- Business-friendly date
    ds.DeliveryStatus,                 -- Status dimension
    SUM(CAST(pf.PriorityFlag AS INT)) AS PriorityDeliveries,  -- Priority deliveries aggregated
    COUNT(*) AS TotalDeliveries        -- Total deliveries for this grouping
FROM dw.fact_deliveries d
JOIN dw.dim_route r ON d.RouteID = r.RouteID
JOIN dw.dim_driver drv ON d.DriverID = drv.DriverID
JOIN dw.dim_shipment_type st ON d.ShipmentTypeID = st.ShipmentTypeID
JOIN dw.dim_delivery_status ds ON d.DeliveryStatusID = ds.DeliveryStatusID
JOIN dw.dim_priority_flag pf ON d.PriorityFlagID = pf.PriorityFlagID
JOIN dw.dim_date dd ON d.DateKey = dd.DateKey
GROUP BY d.DeliveryID, r.RouteName, drv.DriverName, st.ShipmentType, dd.FullDate, ds.DeliveryStatus;
GO

-----------------------------------------------------
-- 3️. Exception Dashboard
-- Purpose: Monitor exceptions with priority and resolution time
-- Joins: Exceptions fact → Exception type, Priority, Region, Date
-- Calculates: Total priority exceptions per record
-----------------------------------------------------
CREATE OR ALTER VIEW reporting.vw_exception_dashboard
AS
SELECT
    e.ExceptionID,                    -- Exception identifier
    e.DeliveryID,                     -- Associated delivery
    et.ExceptionType,                  -- Exception type dimension
    dd.FullDate AS DateReported,       -- Reported date
    e.ResolutionTimeHours,             -- Resolution duration
    SUM(CAST(pf.PriorityFlag AS INT)) AS PriorityExceptions,  -- Aggregate priority
    rg.Region                           -- Region dimension
FROM dw.fact_exceptions e
JOIN dw.dim_exception_type et ON e.ExceptionTypeID = et.ExceptionTypeID
JOIN dw.dim_priority_flag pf ON e.PriorityFlagID = pf.PriorityFlagID
JOIN dw.dim_region rg ON e.RegionID = rg.RegionID
JOIN dw.dim_date dd ON e.DateKey = dd.DateKey
GROUP BY e.ExceptionID, e.DeliveryID, et.ExceptionType, dd.FullDate, e.ResolutionTimeHours, rg.Region;
GO

-----------------------------------------------------
-- 4️. Route Efficiency
-- Purpose: Evaluate route and driver efficiency
-- Joins: Routes fact → Route, Driver, Region
-- Calculates: Stop completion ratio and hours efficiency
-----------------------------------------------------
CREATE OR ALTER VIEW reporting.vw_route_efficiency
AS
SELECT
    r.RouteID,                        -- Route surrogate key
    r.RouteName AS Route,             -- Route name for dashboards
    drv.DriverName AS Driver,         -- Driver dimension
    rg.Region,                        -- Region dimension
    SUM(rt.PlannedStops) AS TotalPlannedStops,  -- Aggregated planned stops
    SUM(rt.ActualStops) AS TotalActualStops,    -- Aggregated actual stops
    SUM(rt.PlannedHours) AS TotalPlannedHours, -- Aggregated planned hours
    SUM(rt.ActualHours) AS TotalActualHours,   -- Aggregated actual hours
    CASE 
        WHEN SUM(rt.PlannedStops) = 0 THEN NULL
        ELSE CAST(SUM(rt.ActualStops) AS FLOAT)/SUM(rt.PlannedStops)
    END AS StopCompletionRatio,       -- Efficiency ratio for stops
    CASE 
        WHEN SUM(rt.PlannedHours) = 0 THEN NULL
        ELSE CAST(SUM(rt.ActualHours) AS FLOAT)/SUM(rt.PlannedHours)
    END AS HourEfficiencyRatio       -- Efficiency ratio for hours
FROM dw.fact_routes rt
JOIN dw.dim_route r ON rt.RouteID = r.RouteID
JOIN dw.dim_driver drv ON rt.DriverID = drv.DriverID
JOIN dw.dim_region rg ON rt.RegionID = rg.RegionID
GROUP BY r.RouteID, r.RouteName, drv.DriverName, rg.Region;
GO

/*
/*==============================================================
  REPORTING LAYER VIEWS - PRODUCTION READY
  Purpose:
      Provide stable, business-friendly datasets for Power BI
      - Aggregates facts
      - Joins dimensions
      - Simplifies warehouse complexity
      - Semantic layer for dashboards
==============================================================*/

-----------------------------------------------------
-- 1️. Sales Summary
-----------------------------------------------------
CREATE OR ALTER VIEW reporting.vw_sales_summary
AS
SELECT
    s.SalesID,
    s.DeliveryID,
    dd.FullDate AS SaleDate,
    dp.ProductType,
    dr.Region,
    s.UnitsSold,
    s.SalesAmount,
    (s.UnitsSold * s.SalesAmount) AS TotalRevenue
FROM dw.fact_sales s
JOIN dw.dim_product_type dp ON s.ProductTypeID = dp.ProductTypeID
JOIN dw.dim_region dr ON s.RegionID = dr.RegionID
JOIN dw.dim_date dd ON s.DateKey = dd.DateKey;
GO

-----------------------------------------------------
-- 2️. Delivery Performance
-----------------------------------------------------
CREATE OR ALTER VIEW reporting.vw_delivery_performance
AS
SELECT
    d.DeliveryID,
    r.RouteName AS Route,
    drv.DriverName AS Driver,
    st.ShipmentType,
    dd.FullDate AS DeliveryDate,
    ds.DeliveryStatus,
    SUM(CAST(pf.PriorityFlag AS INT)) AS PriorityDeliveries,
    COUNT(*) AS TotalDeliveries
FROM dw.fact_deliveries d
JOIN dw.dim_route r ON d.RouteID = r.RouteID
JOIN dw.dim_driver drv ON d.DriverID = drv.DriverID
JOIN dw.dim_shipment_type st ON d.ShipmentTypeID = st.ShipmentTypeID
JOIN dw.dim_delivery_status ds ON d.DeliveryStatusID = ds.DeliveryStatusID
JOIN dw.dim_priority_flag pf ON d.PriorityFlagID = pf.PriorityFlagID
JOIN dw.dim_date dd ON d.DateKey = dd.DateKey
GROUP BY d.DeliveryID, r.RouteName, drv.DriverName, st.ShipmentType, dd.FullDate, ds.DeliveryStatus;
GO

-----------------------------------------------------
-- 3️. Exception Dashboard
-----------------------------------------------------
CREATE OR ALTER VIEW reporting.vw_exception_dashboard
AS
SELECT
    e.ExceptionID,
    e.DeliveryID,
    et.ExceptionType,
    dd.FullDate AS DateReported,
    e.ResolutionTimeHours,
    SUM(CAST(pf.PriorityFlag AS INT)) AS PriorityExceptions,
    rg.Region
FROM dw.fact_exceptions e
JOIN dw.dim_exception_type et ON e.ExceptionTypeID = et.ExceptionTypeID
JOIN dw.dim_priority_flag pf ON e.PriorityFlagID = pf.PriorityFlagID
JOIN dw.dim_region rg ON e.RegionID = rg.RegionID
JOIN dw.dim_date dd ON e.DateKey = dd.DateKey
GROUP BY e.ExceptionID, e.DeliveryID, et.ExceptionType, dd.FullDate, e.ResolutionTimeHours, rg.Region;
GO

-----------------------------------------------------
-- 4️. Route Efficiency
-----------------------------------------------------
CREATE OR ALTER VIEW reporting.vw_route_efficiency
AS
SELECT
    r.RouteID,
    r.RouteName AS Route,
    drv.DriverName AS Driver,
    rg.Region,
    SUM(rt.PlannedStops) AS TotalPlannedStops,
    SUM(rt.ActualStops) AS TotalActualStops,
    SUM(rt.PlannedHours) AS TotalPlannedHours,
    SUM(rt.ActualHours) AS TotalActualHours,
    CASE 
        WHEN SUM(rt.PlannedStops) = 0 THEN NULL
        ELSE CAST(SUM(rt.ActualStops) AS FLOAT)/SUM(rt.PlannedStops)
    END AS StopCompletionRatio,
    CASE 
        WHEN SUM(rt.PlannedHours) = 0 THEN NULL
        ELSE CAST(SUM(rt.ActualHours) AS FLOAT)/SUM(rt.PlannedHours)
    END AS HourEfficiencyRatio
FROM dw.fact_routes rt
JOIN dw.dim_route r ON rt.RouteID = r.RouteID
JOIN dw.dim_driver drv ON rt.DriverID = drv.DriverID
JOIN dw.dim_region rg ON rt.RegionID = rg.RegionID
GROUP BY r.RouteID, r.RouteName, drv.DriverName, rg.Region;
GO
*/
