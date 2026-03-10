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