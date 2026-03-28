/*=============================================================
  REPORTING LAYER
  Database: Fedex_Ops_Database
  Version:  1.0

  Purpose:
      Create aggregated views in the reporting schema for BI
      consumption. All views join DW fact and dimension tables
      and pre-compute the metrics most commonly needed in
      Power BI dashboards and operational reports.

  Run Order:
      1. etl_staging_setup.sql            -- build schemas/tables
      2. load_staging.py                  -- load CSV data
      3. staging_layer_validation.sql     -- validate staging
      4. clean_layer.sql                  -- build clean views
      5. 07_clean_validation_gate.sql     -- clean layer gate
      6. dw_layer_v1.sql                  -- build DW tables
      7. 09_dw_validation.sql             -- DW gate
      8. THIS SCRIPT                      -- build reporting views

  Reporting Views:
      reporting.rpt_delivery_performance  -- on-time/late/exception rates
      reporting.rpt_sales_performance     -- revenue and volume metrics
      reporting.rpt_exception_analysis    -- exception rates and resolution
      reporting.rpt_driver_performance    -- per-driver KPIs
      reporting.rpt_route_efficiency      -- route planned vs actual metrics
      reporting.rpt_executive_summary     -- single-row KPI snapshot

  Design Decisions:
      - All views are read-only aggregations over DW tables.
        No data is stored in the reporting schema.
      - Views are safe to re-run -- DROP + CREATE on each run.
      - All rate/percentage columns are rounded to 2 decimal
        places and expressed as percentages (0-100 scale).
      - NULL-safe division uses NULLIF to prevent divide-by-zero.
      - Views are designed for direct Power BI consumption:
        each view answers one specific analytical question
        and avoids wide unpivoted result sets.

=============================================================*/

USE Fedex_Ops_Database;
GO

/*=============================================================
  HELPER: DROP VIEWS IF THEY EXIST
  Allows this script to be safely re-run.
=============================================================*/

IF OBJECT_ID('reporting.rpt_delivery_performance', 'V') IS NOT NULL DROP VIEW reporting.rpt_delivery_performance;
IF OBJECT_ID('reporting.rpt_sales_performance',    'V') IS NOT NULL DROP VIEW reporting.rpt_sales_performance;
IF OBJECT_ID('reporting.rpt_exception_analysis',   'V') IS NOT NULL DROP VIEW reporting.rpt_exception_analysis;
IF OBJECT_ID('reporting.rpt_driver_performance',   'V') IS NOT NULL DROP VIEW reporting.rpt_driver_performance;
IF OBJECT_ID('reporting.rpt_route_efficiency',     'V') IS NOT NULL DROP VIEW reporting.rpt_route_efficiency;
IF OBJECT_ID('reporting.rpt_executive_summary',    'V') IS NOT NULL DROP VIEW reporting.rpt_executive_summary;
GO

/*=============================================================
  VIEW: reporting.rpt_delivery_performance

  Purpose:
      Delivery performance metrics grouped by year, month,
      region, shipment type, and route. Provides on-time rate,
      late rate, exception rate, and average days variance for
      trend analysis and operational dashboards.

  Grain:
      One row per unique combination of YearNumber, MonthNumber,
      Region, ShipmentType, and RouteID.

  Key Metrics:
      TotalDeliveries     Total delivery count for the group
      OnTimeCount         Deliveries with DeliveryStatus = On-Time
      LateCount           Deliveries with DeliveryStatus = Late
      ExceptionCount      Deliveries with DeliveryStatus = Exception
      OnTimeRate          OnTimeCount / TotalDeliveries * 100
      LateRate            LateCount / TotalDeliveries * 100
      ExceptionRate       ExceptionCount / TotalDeliveries * 100
      PriorityCount       Deliveries with PriorityFlag = 1
      AvgDaysVariance     Average days between actual and expected
                          delivery date (positive = late)
=============================================================*/

CREATE VIEW reporting.rpt_delivery_performance AS
SELECT
    -- Time dimensions from dim_date
    dd.YearNumber,
    dd.MonthNumber,
    dd.MonthName,
    dd.MonthShort,
    dd.Quarter,
    dd.QuarterName,
    dd.YearMonth,
    dd.YearQuarter,

    -- Dimension attributes
    dr.RegionCode,
    dr.RegionName,
    dst.ShipmentType,
    drt.RouteID,

    -- Volume metrics
    COUNT(*) AS TotalDeliveries,

    -- Status breakdown counts
    SUM(CASE WHEN fd.DeliveryStatus = 'On-Time' 
		THEN 1 ELSE 0 END) AS OnTimeCount,
    SUM(CASE WHEN fd.DeliveryStatus = 'Late'
		THEN 1 ELSE 0 END) AS LateCount,
    SUM(CASE WHEN fd.DeliveryStatus = 'Exception'
		THEN 1 ELSE 0 END) AS ExceptionCount,

    -- Rate metrics (0-100 scale, rounded to 2dp)
    ROUND(SUM(CASE WHEN fd.DeliveryStatus = 'On-Time'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS OnTimeRate,
    ROUND(SUM(CASE WHEN fd.DeliveryStatus = 'Late'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS LateRate,
    ROUND(SUM(CASE WHEN fd.DeliveryStatus = 'Exception'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS ExceptionRate,

    -- Priority delivery count
    SUM(CAST(fd.PriorityFlag AS INT)) AS PriorityCount,

    -- Average days variance (positive = delivered late on average)
    ROUND(AVG(CAST(fd.DaysVariance AS DECIMAL(10,2))), 2) AS AvgDaysVariance

	FROM dw.fact_deliveries fd
	JOIN dw.dim_date dd  ON dd.DateKey = fd.DateKey
	JOIN dw.dim_region dr  ON dr.RegionSK = fd.RegionSK
	JOIN dw.dim_shipment_type dst ON dst.ShipmentTypeSK = fd.ShipmentTypeSK
	JOIN dw.dim_route drt ON drt.RouteSK = fd.RouteSK

	GROUP BY
		dd.YearNumber,
		dd.MonthNumber,
		dd.MonthName,
		dd.MonthShort,
		dd.Quarter,
		dd.QuarterName,
		dd.YearMonth,
		dd.YearQuarter,
		dr.RegionCode,
		dr.RegionName,
		dst.ShipmentType,
		drt.RouteID;
GO

PRINT 'View created: reporting.rpt_delivery_performance';
GO

/*=============================================================
  VIEW: reporting.rpt_sales_performance

  Purpose:
      Sales revenue and volume metrics grouped by year, month,
      region, and product type. Supports revenue trending,
      product mix analysis, and regional performance comparison.

  Grain:
      One row per unique combination of YearNumber, MonthNumber,
      Region, and ProductType.

  Key Metrics:
      TotalTransactions   Count of sales records in the group
      TotalRevenue        Sum of SalesAmount
      TotalUnitsSold      Sum of UnitsSold
      AvgRevenuePerTx     Average SalesAmount per transaction
      AvgRevenuePerUnit   Average revenue per unit sold
      MinRevenue          Minimum SalesAmount in the group
      MaxRevenue          Maximum SalesAmount in the group
=============================================================*/

CREATE VIEW reporting.rpt_sales_performance AS
SELECT
    -- Time dimensions from dim_date
    dd.YearNumber,
    dd.MonthNumber,
    dd.MonthName,
    dd.MonthShort,
    dd.Quarter,
    dd.QuarterName,
    dd.YearMonth,
    dd.YearQuarter,

    -- Dimension attributes
    dr.RegionCode,
    dr.RegionName,
    dp.ProductType,

    -- Volume metrics
    COUNT(*)                                                AS TotalTransactions,
    SUM(fs.UnitsSold)                                       AS TotalUnitsSold,

    -- Revenue metrics
    ROUND(SUM(fs.SalesAmount), 2)                           AS TotalRevenue,
    ROUND(AVG(fs.SalesAmount), 2)                           AS AvgRevenuePerTx,
    ROUND(SUM(fs.SalesAmount) / NULLIF(SUM(fs.UnitsSold), 0), 2) AS AvgRevenuePerUnit,
    MIN(fs.SalesAmount)                                     AS MinRevenue,
    MAX(fs.SalesAmount)                                     AS MaxRevenue

	FROM dw.fact_sales fs
	JOIN dw.dim_date dd ON dd.DateKey = fs.DateKey
	JOIN dw.dim_region dr ON dr.RegionSK = fs.RegionSK
	JOIN dw.dim_product dp ON dp.ProductSK = fs.ProductSK

	GROUP BY
		dd.YearNumber,
		dd.MonthNumber,
		dd.MonthName,
		dd.MonthShort,
		dd.Quarter,
		dd.QuarterName,
		dd.YearMonth,
		dd.YearQuarter,
		dr.RegionCode,
		dr.RegionName,
		dp.ProductType;
GO

PRINT 'View created: reporting.rpt_sales_performance';
GO

/*=============================================================
  VIEW: reporting.rpt_exception_analysis

  Purpose:
      Exception counts, resolution rates, and average resolution
      times grouped by year, month, exception type, and region.
      Supports operational monitoring of exception trends and
      resolution efficiency.

  Grain:
      One row per unique combination of YearNumber, MonthNumber,
      ExceptionType, and Region.

  Key Metrics:
      TotalExceptions     Total exception count for the group
      ResolvedCount       Exceptions with IsResolved = 1
      OpenCount           Exceptions with IsResolved = 0
      ResolutionRate      ResolvedCount / TotalExceptions * 100
      AvgResolutionHours  Average hours to resolve (resolved only)
      AvgResolutionDays   Average days to resolve (resolved only)
      MinResolutionHours  Fastest resolution in the group
      MaxResolutionHours  Slowest resolution in the group
      PriorityCount       Exceptions with PriorityFlag = 1
=============================================================*/

CREATE VIEW reporting.rpt_exception_analysis AS
SELECT
    -- Time dimensions from dim_date (based on DateReported)
    dd.YearNumber,
    dd.MonthNumber,
    dd.MonthName,
    dd.MonthShort,
    dd.Quarter,
    dd.QuarterName,
    dd.YearMonth,
    dd.YearQuarter,

    -- Dimension attributes
    det.ExceptionType,
    dr.RegionCode,
    dr.RegionName,

    -- Volume metrics
    COUNT(*)                                                AS TotalExceptions,

    -- Resolution breakdown
    SUM(CAST(fe.IsResolved AS INT))                         AS ResolvedCount,
    SUM(CASE WHEN fe.IsResolved = 0
		THEN 1 ELSE 0 END) AS OpenCount,

    -- Resolution rate (0-100 scale)
    ROUND(SUM(CAST(fe.IsResolved AS INT)) * 100.0 / NULLIF(COUNT(*), 0), 2) AS ResolutionRate,

    -- Resolution time metrics (resolved exceptions only)
    ROUND(AVG(CASE WHEN fe.IsResolved = 1
		THEN fe.ResolutionTimeHours END), 2) AS AvgResolutionHours,

    ROUND(AVG(CASE WHEN fe.IsResolved = 1
		THEN CAST(fe.ResolutionDays AS DECIMAL(10,2)) END), 2) AS AvgResolutionDays,

    MIN(CASE WHEN fe.IsResolved = 1
		THEN fe.ResolutionTimeHours END) AS MinResolutionHours,
    MAX(CASE WHEN fe.IsResolved = 1
		THEN fe.ResolutionTimeHours END) AS MaxResolutionHours,

    -- Priority exception count
    SUM(CAST(fe.PriorityFlag AS INT)) AS PriorityCount

	FROM dw.fact_exceptions fe
	JOIN dw.dim_date dd ON dd.DateKey = fe.DateReportedKey
	JOIN dw.dim_exception_type det ON det.ExceptionTypeSK = fe.ExceptionTypeSK
	JOIN dw.dim_region dr ON dr.RegionSK = fe.RegionSK

	GROUP BY
		dd.YearNumber,
		dd.MonthNumber,
		dd.MonthName,
		dd.MonthShort,
		dd.Quarter,
		dd.QuarterName,
		dd.YearMonth,
		dd.YearQuarter,
		det.ExceptionType,
		dr.RegionCode,
		dr.RegionName;
GO

PRINT 'View created: reporting.rpt_exception_analysis';
GO

/*=============================================================
  VIEW: reporting.rpt_driver_performance

  Purpose:
      Per-driver KPI summary covering delivery performance,
      exception exposure, and priority delivery split. Used
      for driver-level operational reporting and ranking.

  Grain:
      One row per DriverCode (one row per driver).

  Key Metrics:
      TotalDeliveries     Total deliveries assigned to driver
      OnTimeCount         On-Time deliveries
      LateCount           Late deliveries
      ExceptionCount      Exception deliveries
      OnTimeRate          On-Time rate as a percentage
      LateRate            Late rate as a percentage
      ExceptionRate       Exception rate as a percentage
      TotalExceptions     Exceptions linked to driver deliveries
      ExceptionPerDelivery Exceptions per delivery ratio
      PriorityCount       Priority deliveries handled
      PriorityRate        Priority deliveries as % of total
      AvgDaysVariance     Average delivery days variance
      RegionsServed       Count of distinct regions served
=============================================================*/

CREATE VIEW reporting.rpt_driver_performance AS
SELECT
    -- Driver attributes
    drv.DriverCode,
    drv.DriverLabel,
    drv.IsUnknown,

    -- Delivery volume
    COUNT(DISTINCT fd.DeliveryID) AS TotalDeliveries,

    -- Delivery status breakdown
    SUM(CASE WHEN fd.DeliveryStatus = 'On-Time'
		THEN 1 ELSE 0 END) AS OnTimeCount,
    SUM(CASE WHEN fd.DeliveryStatus = 'Late'
		THEN 1 ELSE 0 END) AS LateCount,
    SUM(CASE WHEN fd.DeliveryStatus = 'Exception'
		THEN 1 ELSE 0 END) AS ExceptionCount,

    -- Delivery rate metrics (0-100 scale)
    ROUND(SUM(CASE WHEN fd.DeliveryStatus = 'On-Time'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT fd.DeliveryID), 0), 2) AS OnTimeRate,

    ROUND(SUM(CASE WHEN fd.DeliveryStatus = 'Late'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT fd.DeliveryID), 0), 2) AS LateRate,

    ROUND(SUM(CASE WHEN fd.DeliveryStatus = 'Exception'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT fd.DeliveryID), 0), 2) AS ExceptionRate,

    -- Exception exposure
    -- Left join so drivers with zero exceptions still appear
    COUNT(DISTINCT fe.ExceptionID) AS TotalExceptions,

    ROUND(COUNT(DISTINCT fe.ExceptionID) * 1.0 / NULLIF(COUNT(DISTINCT fd.DeliveryID), 0), 4) AS ExceptionPerDelivery,

    -- Priority delivery metrics
    SUM(CAST(fd.PriorityFlag AS INT)) AS PriorityCount,

    ROUND(SUM(CAST(fd.PriorityFlag AS INT)) * 100.0 / NULLIF(COUNT(DISTINCT fd.DeliveryID), 0), 2) AS PriorityRate,

    -- Average delivery variance
    ROUND(AVG(CAST(fd.DaysVariance AS DECIMAL(10,2))), 2) AS AvgDaysVariance,

    -- Operational breadth
    COUNT(DISTINCT fd.RegionSK) AS RegionsServed

	FROM dw.dim_driver drv
	JOIN dw.fact_deliveries fd ON fd.DriverSK = drv.DriverSK
	LEFT JOIN dw.fact_exceptions fe ON fe.DeliveryID = fd.DeliveryID

	GROUP BY
		drv.DriverCode,
		drv.DriverLabel,
		drv.IsUnknown;
GO

PRINT 'View created: reporting.rpt_driver_performance';
GO

/*=============================================================
  VIEW: reporting.rpt_route_efficiency

  Purpose:
      Route-level efficiency metrics comparing planned vs actual
      stops and hours across all route runs. Supports operational
      analysis of route planning accuracy and driver efficiency
      by route.

  Grain:
      One row per RouteID. Aggregates across all route runs
      and all drivers that have operated that route.

  Key Metrics:
      TotalRuns           Total route run records for the route
      DriversAssigned     Count of distinct drivers on the route
      TotalPlannedStops   Sum of all planned stops
      TotalActualStops    Sum of all actual stops
      AvgPlannedStops     Average planned stops per run
      AvgActualStops      Average actual stops per run
      AvgStopVariance     Average (ActualStops - PlannedStops)
      AvgPlannedHours     Average planned hours per run
      AvgActualHours      Average actual hours per run
      AvgHourVariance     Average (ActualHours - PlannedHours)
      AvgStopEfficiency   Average stop efficiency % across runs
      AvgHourEfficiency   Average hour efficiency % across runs
=============================================================*/

CREATE VIEW reporting.rpt_route_efficiency AS
SELECT
    -- Route identifier
    drt.RouteID,

    -- Run volume
    COUNT(*)                                                AS TotalRuns,
    COUNT(DISTINCT cr.DriverID)                             AS DriversAssigned,

    -- Stop metrics (aggregated across all runs)
    SUM(cr.PlannedStops)                                    AS TotalPlannedStops,
    SUM(cr.ActualStops)                                     AS TotalActualStops,
    ROUND(AVG(CAST(cr.PlannedStops AS DECIMAL(10,2))), 2)   AS AvgPlannedStops,
    ROUND(AVG(CAST(cr.ActualStops  AS DECIMAL(10,2))), 2)   AS AvgActualStops,
    ROUND(AVG(CAST(cr.StopVariance AS DECIMAL(10,2))), 2)   AS AvgStopVariance,

    -- Hour metrics (aggregated across all runs)
    ROUND(SUM(cr.PlannedHours), 2)                          AS TotalPlannedHours,
    ROUND(SUM(cr.ActualHours),  2)                          AS TotalActualHours,
    ROUND(AVG(cr.PlannedHours), 2)                          AS AvgPlannedHours,
    ROUND(AVG(cr.ActualHours),  2)                          AS AvgActualHours,
    ROUND(AVG(cr.HourVariance), 2)                          AS AvgHourVariance,

    -- Efficiency metrics (average across all runs)
    ROUND(AVG(cr.StopEfficiencyPct), 2)                     AS AvgStopEfficiency,
    ROUND(AVG(cr.HourEfficiencyPct), 2)                     AS AvgHourEfficiency,

    -- Overall stop efficiency across all runs combined
    ROUND(SUM(cr.ActualStops) * 100.0 / NULLIF(SUM(cr.PlannedStops), 0), 2) AS OverallStopEfficiency,

    -- Overall hour efficiency across all runs combined
    ROUND(SUM(cr.PlannedHours) * 100.0 / NULLIF(SUM(cr.ActualHours), 0), 2) AS OverallHourEfficiency

	FROM clean.clean_routes cr
	JOIN dw.dim_route drt ON drt.RouteID = cr.RouteID

	GROUP BY drt.RouteID;
GO

PRINT 'View created: reporting.rpt_route_efficiency';
GO

/*=============================================================
  VIEW: reporting.rpt_executive_summary

  Purpose:
      Single-row KPI snapshot across all operational areas.
      Designed for Power BI dashboard header cards and
      executive-level reporting. Covers the full dataset
      with no time or dimensional filtering.

  Grain:
      One row (full dataset aggregate).

  Key Metrics:
      -- Delivery KPIs
      TotalDeliveries, OnTimeRate, LateRate, ExceptionRate
      PriorityDeliveries, AvgDaysVariance

      -- Sales KPIs
      TotalSalesTransactions, TotalRevenue, AvgRevenuePerTx
      TotalUnitsSold, AvgRevenuePerUnit

      -- Exception KPIs
      TotalExceptions, ResolvedExceptions, OpenExceptions
      OverallResolutionRate, AvgResolutionHours

      -- Route KPIs
      TotalRouteRuns, AvgStopEfficiency, AvgHourEfficiency
=============================================================*/

CREATE VIEW reporting.rpt_executive_summary AS
SELECT

    /*---------------------------------------------------------
      DELIVERY KPIs
    ---------------------------------------------------------*/
    (SELECT COUNT(*)
     FROM dw.fact_deliveries) AS TotalDeliveries,

    (SELECT ROUND(SUM(CASE WHEN DeliveryStatus = 'On-Time'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
     FROM dw.fact_deliveries) AS OnTimeRate,

    (SELECT ROUND(SUM(CASE WHEN DeliveryStatus = 'Late'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
     FROM dw.fact_deliveries) AS LateRate,

    (SELECT ROUND(SUM(CASE WHEN DeliveryStatus = 'Exception'
		THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
     FROM dw.fact_deliveries) AS ExceptionRate,

    (SELECT SUM(CAST(PriorityFlag AS INT))
     FROM dw.fact_deliveries) AS PriorityDeliveries,

    (SELECT ROUND(AVG(CAST(DaysVariance AS DECIMAL(10,2))), 2)
     FROM dw.fact_deliveries
     WHERE DaysVariance IS NOT NULL) AS AvgDaysVariance,

    /*---------------------------------------------------------
      SALES KPIs
    ---------------------------------------------------------*/
    (SELECT COUNT(*)
     FROM dw.fact_sales) AS TotalSalesTransactions,

    (SELECT ROUND(SUM(SalesAmount), 2)
     FROM dw.fact_sales) AS TotalRevenue,

    (SELECT ROUND(AVG(SalesAmount), 2)
     FROM dw.fact_sales) AS AvgRevenuePerTx,

    (SELECT SUM(UnitsSold)
     FROM dw.fact_sales) AS TotalUnitsSold,

    (SELECT ROUND(SUM(SalesAmount) / NULLIF(SUM(UnitsSold), 0), 2)
	FROM dw.fact_sales) AS AvgRevenuePerUnit,

    /*---------------------------------------------------------
      EXCEPTION KPIs
    ---------------------------------------------------------*/
    (SELECT COUNT(*)
     FROM dw.fact_exceptions) AS TotalExceptions,

    (SELECT SUM(CAST(IsResolved AS INT))
     FROM dw.fact_exceptions) AS ResolvedExceptions,

    (SELECT SUM(CASE WHEN IsResolved = 0
		THEN 1 ELSE 0 END)
     FROM dw.fact_exceptions) AS OpenExceptions,

    (SELECT ROUND(SUM(CAST(IsResolved AS INT)) * 100.0 / NULLIF(COUNT(*), 0), 2)
     FROM dw.fact_exceptions) AS OverallResolutionRate,

    (SELECT ROUND(AVG(ResolutionTimeHours), 2)
     FROM dw.fact_exceptions
     WHERE ResolutionTimeHours IS NOT NULL) AS AvgResolutionHours,

    /*---------------------------------------------------------
      ROUTE KPIs
      Sourced from clean_routes as route performance data
      is not stored in a separate fact table.
    ---------------------------------------------------------*/
    (SELECT COUNT(*)
     FROM clean.clean_routes) AS TotalRouteRuns,

    (SELECT ROUND(AVG(StopEfficiencyPct), 2)
     FROM clean.clean_routes
     WHERE StopEfficiencyPct IS NOT NULL) AS AvgStopEfficiency,

    (SELECT ROUND(AVG(HourEfficiencyPct), 2)
     FROM clean.clean_routes
     WHERE HourEfficiencyPct IS NOT NULL) AS AvgHourEfficiency;
GO

PRINT 'View created: reporting.rpt_executive_summary';
GO

/*=============================================================
  REPORTING LAYER SMOKE TEST
  Confirms all six views are accessible and returning rows.
  Row counts and a sample from each view are printed for
  quick sense checking.
=============================================================*/

PRINT '--- REPORTING LAYER SMOKE TEST ---';

-- Row counts per reporting view
SELECT 'rpt_delivery_performance' AS ViewName, COUNT(*) AS RowsCount
FROM reporting.rpt_delivery_performance
UNION ALL

SELECT 'rpt_sales_performance', COUNT(*)
FROM reporting.rpt_sales_performance
UNION ALL

SELECT 'rpt_exception_analysis', COUNT(*)
FROM reporting.rpt_exception_analysis
UNION ALL

SELECT 'rpt_driver_performance', COUNT(*)
FROM reporting.rpt_driver_performance
UNION ALL

SELECT 'rpt_route_efficiency', COUNT(*)
FROM reporting.rpt_route_efficiency
UNION ALL

SELECT 'rpt_executive_summary', COUNT(*)
FROM reporting.rpt_executive_summary;

-- Executive summary KPI snapshot
SELECT * FROM reporting.rpt_executive_summary;

-- Top 5 drivers by on-time rate (minimum 50 deliveries)
SELECT TOP 5
    DriverCode,
    TotalDeliveries,
    OnTimeRate,
    LateRate,
    ExceptionRate,
    AvgDaysVariance
FROM reporting.rpt_driver_performance
WHERE TotalDeliveries >= 50
  AND IsUnknown = 0
ORDER BY OnTimeRate DESC;

-- Route efficiency summary
SELECT
    RouteID,
    TotalRuns,
    AvgStopEfficiency,
    AvgHourEfficiency,
    OverallStopEfficiency,
    OverallHourEfficiency
FROM reporting.rpt_route_efficiency
ORDER BY RouteID;

PRINT '--- END OF SMOKE TEST ---';
GO
