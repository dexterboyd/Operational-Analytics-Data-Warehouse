/*==============================================================
  REPORTING LAYER VIEWS
  Database: Fedex_Ops_Database
  Schema:   reporting
  Version:  2.0

  Purpose:
      Provide stable, business-friendly datasets for Power BI.
      Each view joins DW fact tables to their dimension tables
      and exposes columns in business-readable terms.
      Calculations that are already stored in fact tables are
      passed through directly rather than recomputed here.

  Views:
      1. vw_sales_summary        - One row per sales transaction
      2. vw_delivery_performance - One row per delivery
      3. vw_exception_dashboard  - One row per exception record
      4. vw_route_efficiency     - One row per route + driver

  Pipeline Position:
      staging -> clean -> dw -> [THIS SCRIPT] -> Power BI

  Design Notes:
      - These are flat detail views (one row per fact row).
        Aggregations and roll-ups belong in Power BI measures
        or in a separate set of summary views at a higher grain.
      - All GROUP BY / SUM patterns have been removed from the
        per-row detail views because grouping by the fact PK
        produces trivially single-row groups with no meaningful
        aggregation effect.
      - Efficiency ratios and variances stored in fact_routes
        are surfaced directly rather than recomputed, ensuring
        Power BI shows the same values as the DW fact table.

  Change Log:
      v2.0 - Removed duplicate commented block.
           - Updated all RouteName -> RouteCode and
             DriverName -> DriverCode references to match the
             v2.0 DW dimension column renames.
           - Fixed vw_delivery_performance: replaced the old
             single DateKey join with separate joins for
             DeliveryDateKey and ExpectedDeliveryDateKey, and
             added ExpectedDeliveryDate to the SELECT so
             late-delivery analysis is possible in Power BI.
           - Removed vacuous GROUP BY / aggregate pattern from
             vw_delivery_performance, vw_exception_dashboard,
             and vw_route_efficiency. Each fact table has one
             row per PK; grouping by the PK and summing a
             single-row group always returns 1. These are now
             flat joins.
           - vw_exception_dashboard: PriorityFlag is now
             surfaced as a plain BIT column (renamed
             IsPriority for readability) rather than a
             meaningless single-row SUM. Added IsDateCorrected
             to support audit reporting.
           - vw_route_efficiency: removed recomputed ratio
             columns; now uses EfficiencyRatio and StopVariance
             stored in fact_routes v2.0. Kept the raw stop and
             hour columns for consumers who want them.
           - vw_sales_summary: TotalRevenue removed. The
             staging column comment describes SalesAmount as
             "Total revenue for the transaction", meaning it is
             already the transaction total. Multiplying by
             UnitsSold would double-count. SalesAmount is
             surfaced as-is; Power BI can create its own
             SUM(SalesAmount) measure for rolled-up revenue.
==============================================================*/


/*==============================================================
  VIEW 1: vw_sales_summary
  Grain:    One row per sales transaction (SalesID)
  Joins:    fact_sales -> dim_product_type, dim_region, dim_date
  Use:      Transaction-level sales analysis by product,
            region, and date in Power BI
==============================================================*/
CREATE OR ALTER VIEW reporting.vw_sales_summary
AS
SELECT
    s.SalesID,                          -- Transaction identifier (PK)
    s.DeliveryID,                       -- Associated delivery
    dd.FullDate        AS SaleDate,     -- Business-readable sale date
    dd.[Year]          AS SaleYear,
    dd.Quarter         AS SaleQuarter,
    dd.[Month]         AS SaleMonth,
    dd.MonthName       AS SaleMonthName,
    dp.ProductType,                     -- Product dimension
    dr.Region,                          -- Region dimension
    s.UnitsSold,                        -- Units in this transaction
    s.SalesAmount                       -- Total revenue for this transaction
FROM dw.fact_sales          s
JOIN dw.dim_product_type    dp  ON s.ProductTypeID  = dp.ProductTypeID
JOIN dw.dim_region          dr  ON s.RegionID        = dr.RegionID
JOIN dw.dim_date            dd  ON s.DateKey          = dd.DateKey;
GO


/*==============================================================
  VIEW 2: vw_delivery_performance
  Grain:    One row per delivery (DeliveryID)
  Joins:    fact_deliveries -> dim_route, dim_driver,
            dim_shipment_type, dim_delivery_status,
            dim_priority_flag, dim_date (x2)
  Use:      Delivery tracking, on-time performance, and
            late-delivery analysis by route, driver, and date.
            ExpectedDeliveryDate is exposed so Power BI can
            compute late-delivery metrics directly.
==============================================================*/
CREATE OR ALTER VIEW reporting.vw_delivery_performance
AS
SELECT
    d.DeliveryID,                               -- Delivery identifier (PK)
    r.RouteCode         AS Route,               -- Route code from source system
    drv.DriverCode      AS Driver,              -- Driver code from source system
    st.ShipmentType,                            -- Shipment classification
    dd_actual.FullDate  AS DeliveryDate,        -- Actual delivery date
    dd_expected.FullDate AS ExpectedDeliveryDate, -- Planned delivery date
    ds.DeliveryStatus,                          -- LATE / DELIVERED / etc.
    CAST(pf.PriorityFlag AS INT) AS IsPriority  -- 1 = priority shipment
FROM dw.fact_deliveries         d
JOIN dw.dim_route               r           ON d.RouteID                 = r.RouteID
JOIN dw.dim_driver              drv         ON d.DriverID                = drv.DriverID
JOIN dw.dim_shipment_type       st          ON d.ShipmentTypeID          = st.ShipmentTypeID
JOIN dw.dim_delivery_status     ds          ON d.DeliveryStatusID        = ds.DeliveryStatusID
JOIN dw.dim_priority_flag       pf          ON d.PriorityFlagID          = pf.PriorityFlagID
JOIN dw.dim_date                dd_actual   ON d.DeliveryDateKey         = dd_actual.DateKey
JOIN dw.dim_date                dd_expected ON d.ExpectedDeliveryDateKey = dd_expected.DateKey;
GO


/*==============================================================
  VIEW 3: vw_exception_dashboard
  Grain:    One row per exception record (ExceptionID)
  Joins:    fact_exceptions -> dim_exception_type,
            dim_priority_flag, dim_region, dim_date
  Use:      Exception monitoring, resolution time analysis,
            and audit of date-corrected records.
            IsDateCorrected flags records where the clean layer
            adjusted a ResolvedDate that preceded DateReported.
==============================================================*/
CREATE OR ALTER VIEW reporting.vw_exception_dashboard
AS
SELECT
    e.ExceptionID,                              -- Exception identifier (PK)
    e.DeliveryID,                               -- Associated delivery
    et.ExceptionType,                           -- Exception category
    dd.FullDate             AS DateReported,    -- Date exception was reported
    e.ResolutionTimeHours,                      -- Hours to resolve (NULL = open)
    CAST(pf.PriorityFlag AS INT) AS IsPriority, -- 1 = critical exception
    e.IsDateCorrected,                          -- 1 = ResolvedDate was corrected
                                                --     by the clean layer; use for audit
    rg.Region                                   -- Region where exception occurred
FROM dw.fact_exceptions         e
JOIN dw.dim_exception_type      et  ON e.ExceptionTypeID = et.ExceptionTypeID
JOIN dw.dim_priority_flag       pf  ON e.PriorityFlagID  = pf.PriorityFlagID
JOIN dw.dim_region              rg  ON e.RegionID         = rg.RegionID
JOIN dw.dim_date                dd  ON e.DateKey           = dd.DateKey;
GO


/*==============================================================
  VIEW 4: vw_route_efficiency
  Grain:    One row per route + driver combination
  Joins:    fact_routes -> dim_route, dim_driver, dim_region
  Use:      Route and driver efficiency analysis.
            EfficiencyRatio and StopVariance are sourced
            directly from fact_routes (computed in the DW load
            from vw_routes v2.0) to ensure Power BI shows
            values consistent with the fact table.

  Column reference:
      EfficiencyRatio  = ActualHours / PlannedHours
                         > 1.0 = ran over schedule
                         < 1.0 = completed ahead of schedule
      StopVariance     = ActualStops - PlannedStops
                         positive = more stops than planned
                         negative = fewer stops than planned
==============================================================*/
CREATE OR ALTER VIEW reporting.vw_route_efficiency
AS
SELECT
    r.RouteCode         AS Route,           -- Route code from source system
    drv.DriverCode      AS Driver,          -- Driver code from source system
    rg.Region,                              -- Operating region
    rt.PlannedStops,                        -- Planned number of stops
    rt.ActualStops,                         -- Actual number of stops completed
    rt.PlannedHours,                        -- Planned route duration (hours)
    rt.ActualHours,                         -- Actual route duration (hours)
    rt.EfficiencyRatio,                     -- ActualHours / PlannedHours
    rt.StopVariance                         -- ActualStops - PlannedStops
FROM dw.fact_routes     rt
JOIN dw.dim_route       r   ON rt.RouteID   = r.RouteID
JOIN dw.dim_driver      drv ON rt.DriverID  = drv.DriverID
JOIN dw.dim_region      rg  ON rt.RegionID  = rg.RegionID;
GO
