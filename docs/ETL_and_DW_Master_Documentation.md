# Master Data Warehouse Documentation

This document describes the full ETL pipeline, including table definitions, views, data lineage, and data flow. It is intended for developers, analysts, and BI stakeholders.

---

## 1️. Objective

**Data Engineering (DW Build):**  
- Create a robust, validated Data Warehouse (DW) from raw CSV sources.  
- Enforce referential integrity using surrogate keys.  
- Provide a trusted source for reporting and analytics.

**Business Intelligence (Analytics & Reporting):**  
- Enable standardized reporting with facts and dimensions.  
- Power BI dashboards and metrics rely on the DW.  
- Maintain traceable lineage for audit and compliance.

---

## 2️. ETL Pipeline Overview

Raw CSV (dbo)
│
▼
Staging Tables (staging schema)
│
▼
Clean Views (clean schema)
│
▼
DW Tables (dw schema) → Dimensions & Facts
│
▼
Reporting Views (reporting schema)
│
▼
Power BI / Analytics


---

## 3️. Staging Layer

| Table | Column | Description |
|-------|--------|-------------|
| staging.staging_sales | SalesID | Unique sales transaction ID |
| staging.staging_sales | DeliveryID | FK to delivery |
| staging.staging_sales | DateKey | Raw date from source |
| staging.staging_sales | ProductType | Product category |
| staging.staging_sales | Region | Sales region |
| staging.staging_sales | UnitsSold | Quantity sold |
| staging.staging_sales | SalesAmount | Total sale value |
| staging.staging_deliveries | DeliveryID | Unique delivery ID |
| staging.staging_deliveries | RouteID | FK to route |
| staging.staging_deliveries | DriverID | FK to driver |
| staging.staging_deliveries | Region | Delivery region |
| staging.staging_deliveries | ShipmentType | Type of shipment |
| staging.staging_deliveries | DeliveryDate | Actual delivery date |
| staging.staging_deliveries | ExpectedDeliveryDate | Planned delivery date |
| staging.staging_deliveries | DeliveryStatus | Status of delivery |
| staging.staging_deliveries | PriorityFlag | Priority indicator |
| staging.staging_routes | RouteID | Unique route ID |
| staging.staging_routes | DriverID | Driver assignment |
| staging.staging_routes | PlannedStops | Planned number of stops |
| staging.staging_routes | ActualStops | Actual number of stops |
| staging.staging_routes | PlannedHours | Planned hours |
| staging.staging_routes | ActualHours | Actual hours |
| staging.staging_routes | Region | Route region |
| staging.staging_exceptions | ExceptionID | Unique exception ID |
| staging.staging_exceptions | DeliveryID | FK to delivery |
| staging.staging_exceptions | ExceptionType | Type of exception |
| staging.staging_exceptions | DateReported | Reported date |
| staging.staging_exceptions | ResolvedDate | Resolution date |
| staging.staging_exceptions | ResolutionTimeHours | Duration of resolution |
| staging.staging_exceptions | PriorityFlag | Priority indicator |
| staging.staging_exceptions | Region | Region of exception |

---

## 4️. Clean Layer Views

| View | Column | Description |
|------|--------|-------------|
| clean.vw_sales | SalesID | Unique sales transaction |
| clean.vw_sales | DeliveryID | FK to delivery |
| clean.vw_sales | DateKey | YYYYMMDD, surrogate-ready |
| clean.vw_sales | ProductType | Trimmed, standardized |
| clean.vw_sales | Region | Trimmed, standardized |
| clean.vw_sales | UnitsSold | Numeric, positive |
| clean.vw_sales | SalesAmount | Decimal, positive |
| clean.vw_deliveries | DeliveryID | Unique delivery |
| clean.vw_deliveries | RouteID | FK route surrogate |
| clean.vw_deliveries | DriverID | FK driver surrogate |
| clean.vw_deliveries | Region | Standardized |
| clean.vw_deliveries | ShipmentType | Standardized |
| clean.vw_deliveries | DeliveryDate | Actual date |
| clean.vw_deliveries | ExpectedDeliveryDate | Planned date |
| clean.vw_deliveries | DeliveryStatus | 'Late' flagged if overdue |
| clean.vw_deliveries | PriorityFlag | Normalized 0/1 |
| clean.vw_routes | RouteID | Unique route |
| clean.vw_routes | DriverID | Assigned driver |
| clean.vw_routes | PlannedStops | Non-negative |
| clean.vw_routes | ActualStops | Non-negative |
| clean.vw_routes | PlannedHours | >0 |
| clean.vw_routes | ActualHours | >0 |
| clean.vw_routes | Region | Standardized |
| clean.vw_exceptions | ExceptionID | Unique exception |
| clean.vw_exceptions | DeliveryID | FK delivery |
| clean.vw_exceptions | ExceptionType | Standardized |
| clean.vw_exceptions | DateReported | Reported date |
| clean.vw_exceptions | ResolvedDate | Corrected for invalid dates |
| clean.vw_exceptions | ResolutionTimeHours | Null if negative |
| clean.vw_exceptions | PriorityFlag | Normalized 0/1 |
| clean.vw_exceptions | Region | Standardized |

---

## 5️. Data Warehouse (DW) Layer

### Dimensions

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_date | DateKey | Primary key, YYYYMMDD |
| dw.dim_date | FullDate | Actual calendar date |
| dw.dim_date | Year | Calendar year |
| dw.dim_date | Quarter | Quarter number |
| dw.dim_date | Month | Month number |
| dw.dim_date | Day | Day of month |
| dw.dim_date | Weekday | Numeric weekday |
| dw.dim_date | IsWeekend | Weekend flag |
| dw.dim_date | MonthName | Text month |
| dw.dim_date | WeekOfYear | Week number |
| dw.dim_date | FiscalYear | Fiscal year |
| dw.dim_product_type | ProductTypeID | PK |
| dw.dim_product_type | ProductType | Product name |
| dw.dim_region | RegionID | PK |
| dw.dim_region | Region | Name |
| dw.dim_driver | DriverID | PK |
| dw.dim_driver | DriverName | Name |
| dw.dim_route | RouteID | PK |
| dw.dim_route | RouteName | Name |
| dw.dim_shipment_type | ShipmentTypeID | PK |
| dw.dim_shipment_type | ShipmentType | Name |
| dw.dim_delivery_status | DeliveryStatusID | PK |
| dw.dim_delivery_status | DeliveryStatus | Name |
| dw.dim_exception_type | ExceptionTypeID | PK |
| dw.dim_exception_type | ExceptionType | Name |
| dw.dim_priority_flag | PriorityFlagID | PK |
| dw.dim_priority_flag | PriorityFlag | 0/1 |

### Fact Tables

| Table | Column | Description |
|-------|--------|-------------|
| dw.fact_sales | SalesID | Source sales ID |
| dw.fact_sales | DeliveryID | FK |
| dw.fact_sales | DateKey | FK dim_date |
| dw.fact_sales | ProductTypeID | FK dim_product_type |
| dw.fact_sales | RegionID | FK dim_region |
| dw.fact_sales | UnitsSold | Quantity |
| dw.fact_sales | SalesAmount | Value |
| dw.fact_deliveries | DeliveryID | Source delivery ID |
| dw.fact_deliveries | RouteID | FK dim_route |
| dw.fact_deliveries | DriverID | FK dim_driver |
| dw.fact_deliveries | ShipmentTypeID | FK dim_shipment_type |
| dw.fact_deliveries | DateKey | FK dim_date |
| dw.fact_deliveries | DeliveryStatusID | FK dim_delivery_status |
| dw.fact_deliveries | PriorityFlagID | FK dim_priority_flag |
| dw.fact_routes | RouteID | FK dim_route |
| dw.fact_routes | DriverID | FK dim_driver |
| dw.fact_routes | PlannedStops | Numeric |
| dw.fact_routes | ActualStops | Numeric |
| dw.fact_routes | PlannedHours | Numeric |
| dw.fact_routes | ActualHours | Numeric |
| dw.fact_routes | RegionID | FK dim_region |
| dw.fact_exceptions | ExceptionID | Source exception |
| dw.fact_exceptions | DeliveryID | FK |
| dw.fact_exceptions | ExceptionTypeID | FK dim_exception_type |
| dw.fact_exceptions | DateKey | FK dim_date |
| dw.fact_exceptions | ResolutionTimeHours | Duration |
| dw.fact_exceptions | PriorityFlagID | FK dim_priority_flag |
| dw.fact_exceptions | RegionID | FK dim_region |

---

## 6️. Reporting Layer

| Object | Source | Purpose |
|--------|--------|---------|
| reporting.vw_sales_summary | DW fact_sales, dim_product_type, dim_region, dim_date | Aggregated sales metrics and revenue for Power BI dashboards; business-friendly columns and calculations |
| reporting.vw_delivery_performance | DW fact_deliveries, dim_route, dim_driver, dim_shipment_type, dim_delivery_status, dim_priority_flag, dim_date | KPIs for delivery operations; includes late deliveries, priority delivery counts, total deliveries per route and driver |
| reporting.vw_exception_dashboard | DW fact_exceptions, dim_exception_type, dim_priority_flag, dim_region, dim_date | Tracks exceptions and resolution times; aggregates priority exceptions and provides regional breakdowns |
| reporting.vw_route_efficiency | DW fact_routes, dim_route, dim_driver, dim_region | Evaluates route performance and efficiency; computes stop completion ratio and hour efficiency ratio per route and driver |

---

## 7️. Data Lineage Overview

Raw CSV (dbo)
│
▼
Staging Tables (staging schema)
│
▼
Clean Views (clean schema)
│
▼
DW Tables (dw schema) → Dimensions & Facts
│
▼
Reporting Views (reporting schema)
│
▼
Power BI / Analytics


**Key Points:**
- Data quality enforced at clean layer (business rules, NULL checks).
- Surrogate keys in DW prevent FK violations.
- Reporting layer depends entirely on DW.
- All transformations documented via clean views and DW load scripts.

---

## 8️. Notes

- This documentation should be updated whenever new tables, views, or transformations are added.
- Optional metrics and profiling queries can be included for audit purposes.