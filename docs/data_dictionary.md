# Data Dictionary

This document provides a comprehensive reference for all tables, views, and columns in the Data Warehouse project. It covers **staging**, **clean**, **DW**, and **reporting layers**.

---

## 1. Staging Layer

| Table | Column | Data Type | Description |
|-------|--------|-----------|-------------|
| staging_sales | SalesID | INT | Unique identifier for a sales transaction |
| staging_sales | DeliveryID | INT | Foreign key linking to a delivery record |
| staging_sales | DateKey | DATE | Transaction date |
| staging_sales | ProductType | NVARCHAR(100) | Type/category of product sold |
| staging_sales | Region | NVARCHAR(50) | Geographic region of sale |
| staging_sales | UnitsSold | INT | Quantity sold |
| staging_sales | SalesAmount | DECIMAL(18,2) | Transaction amount |

| Table | Column | Data Type | Description |
|-------|--------|-----------|-------------|
| staging_deliveries | DeliveryID | INT | Unique delivery identifier |
| staging_deliveries | RouteID | INT | Delivery route identifier |
| staging_deliveries | DriverID | INT | Assigned driver identifier |
| staging_deliveries | Region | NVARCHAR(50) | Delivery region |
| staging_deliveries | ShipmentType | NVARCHAR(50) | Type of shipment (standard, express, etc.) |
| staging_deliveries | DeliveryDate | DATE | Actual delivery date |
| staging_deliveries | ExpectedDeliveryDate | DATE | Planned delivery date |
| staging_deliveries | DeliveryStatus | NVARCHAR(20) | Delivery status (On-Time, Late, etc.) |
| staging_deliveries | PriorityFlag | BIT | High priority delivery indicator |

| Table | Column | Data Type | Description |
|-------|--------|-----------|-------------|
| staging_exceptions | ExceptionID | INT | Unique identifier for exception events |
| staging_exceptions | DeliveryID | INT | Related delivery record |
| staging_exceptions | ExceptionType | NVARCHAR(100) | Type of exception |
| staging_exceptions | DateReported | DATE | When exception was reported |
| staging_exceptions | ResolvedDate | DATE | When exception was resolved |
| staging_exceptions | ResolutionTimeHours | DECIMAL(10,2) | Duration of resolution |
| staging_exceptions | PriorityFlag | BIT | High priority exception |
| staging_exceptions | Region | NVARCHAR(50) | Geographic region of exception |

| Table | Column | Data Type | Description |
|-------|--------|-----------|-------------|
| staging_routes | RouteID | INT | Unique route identifier |
| staging_routes | DriverID | INT | Assigned driver for route |
| staging_routes | PlannedStops | INT | Planned stops along route |
| staging_routes | ActualStops | INT | Actual stops completed |
| staging_routes | PlannedHours | DECIMAL(10,2) | Planned hours for route |
| staging_routes | ActualHours | DECIMAL(10,2) | Actual hours for route |
| staging_routes | Region | NVARCHAR(50) | Geographic region of route |

---

## 2. Clean Layer (Views)

Clean layer views **standardize, cleanse, and validate** staging data before loading into DW.

| View | Column | Description |
|------|--------|-------------|
| clean.vw_sales | SalesID | Standardized sales ID |
| clean.vw_sales | DeliveryID | Standardized delivery ID |
| clean.vw_sales | DateKey | Surrogate key date in YYYYMMDD format |
| clean.vw_sales | ProductType | Trimmed/normalized product type |
| clean.vw_sales | Region | Trimmed/normalized region |
| clean.vw_sales | UnitsSold | Enforced numeric value |
| clean.vw_sales | SalesAmount | Enforced numeric value |

| View | Column | Description |
|------|--------|-------------|
| clean.vw_deliveries | DeliveryID | Cleaned delivery ID |
| clean.vw_deliveries | RouteID | Standardized route ID |
| clean.vw_deliveries | DriverID | Standardized driver ID |
| clean.vw_deliveries | Region | Cleaned region |
| clean.vw_deliveries | ShipmentType | Standardized shipment type |
| clean.vw_deliveries | DeliveryDate | Actual delivery date |
| clean.vw_deliveries | ExpectedDeliveryDate | Expected delivery date |
| clean.vw_deliveries | DeliveryStatus | Late flagged as per business rules |
| clean.vw_deliveries | PriorityFlag | Normalized 0/1 |

| View | Column | Description |
|------|--------|-------------|
| clean.vw_exceptions | ExceptionID | Cleaned exception ID |
| clean.vw_exceptions | DeliveryID | Cleaned delivery ID |
| clean.vw_exceptions | ExceptionType | Normalized exception type |
| clean.vw_exceptions | DateReported | Standardized reporting date |
| clean.vw_exceptions | ResolvedDate | Corrected resolution date |
| clean.vw_exceptions | ResolutionTimeHours | Null for negative durations |
| clean.vw_exceptions | PriorityFlag | Normalized 0/1 |
| clean.vw_exceptions | Region | Standardized region |

| View | Column | Description |
|------|--------|-------------|
| clean.vw_routes | RouteID | Standardized route ID |
| clean.vw_routes | DriverID | Standardized driver ID |
| clean.vw_routes | PlannedStops | Validated stop count |
| clean.vw_routes | ActualStops | Validated stop count |
| clean.vw_routes | PlannedHours | Positive, non-zero |
| clean.vw_routes | ActualHours | Positive, non-zero |
| clean.vw_routes | Region | Standardized region |

---

## 3. Data Warehouse Layer (DW)

### Dimension Tables

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_date | DateKey | Primary key, YYYYMMDD |
| dw.dim_date | FullDate | Actual calendar date |
| dw.dim_date | Year | Calendar year |
| dw.dim_date | Quarter | Calendar quarter |
| dw.dim_date | Month | Month number |
| dw.dim_date | Day | Day of month |
| dw.dim_date | Weekday | Numeric weekday (1-7) |
| dw.dim_date | IsWeekend | Flag for weekends (1 = weekend, 0 = weekday) |
| dw.dim_date | MonthName | Text month name |
| dw.dim_date | IsHoliday | Flag for holidays (placeholder) |
| dw.dim_date | DayName | Name of weekday |
| dw.dim_date | WeekOfYear | Week number |
| dw.dim_date | MonthYear | MM-YYYY format |
| dw.dim_date | YearMonth | YYYY-MM format |
| dw.dim_date | FiscalYear | Fiscal year |

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_product_type | ProductTypeID | Surrogate key |
| dw.dim_product_type | ProductType | Product type name |

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_region | RegionID | Surrogate key |
| dw.dim_region | Region | Region name |

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_driver | DriverID | Surrogate key |
| dw.dim_driver | DriverName | Driver name |

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_route | RouteID | Surrogate key |
| dw.dim_route | RouteName | Route identifier or name |

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_shipment_type | ShipmentTypeID | Surrogate key |
| dw.dim_shipment_type | ShipmentType | Type of shipment |

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_delivery_status | DeliveryStatusID | Surrogate key |
| dw.dim_delivery_status | DeliveryStatus | Delivery status description |

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_exception_type | ExceptionTypeID | Surrogate key |
| dw.dim_exception_type | ExceptionType | Type of exception |

| Table | Column | Description |
|-------|--------|-------------|
| dw.dim_priority_flag | PriorityFlagID | Surrogate key |
| dw.dim_priority_flag | PriorityFlag | Priority indicator (0 = normal, 1 = high) |

---

### Fact Tables

| Table | Column | Description |
|-------|--------|-------------|
| dw.fact_sales | SalesID | Original sales transaction ID |
| dw.fact_sales | DeliveryID | Linked delivery ID |
| dw.fact_sales | DateKey | Foreign key to dim_date |
| dw.fact_sales | ProductTypeID | Foreign key to dim_product_type |
| dw.fact_sales | RegionID | Foreign key to dim_region |
| dw.fact_sales | UnitsSold | Quantity sold |
| dw.fact_sales | SalesAmount | Total sales amount |

| Table | Column | Description |
|-------|--------|-------------|
| dw.fact_deliveries | DeliveryID | Delivery transaction ID |
| dw.fact_deliveries | RouteID | Foreign key to dim_route |
| dw.fact_deliveries | DriverID | Foreign key to dim_driver |
| dw.fact_deliveries | ShipmentTypeID | Foreign key to dim_shipment_type |
| dw.fact_deliveries | DateKey | Foreign key to dim_date |
| dw.fact_deliveries | DeliveryStatusID | Foreign key to dim_delivery_status |
| dw.fact_deliveries | PriorityFlagID | Foreign key to dim_priority_flag |

| Table | Column | Description |
|-------|--------|-------------|
| dw.fact_routes | RouteID | Foreign key to dim_route |
| dw.fact_routes | DriverID | Foreign key to dim_driver |
| dw.fact_routes | PlannedStops | Planned number of stops |
| dw.fact_routes | ActualStops | Actual number of stops |
| dw.fact_routes | PlannedHours | Planned hours for route |
| dw.fact_routes | ActualHours | Actual hours for route |
| dw.fact_routes | RegionID | Foreign key to dim_region |

| Table | Column | Description |
|-------|--------|-------------|
| dw.fact_exceptions | ExceptionID | Original exception ID |
| dw.fact_exceptions | DeliveryID | Linked delivery ID |
| dw.fact_exceptions | ExceptionTypeID | Foreign key to dim_exception_type |
| dw.fact_exceptions | DateKey | Foreign key to dim_date |
| dw.fact_exceptions | ResolutionTimeHours | Resolution time in hours |
| dw.fact_exceptions | PriorityFlagID | Foreign key to dim_priority_flag |
| dw.fact_exceptions | RegionID | Foreign key to dim_region |

---

## 4. Reporting Layer

| View | Column | Description |
|------|--------|-------------|
| reporting.vw_sales_summary | ProductType | Aggregated by product type |
| reporting.vw_sales_summary | TotalSales | Aggregated metric |
| reporting.vw_delivery_metrics | DeliveryStatus | Aggregated by status |
| reporting.vw_delivery_metrics | LateDeliveries | Metric count |

---

**Note:** This dictionary is **living documentation**. Update whenever new columns, tables, or views are added.