# Data Dictionary
**Project:** FedEx Operations Data Warehouse
**Version:** 2.0
**Last Updated:** 2026

This document is the authoritative column-level reference for all tables and views across the four pipeline layers: **staging**, **clean**, **dw**, and **reporting**. It reflects the v2.0 schema produced by the full ETL project review.

> **Living Document:** Update this file whenever a column, table, or view is added, renamed, or removed. Keep it in sync with the SQL scripts — particularly `02_etl_staging_setup.sql`, `04_clean_layer_views_v2_0.sql`, `08_dw_load.sql`, and `12_reporting_views.sql`.

---

## Table of Contents

1. [Staging Layer](#1-staging-layer)
2. [Clean Layer (Views)](#2-clean-layer-views)
3. [Data Warehouse Layer](#3-data-warehouse-layer)
   - [Dimension Tables](#dimension-tables)
   - [Fact Tables](#fact-tables)
   - [ETL Logging Tables](#etl-logging-tables)
4. [Reporting Layer (Views)](#4-reporting-layer-views)

---

## 1. Staging Layer

Staging tables receive raw data loaded directly from CSV source files via `BULK INSERT`. No business rules are applied here. All columns reflect the source data as closely as possible, with explicit `NOT NULL` constraints on required fields.

**Schema:** `staging`
**Populated by:** `02_etl_staging_setup.sql`

---

### staging.staging_sales

Primary key: `SalesID`

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| SalesID | INT | NOT NULL | Unique identifier for a sales transaction (PK) |
| DeliveryID | INT | NOT NULL | Links the sale to its associated delivery record |
| DateKey | DATE | NOT NULL | Transaction date; converted to INT (YYYYMMDD) during DW load |
| ProductType | NVARCHAR(50) | NOT NULL | Category of product sold (e.g. Standard, Express) |
| Region | NVARCHAR(10) | NOT NULL | Geographic region code of the sale |
| UnitsSold | INT | NOT NULL | Number of units in this transaction |
| SalesAmount | DECIMAL(10,2) | NOT NULL | Total revenue for this transaction (not per-unit price) |

---

### staging.staging_deliveries

Primary key: `DeliveryID`

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| DeliveryID | INT | NOT NULL | Unique delivery record identifier (PK) |
| RouteID | NVARCHAR(10) | NOT NULL | Source system route identifier |
| DriverID | NVARCHAR(50) | NOT NULL | Source system driver identifier |
| Region | NVARCHAR(10) | NOT NULL | Geographic region of the delivery |
| ShipmentType | NVARCHAR(20) | NOT NULL | Classification of shipment (Standard, Express, etc.) |
| DeliveryDate | DATE | NOT NULL | Actual date the delivery was completed |
| ExpectedDeliveryDate | DATE | NULL | Planned delivery date; NULL if not scheduled |
| DeliveryStatus | NVARCHAR(20) | NOT NULL | Raw source status (Delivered, Delayed, Failed, etc.) |
| PriorityFlag | BIT | NOT NULL | 1 = high priority shipment, 0 = standard |

---

### staging.staging_exceptions

Primary key: `ExceptionID`

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| ExceptionID | INT | NOT NULL | Unique exception record identifier (PK) |
| DeliveryID | INT | NOT NULL | Links the exception to its associated delivery |
| ExceptionType | NVARCHAR(50) | NOT NULL | Category of exception (Delay, Damage, Weather, etc.) |
| DateReported | DATE | NOT NULL | Date the exception was first reported |
| ResolvedDate | DATE | NULL | Date the exception was resolved; NULL = still open |
| ResolutionTimeHours | DECIMAL(6,2) | NULL | Hours taken to resolve; DECIMAL supports fractional hours |
| PriorityFlag | BIT | NOT NULL | 1 = critical exception, 0 = standard |
| Region | NVARCHAR(10) | NOT NULL | Geographic region where the exception occurred |

---

### staging.staging_routes

Primary key: `(RouteID, DriverID)` — composite

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| RouteID | NVARCHAR(10) | NOT NULL | Source system route identifier (part of composite PK) |
| DriverID | NVARCHAR(50) | NOT NULL | Source system driver identifier (part of composite PK) |
| PlannedStops | INT | NOT NULL | Number of stops planned for this route |
| ActualStops | INT | NOT NULL | Number of stops actually completed |
| PlannedHours | DECIMAL(5,2) | NOT NULL | Estimated duration of the route in hours |
| ActualHours | DECIMAL(5,2) | NOT NULL | Actual duration of the route in hours |
| Region | NVARCHAR(10) | NOT NULL | Geographic region the route operates in |

---

## 2. Clean Layer (Views)

Clean layer views select from the staging tables and apply standardization, business rules, and quality flags. They do **not** store data — they are read-only SQL views. All text is uppercased and trimmed. Invalid records are filtered. Derived quality flags are added.

**Schema:** `clean`
**Defined in:** `04_clean_layer_views_v2_0.sql`

---

### clean.vw_sales

Source: `staging.staging_sales`
Grain: One row per valid sales transaction.

| Column | Description |
|--------|-------------|
| SalesID | Sales transaction identifier; passed through from staging |
| DeliveryID | Associated delivery identifier; passed through from staging |
| DateKey | Source DATE value; validated to be within 1900-01-01 to 2100-12-31. Set to NULL if out of range. Converted to INT (YYYYMMDD) during DW load |
| IsBadDateKey | 1 = DateKey was out of range and set to NULL; 0 = valid. Rows with IsBadDateKey = 1 are retained for audit; the DW load filters on IsBadDateKey = 0 |
| ProductType | UPPER(LTRIM(RTRIM())) applied; prevents duplicate dimension values from case/whitespace variation |
| Region | UPPER(LTRIM(RTRIM())) applied |
| UnitsSold | Passed through directly; staging column is INT. Rows with UnitsSold <= 0 are excluded |
| SalesAmount | Passed through directly; staging column is DECIMAL(10,2). Rows with SalesAmount <= 0 are excluded |

**Filters applied:** SalesID NOT NULL, DeliveryID NOT NULL, DateKey NOT NULL, UnitsSold > 0, SalesAmount > 0.

---

### clean.vw_deliveries

Source: `staging.staging_deliveries`
Grain: One row per valid delivery record.

| Column | Description |
|--------|-------------|
| DeliveryID | Delivery identifier; passed through |
| RouteID | Route code; passed through |
| DriverID | Driver code; passed through |
| Region | UPPER(LTRIM(RTRIM())) applied |
| ShipmentType | UPPER(LTRIM(RTRIM())) applied |
| DeliveryDate | Actual delivery date; passed through |
| ExpectedDeliveryDate | Planned delivery date; passed through |
| DeliveryStatus | Business rule applied: if DeliveryDate > ExpectedDeliveryDate the value is forced to `'LATE'` regardless of source value. Otherwise the source value is UPPER/TRIM normalized. **All downstream comparisons must use `'LATE'` (all caps)** |
| PriorityFlag | Normalized to strict 0 or 1 INT. BIT column direct comparison; no VARCHAR casting needed |

**Filters applied:** DeliveryID, RouteID, DriverID, DeliveryDate, ExpectedDeliveryDate all NOT NULL.

---

### clean.vw_exceptions

Source: `staging.staging_exceptions`
Grain: One row per valid exception record.

| Column | Description |
|--------|-------------|
| ExceptionID | Exception identifier; passed through |
| DeliveryID | Associated delivery identifier; passed through |
| ExceptionType | UPPER(LTRIM(RTRIM())) applied |
| DateReported | Reporting date; passed through |
| ResolvedDate | Chronology-corrected: if source ResolvedDate precedes DateReported, it is set to DateReported as the floor. NULL = exception still open |
| IsDateCorrected | **Added in v2.0.** 1 = ResolvedDate was out of chronological order and was corrected by this view; 0 = date is as-sourced or NULL. Use this column to identify and audit corrected records |
| ResolutionTimeHours | If stored value >= 0, used as-is. If negative but both dates are valid, derived from DATEDIFF(HOUR, DateReported, ResolvedDate). NULL if no valid derivation is possible |
| PriorityFlag | Normalized to strict 0 or 1 INT |
| Region | UPPER(LTRIM(RTRIM())) applied |

**Filters applied:** ExceptionID, DeliveryID, DateReported all NOT NULL. ResolvedDate NULL is permitted (open exception).

---

### clean.vw_routes

Source: `staging.staging_routes`
Grain: One row per valid route + driver combination.

| Column | Description |
|--------|-------------|
| RouteID | Route identifier; passed through |
| DriverID | Driver identifier; passed through |
| PlannedStops | Passed through; rows with PlannedStops <= 0 are excluded |
| ActualStops | Passed through; rows with ActualStops <= 0 are excluded |
| PlannedHours | Passed through; rows with PlannedHours <= 0 are excluded |
| ActualHours | Passed through; rows with ActualHours <= 0 are excluded |
| Region | UPPER(LTRIM(RTRIM())) applied |
| EfficiencyRatio | **Added in v2.0.** ROUND(ActualHours / PlannedHours, 4). Values > 1.0 = ran over schedule; < 1.0 = completed ahead of schedule |
| StopVariance | **Added in v2.0.** ActualStops - PlannedStops. Positive = more stops than planned; negative = fewer |

**Filters applied:** RouteID, DriverID NOT NULL; PlannedStops, ActualStops, PlannedHours, ActualHours all > 0.

---

## 3. Data Warehouse Layer

The DW layer implements a star schema. Dimension tables hold descriptive attributes with IDENTITY surrogate keys. Fact tables hold measurable metrics and reference dimensions via foreign keys.

**Schema:** `dw`
**Populated by:** `08_dw_load.sql`

---

### Dimension Tables

#### dw.dim_date

Primary key: `DateKey` (INT, YYYYMMDD format)
Source: All DATE columns across `clean.vw_sales`, `clean.vw_deliveries`, `clean.vw_exceptions`

| Column | Data Type | Description |
|--------|-----------|-------------|
| DateKey | INT | Primary key in YYYYMMDD integer format (e.g. 20241231) |
| FullDate | DATE | Full calendar date value |
| Year | INT | Calendar year |
| Quarter | INT | Calendar quarter (1–4) |
| Month | INT | Month number (1–12) |
| Day | INT | Day of month (1–31) |
| Weekday | INT | Day of week (1 = Sunday … 7 = Saturday, per @@DATEFIRST default) |
| IsWeekend | BIT | 1 = Saturday or Sunday; 0 = weekday |
| MonthName | NVARCHAR(20) | Full month name (e.g. 'January') |
| DayName | NVARCHAR(20) | Full weekday name (e.g. 'Monday') |
| WeekOfYear | INT | ISO week number within the calendar year |
| MonthYear | NVARCHAR(7) | 'YYYY-MM' format (e.g. '2024-01') |
| YearMonth | NVARCHAR(6) | 'YYYYMM' format (e.g. '202401') |
| FiscalYear | INT | Fiscal year; currently set equal to calendar year (placeholder) |
| IsHoliday | BIT | 1 = public holiday; 0 = normal day. Placeholder — update from a holiday reference table |

---

#### dw.dim_product_type

Primary key: `ProductTypeID` (IDENTITY surrogate)

| Column | Data Type | Description |
|--------|-----------|-------------|
| ProductTypeID | INT | Surrogate key (IDENTITY) |
| ProductType | NVARCHAR(100) | Product category name; sourced from `clean.vw_sales` |

---

#### dw.dim_region

Primary key: `RegionID` (IDENTITY surrogate)

| Column | Data Type | Description |
|--------|-----------|-------------|
| RegionID | INT | Surrogate key (IDENTITY) |
| Region | NVARCHAR(50) | Region name; sourced from all four clean views |

---

#### dw.dim_driver

Primary key: `DriverID` (IDENTITY surrogate)

> **v2.0 rename:** Column was `DriverName` in v1.0. Renamed to `DriverCode` to accurately reflect that this column stores the source system identifier code, not a human-readable display name.

| Column | Data Type | Description |
|--------|-----------|-------------|
| DriverID | INT | Surrogate key (IDENTITY) |
| DriverCode | NVARCHAR(50) | Source system driver identifier from staging (e.g. 'DRV-047') |

---

#### dw.dim_route

Primary key: `RouteID` (IDENTITY surrogate)

> **v2.0 rename:** Column was `RouteName` in v1.0. Renamed to `RouteCode` to accurately reflect that this column stores the source system identifier code, not a display name.

| Column | Data Type | Description |
|--------|-----------|-------------|
| RouteID | INT | Surrogate key (IDENTITY) |
| RouteCode | NVARCHAR(10) | Source system route identifier from staging (e.g. 'RT-042') |

---

#### dw.dim_shipment_type

Primary key: `ShipmentTypeID` (IDENTITY surrogate)

| Column | Data Type | Description |
|--------|-----------|-------------|
| ShipmentTypeID | INT | Surrogate key (IDENTITY) |
| ShipmentType | NVARCHAR(50) | Shipment classification (Standard, Express, etc.) |

---

#### dw.dim_delivery_status

Primary key: `DeliveryStatusID` (IDENTITY surrogate)

| Column | Data Type | Description |
|--------|-----------|-------------|
| DeliveryStatusID | INT | Surrogate key (IDENTITY) |
| DeliveryStatus | NVARCHAR(50) | Delivery status value. **Canonical late value is `'LATE'` (all caps)** — all downstream comparisons must use this exact string |

---

#### dw.dim_exception_type

Primary key: `ExceptionTypeID` (IDENTITY surrogate)

| Column | Data Type | Description |
|--------|-----------|-------------|
| ExceptionTypeID | INT | Surrogate key (IDENTITY) |
| ExceptionType | NVARCHAR(100) | Exception category (Delay, Damage, Weather, etc.) |

---

#### dw.dim_priority_flag

Primary key: `PriorityFlagID` (IDENTITY surrogate)

| Column | Data Type | Description |
|--------|-----------|-------------|
| PriorityFlagID | INT | Surrogate key (IDENTITY) |
| PriorityFlag | BIT | 0 = standard priority; 1 = high priority |

---

### Fact Tables

#### dw.fact_sales

Primary key: `SalesID`
Grain: One row per sales transaction.

| Column | Data Type | Description |
|--------|-----------|-------------|
| SalesID | INT | Natural key from source; primary key of this fact table |
| DeliveryID | INT | Business key linking this sale to its delivery (not a FK to fact_deliveries) |
| DateKey | INT | FK → dw.dim_date; YYYYMMDD format |
| ProductTypeID | INT | FK → dw.dim_product_type |
| RegionID | INT | FK → dw.dim_region |
| UnitsSold | INT | Number of units sold in this transaction |
| SalesAmount | DECIMAL(18,2) | Total revenue for this transaction |

---

#### dw.fact_deliveries

Primary key: `DeliveryID`
Grain: One row per delivery.

> **v2.0 change:** Single `DateKey` replaced by two separate date surrogate keys — `DeliveryDateKey` and `ExpectedDeliveryDateKey` — to enable late-delivery analysis by both actual and planned date.

| Column | Data Type | Description |
|--------|-----------|-------------|
| DeliveryID | INT | Natural key from source; primary key of this fact table |
| RouteID | INT | FK → dw.dim_route |
| DriverID | INT | FK → dw.dim_driver |
| ShipmentTypeID | INT | FK → dw.dim_shipment_type |
| DeliveryDateKey | INT | FK → dw.dim_date; the actual delivery date in YYYYMMDD format |
| ExpectedDeliveryDateKey | INT | FK → dw.dim_date; the planned delivery date in YYYYMMDD format |
| DeliveryStatusID | INT | FK → dw.dim_delivery_status |
| PriorityFlagID | INT | FK → dw.dim_priority_flag |

---

#### dw.fact_routes

Primary key: `(RouteID, DriverID)` — composite
Grain: One row per route + driver combination.

> **v2.0 additions:** `EfficiencyRatio` and `StopVariance` added; sourced from `clean.vw_routes` derived columns.

| Column | Data Type | Description |
|--------|-----------|-------------|
| RouteID | INT | FK → dw.dim_route; part of composite PK |
| DriverID | INT | FK → dw.dim_driver; part of composite PK |
| PlannedStops | INT | Planned number of stops on this route |
| ActualStops | INT | Actual number of stops completed |
| PlannedHours | DECIMAL(10,2) | Planned route duration in hours |
| ActualHours | DECIMAL(10,2) | Actual route duration in hours |
| RegionID | INT | FK → dw.dim_region |
| EfficiencyRatio | DECIMAL(10,4) | ActualHours / PlannedHours. > 1.0 = over schedule; < 1.0 = ahead of schedule |
| StopVariance | INT | ActualStops - PlannedStops. Positive = more stops than planned |

---

#### dw.fact_exceptions

Primary key: `ExceptionID`
Grain: One row per exception record.

> **v2.0 addition:** `IsDateCorrected` flag carried through from `clean.vw_exceptions`.

| Column | Data Type | Description |
|--------|-----------|-------------|
| ExceptionID | INT | Natural key from source; primary key of this fact table |
| DeliveryID | INT | Business key linking this exception to its delivery |
| ExceptionTypeID | INT | FK → dw.dim_exception_type |
| DateKey | INT | FK → dw.dim_date; DateReported converted to YYYYMMDD INT |
| ResolutionTimeHours | DECIMAL(10,2) | Hours to resolve; NULL = open exception or unresolvable duration |
| IsDateCorrected | BIT | 1 = the clean layer corrected an out-of-order ResolvedDate; 0 = as-sourced |
| PriorityFlagID | INT | FK → dw.dim_priority_flag |
| RegionID | INT | FK → dw.dim_region |

---

### ETL Logging Tables

**Schema:** `dw`
**Defined in:** `13_etl_logging_setup.sql`

#### dw.etl_run_log

Primary key: `RunID`

| Column | Data Type | Description |
|--------|-----------|-------------|
| RunID | INT | Surrogate key (IDENTITY); returned via OUTPUT parameter by `usp_start_etl_run` |
| PipelineName | NVARCHAR(200) | Name of the pipeline being logged (e.g. 'DW Full Load') |
| StartTime | DATETIME2 | UTC timestamp when the run was opened |
| EndTime | DATETIME2 | UTC timestamp when the run was closed; NULL while run is in progress |
| Status | VARCHAR(20) | Constrained to: RUNNING, SUCCESS, FAILED, SKIPPED |
| ErrorMessage | NVARCHAR(MAX) | Error detail if Status = 'FAILED'; NULL otherwise |

---

#### dw.etl_step_log

Primary key: `StepLogID`
FK: `RunID` → `dw.etl_run_log(RunID)`

| Column | Data Type | Description |
|--------|-----------|-------------|
| StepLogID | INT | Surrogate key (IDENTITY) |
| RunID | INT | FK to the parent run record |
| StepName | VARCHAR(200) | Descriptive step name (e.g. 'Load fact_sales') |
| StartTime | DATETIME2 | UTC timestamp captured by caller before the step began |
| EndTime | DATETIME2 | UTC timestamp set by `usp_log_etl_step` at insert time |
| DurationSec | Computed | DATEDIFF(SECOND, StartTime, EndTime) — computed column, no storage cost |
| RowsProcessed | INT | @@ROWCOUNT from the ETL step; NULL if not applicable |
| Status | VARCHAR(20) | Constrained to: RUNNING, SUCCESS, FAILED, SKIPPED |
| ErrorMessage | NVARCHAR(MAX) | Error detail if Status = 'FAILED'; NULL otherwise |

---

## 4. Reporting Layer (Views)

Reporting views are flat, row-level joins over DW fact and dimension tables. They expose business-readable column names and carry pre-computed metrics. They contain **no GROUP BY or aggregations** — each view has the same grain as its source fact table (one row per fact row). Roll-ups and measures are handled in Power BI.

**Schema:** `reporting`
**Defined in:** `12_reporting_views.sql`

---

### reporting.vw_sales_summary

Source: `dw.fact_sales` + `dim_product_type`, `dim_region`, `dim_date`
Grain: One row per sales transaction (SalesID).

| Column | Description |
|--------|-------------|
| SalesID | Transaction identifier |
| DeliveryID | Associated delivery identifier |
| SaleDate | Full calendar date from dim_date (FullDate) |
| SaleYear | Calendar year from dim_date |
| SaleQuarter | Calendar quarter from dim_date |
| SaleMonth | Month number from dim_date |
| SaleMonthName | Month name from dim_date |
| ProductType | Product category from dim_product_type |
| Region | Region name from dim_region |
| UnitsSold | Number of units in this transaction |
| SalesAmount | Total revenue for this transaction |

---

### reporting.vw_delivery_performance

Source: `dw.fact_deliveries` + `dim_route`, `dim_driver`, `dim_shipment_type`, `dim_delivery_status`, `dim_priority_flag`, `dim_date` (x2)
Grain: One row per delivery (DeliveryID).

> **v2.0 changes:** Removed vacuous GROUP BY/COUNT/SUM. Added ExpectedDeliveryDate for late-delivery analysis. Column references updated from `RouteName`/`DriverName` to `RouteCode`/`DriverCode`.

| Column | Description |
|--------|-------------|
| DeliveryID | Delivery identifier |
| Route | Source route code from dim_route (RouteCode) |
| Driver | Source driver code from dim_driver (DriverCode) |
| ShipmentType | Shipment classification |
| DeliveryDate | Actual delivery date (from DeliveryDateKey → dim_date) |
| ExpectedDeliveryDate | Planned delivery date (from ExpectedDeliveryDateKey → dim_date) |
| DeliveryStatus | Delivery status; 'LATE' for late deliveries |
| IsPriority | 1 = high priority shipment; 0 = standard (CAST of BIT PriorityFlag) |

---

### reporting.vw_exception_dashboard

Source: `dw.fact_exceptions` + `dim_exception_type`, `dim_priority_flag`, `dim_region`, `dim_date`
Grain: One row per exception record (ExceptionID).

> **v2.0 changes:** Removed vacuous GROUP BY/SUM. Replaced `PriorityExceptions` aggregate with `IsPriority` flag. Added `IsDateCorrected` for audit reporting.

| Column | Description |
|--------|-------------|
| ExceptionID | Exception identifier |
| DeliveryID | Associated delivery identifier |
| ExceptionType | Exception category from dim_exception_type |
| DateReported | Date the exception was reported (from dim_date) |
| ResolutionTimeHours | Hours to resolve; NULL = open exception |
| IsPriority | 1 = critical exception; 0 = standard |
| IsDateCorrected | 1 = clean layer corrected an out-of-order ResolvedDate; useful for audit filtering |
| Region | Region name from dim_region |

---

### reporting.vw_route_efficiency

Source: `dw.fact_routes` + `dim_route`, `dim_driver`, `dim_region`
Grain: One row per route + driver combination.

> **v2.0 changes:** Removed vacuous GROUP BY/SUM. Replaced recomputed ratio formulas with `EfficiencyRatio` and `StopVariance` stored directly in fact_routes.

| Column | Description |
|--------|-------------|
| Route | Source route code from dim_route (RouteCode) |
| Driver | Source driver code from dim_driver (DriverCode) |
| Region | Region name from dim_region |
| PlannedStops | Planned number of stops |
| ActualStops | Actual number of stops completed |
| PlannedHours | Planned route duration in hours |
| ActualHours | Actual route duration in hours |
| EfficiencyRatio | ActualHours / PlannedHours from fact_routes; > 1.0 = over schedule |
| StopVariance | ActualStops - PlannedStops from fact_routes; positive = more stops than planned |

---

*Update this document whenever the schema changes. Treat a column rename as a breaking change and update all layers simultaneously.*
