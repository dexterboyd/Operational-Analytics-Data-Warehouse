# ETL and Data Warehouse Master Documentation
**Project:** FedEx Operations Data Warehouse
**Version:** 2.0
**Last Updated:** 2026

This document is the single consolidated reference for the full ETL pipeline — from raw CSV ingestion to Power BI dashboards. It covers objectives, pipeline architecture, all table and view definitions, validation strategy, data lineage, and the ETL logging framework. It is intended for developers, analysts, and BI stakeholders.

For column-level detail, see `data_dictionary.md`.
For transformation-level lineage, see `data_lineage.md`.
For design principles and methodology, see `ETL_Methodology_Guide.docx`.

---

## Table of Contents

1. [Objectives](#1-objectives)
2. [Pipeline Architecture](#2-pipeline-architecture)
3. [Script Inventory](#3-script-inventory)
4. [Staging Layer](#4-staging-layer)
5. [Clean Layer Views](#5-clean-layer-views)
6. [Data Warehouse Layer](#6-data-warehouse-layer)
7. [Reporting Layer](#7-reporting-layer)
8. [Validation Strategy](#8-validation-strategy)
9. [ETL Logging Framework](#9-etl-logging-framework)
10. [Data Lineage Summary](#10-data-lineage-summary)
11. [v2.0 Schema Changes](#11-v20-schema-changes)

---

## 1. Objectives

### Data Engineering (DW Build)

- Ingest raw CSV operational data and load it into a validated, production-grade SQL Server data warehouse
- Apply staged data quality checks with hard pipeline gates at each layer boundary
- Enforce referential integrity throughout using IDENTITY surrogate keys
- Provide a single trusted source for all reporting and analytics
- Maintain full data lineage and audit trails from source to dashboard

### Business Intelligence (Analytics & Reporting)

- Expose business-friendly reporting views over the star-schema DW for Power BI
- Enable analysis of sales performance, delivery operations, route efficiency, and exception management
- Ensure all analytical measures and aggregations are defined in Power BI, not in SQL views
- Maintain traceable lineage for compliance and audit

---

## 2. Pipeline Architecture

The pipeline follows a strict linear flow. Each layer has a hard validation gate. No layer loads unless the preceding gate passes. Gates use `THROW` — not `RAISERROR` severity 10 — so failures are fatal and execution halts.

```
Raw CSV Files (sales, deliveries, routes, exceptions)
        │
        │  BULK INSERT — single transaction, UTF-8 encoding,
        │  error file per table, @DataPath variable
        ▼
Staging Layer  (staging schema)
        │
        │  GATE — 03_etl_staging_validation.sql
        │  THROW on: empty tables, NULL keys, negative values,
        │  referential integrity, date range/chronology
        ▼
Clean Layer  (clean schema — SQL views, no stored data)
        │
        │  GATE — 07_clean_validation_gate.sql
        │  THROW on: empty views, NULL fields, LATE flag mismatch,
        │  referential integrity
        ▼
DW Layer  (dw schema — single transaction, ROLLBACK on failure)
│
├── Dimensions: dim_date, dim_product_type, dim_region, dim_driver,
│               dim_route, dim_shipment_type, dim_delivery_status,
│               dim_exception_type, dim_priority_flag
│
└── Facts: fact_sales, fact_deliveries, fact_routes, fact_exceptions
        │
        │  GATE — 09_dw_validation.sql
        │  THROW on: empty tables, row count drops, NULL surrogate keys,
        │  duplicate PKs, FK orphans, metric sanity, date logic
        ▼
Reporting Layer  (reporting schema — SQL views, no stored data)
        │
        ▼
Power BI Dashboards
        (all aggregations and measures defined in Power BI, not SQL)
```

---

## 3. Script Inventory

All scripts are numbered for unambiguous execution order. Scripts sharing a prefix number caused ordering collisions in the original pipeline — each script now has a unique prefix.

| # | Script | Type | Description |
|---|--------|------|-------------|
| 01 | `01_initialize_database.sql` | Setup | Create database with explicit collation (Latin1_General_CI_AS), compatibility level (150), and SIMPLE recovery |
| 02 | `02_etl_staging_setup.sql` | Load | Create schemas, create staging tables, BULK INSERT CSVs in a single transaction |
| 03 | `03_etl_staging_validation.sql` | **Hard Gate** | Validate staging data; THROW halts pipeline on failure |
| 04 | `04_clean_layer_views_v2_0.sql` | Views | Create or replace all four clean layer views |
| 05 | `05_clean_layer_data_profiling_v2_0.sql` | Informational | Row counts, NULL rates, date sanity, referential integrity checks |
| 06 | `06_clean_layer_validation_v2_0.sql` | Informational | Human-readable validation results for pre-load review |
| 07 | `07_clean_validation_gate.sql` | **Hard Gate** | Validate clean views; THROW halts pipeline on failure |
| 08 | `08_dw_load.sql` | Load | Drop/create/load all DW tables in a single transaction |
| 09 | `09_dw_validation.sql` | **Hard Gate** | Validate DW after load; THROW halts pipeline on failure |
| 10 | `10_dw_health_audit.sql` | Monitoring | Ongoing production monitoring — row counts, NULLs, storage, orphans |
| 11 | `11_dw_column_profile.sql` | Monitoring | Deep column-level NULL profiling with quality scorecard |
| 12 | `12_reporting_views.sql` | Views | Create or replace all four reporting layer views |
| 13 | `13_etl_logging_setup.sql` | Setup | Create ETL logging tables and stored procedures |
| 14 | `14_generate_data_dictionary.sql` | Metadata | Auto-generate column catalog, PK catalog, FK catalog, extended property descriptions |

---

## 4. Staging Layer

**Schema:** `staging`
**Script:** `02_etl_staging_setup.sql`

Staging tables preserve raw source data with minimal transformation. All required identifier and date columns are declared `NOT NULL`. `ResolutionTimeHours` uses `DECIMAL(6,2)` to support fractional hours. `staging_routes` uses a composite primary key to prevent duplicate route+driver rows.

### staging.staging_sales — PK: SalesID

| Column | Type | Description |
|--------|------|-------------|
| SalesID | INT NOT NULL | Unique sales transaction identifier (PK) |
| DeliveryID | INT NOT NULL | Links sale to its delivery record |
| DateKey | DATE NOT NULL | Transaction date; converted to YYYYMMDD INT during DW load |
| ProductType | NVARCHAR(50) NOT NULL | Product category |
| Region | NVARCHAR(10) NOT NULL | Geographic region code |
| UnitsSold | INT NOT NULL | Units in this transaction |
| SalesAmount | DECIMAL(10,2) NOT NULL | Total revenue for this transaction (not per-unit price) |

### staging.staging_deliveries — PK: DeliveryID

| Column | Type | Description |
|--------|------|-------------|
| DeliveryID | INT NOT NULL | Unique delivery identifier (PK) |
| RouteID | NVARCHAR(10) NOT NULL | Source system route code |
| DriverID | NVARCHAR(50) NOT NULL | Source system driver code |
| Region | NVARCHAR(10) NOT NULL | Geographic region of the delivery |
| ShipmentType | NVARCHAR(20) NOT NULL | Shipment classification (Standard, Express, etc.) |
| DeliveryDate | DATE NOT NULL | Actual delivery completion date |
| ExpectedDeliveryDate | DATE NULL | Planned delivery date; NULL if unscheduled |
| DeliveryStatus | NVARCHAR(20) NOT NULL | Raw source status (Delivered, Delayed, Failed, etc.) |
| PriorityFlag | BIT NOT NULL | 1 = high priority, 0 = standard |

### staging.staging_exceptions — PK: ExceptionID

| Column | Type | Description |
|--------|------|-------------|
| ExceptionID | INT NOT NULL | Unique exception identifier (PK) |
| DeliveryID | INT NOT NULL | Links exception to its delivery |
| ExceptionType | NVARCHAR(50) NOT NULL | Exception category (Delay, Damage, Weather, etc.) |
| DateReported | DATE NOT NULL | Date exception was first reported |
| ResolvedDate | DATE NULL | Date exception was resolved; NULL = still open |
| ResolutionTimeHours | DECIMAL(6,2) NULL | Hours to resolve; DECIMAL supports fractional values |
| PriorityFlag | BIT NOT NULL | 1 = critical exception, 0 = standard |
| Region | NVARCHAR(10) NOT NULL | Region where exception occurred |

### staging.staging_routes — PK: (RouteID, DriverID) composite

| Column | Type | Description |
|--------|------|-------------|
| RouteID | NVARCHAR(10) NOT NULL | Route code; part of composite PK |
| DriverID | NVARCHAR(50) NOT NULL | Driver code; part of composite PK |
| PlannedStops | INT NOT NULL | Planned number of stops |
| ActualStops | INT NOT NULL | Actual stops completed |
| PlannedHours | DECIMAL(5,2) NOT NULL | Planned route duration in hours |
| ActualHours | DECIMAL(5,2) NOT NULL | Actual route duration in hours |
| Region | NVARCHAR(10) NOT NULL | Region the route operates in |

---

## 5. Clean Layer Views

**Schema:** `clean`
**Script:** `04_clean_layer_views_v2_0.sql`

Clean layer objects are SQL views — they store no data. Each view applies standardization, business rules, and quality flags to its staging source. The DW load reads exclusively from clean views, never from staging tables directly.

All text columns use `UPPER(LTRIM(RTRIM()))` to prevent duplicate dimension values from case or whitespace variation. Invalid rows are filtered. Derived quality flags are added for downstream audit use.

### clean.vw_sales ← staging.staging_sales

| Column | Description |
|--------|-------------|
| SalesID | Passed through |
| DeliveryID | Passed through |
| DateKey | Validated: 1900-01-01 to 2100-12-31; set to NULL if out of range |
| IsBadDateKey | **New v2.0.** 1 = DateKey out of range; rows retained for audit |
| ProductType | UPPER/TRIM normalized |
| Region | UPPER/TRIM normalized |
| UnitsSold | Passed through; rows with UnitsSold ≤ 0 excluded |
| SalesAmount | Passed through; rows with SalesAmount ≤ 0 excluded |

### clean.vw_deliveries ← staging.staging_deliveries

| Column | Description |
|--------|-------------|
| DeliveryID | Passed through |
| RouteID | Passed through |
| DriverID | Passed through |
| Region | UPPER/TRIM normalized |
| ShipmentType | UPPER/TRIM normalized |
| DeliveryDate | Passed through |
| ExpectedDeliveryDate | Passed through |
| DeliveryStatus | **Business rule:** forced to `'LATE'` if DeliveryDate > ExpectedDeliveryDate; otherwise UPPER/TRIM normalized. **All downstream comparisons must use `'LATE'` (all caps)** |
| PriorityFlag | Normalized to strict INT 0 or 1 via direct BIT comparison |

### clean.vw_exceptions ← staging.staging_exceptions

| Column | Description |
|--------|-------------|
| ExceptionID | Passed through |
| DeliveryID | Passed through |
| ExceptionType | UPPER/TRIM normalized |
| DateReported | Passed through |
| ResolvedDate | Chronology corrected: if source value precedes DateReported, set to DateReported as floor |
| IsDateCorrected | **New v2.0.** 1 = ResolvedDate was out of order and corrected; use for audit |
| ResolutionTimeHours | If stored value ≥ 0: used as-is. If negative but dates valid: derived from DATEDIFF(HOUR, DateReported, ResolvedDate). NULL if unresolvable |
| PriorityFlag | Normalized to strict INT 0 or 1 |
| Region | UPPER/TRIM normalized |

### clean.vw_routes ← staging.staging_routes

| Column | Description |
|--------|-------------|
| RouteID | Passed through |
| DriverID | Passed through |
| PlannedStops | Passed through; rows with PlannedStops ≤ 0 excluded |
| ActualStops | Passed through; rows with ActualStops ≤ 0 excluded |
| PlannedHours | Passed through; rows with PlannedHours ≤ 0 excluded |
| ActualHours | Passed through; rows with ActualHours ≤ 0 excluded |
| Region | UPPER/TRIM normalized |
| EfficiencyRatio | **New v2.0.** ROUND(ActualHours / PlannedHours, 4) |
| StopVariance | **New v2.0.** ActualStops - PlannedStops |

---

## 6. Data Warehouse Layer

**Schema:** `dw`
**Script:** `08_dw_load.sql`

The DW implements a star schema. The entire load runs inside a single transaction — any failure rolls back all tables. Facts are dropped before dimensions (FK order), and dimensions are loaded before facts (same reason).

`DATE` columns from clean views are converted to YYYYMMDD `INT` using:
`CONVERT(INT, CONVERT(VARCHAR(8), <date_col>, 112))`

### Dimension Tables

#### dw.dim_date — PK: DateKey (INT, YYYYMMDD)
Source: All date columns across vw_sales, vw_deliveries, vw_exceptions

| Column | Type | Description |
|--------|------|-------------|
| DateKey | INT | Primary key in YYYYMMDD format (e.g. 20241231) |
| FullDate | DATE | Full calendar date |
| Year | INT | Calendar year |
| Quarter | INT | Quarter (1–4) |
| Month | INT | Month number (1–12) |
| Day | INT | Day of month |
| Weekday | INT | 1 = Sunday … 7 = Saturday (@@DATEFIRST default) |
| IsWeekend | BIT | 1 = weekend, 0 = weekday |
| MonthName | NVARCHAR(20) | Full month name |
| DayName | NVARCHAR(20) | Full weekday name |
| WeekOfYear | INT | ISO week number |
| MonthYear | NVARCHAR(7) | 'YYYY-MM' format |
| YearMonth | NVARCHAR(6) | 'YYYYMM' format |
| FiscalYear | INT | Fiscal year (currently equals calendar year — placeholder) |
| IsHoliday | BIT | 1 = public holiday; placeholder — update from holiday reference table |

#### dw.dim_product_type — PK: ProductTypeID (IDENTITY)
Source: clean.vw_sales only

| Column | Type | Description |
|--------|------|-------------|
| ProductTypeID | INT | Surrogate key |
| ProductType | NVARCHAR(100) | Product category name |

#### dw.dim_region — PK: RegionID (IDENTITY)
Source: UNION of all four clean views

| Column | Type | Description |
|--------|------|-------------|
| RegionID | INT | Surrogate key |
| Region | NVARCHAR(50) | Region name |

#### dw.dim_driver — PK: DriverID (IDENTITY)
Source: vw_deliveries ∪ vw_routes

> **v2.0 rename:** `DriverName` → `DriverCode`. This column stores the source system identifier code, not a human-readable display name.

| Column | Type | Description |
|--------|------|-------------|
| DriverID | INT | Surrogate key |
| DriverCode | NVARCHAR(50) | Source system driver identifier (e.g. 'DRV-047') |

#### dw.dim_route — PK: RouteID (IDENTITY)
Source: vw_deliveries ∪ vw_routes

> **v2.0 rename:** `RouteName` → `RouteCode`. Same rationale as DriverCode.

| Column | Type | Description |
|--------|------|-------------|
| RouteID | INT | Surrogate key |
| RouteCode | NVARCHAR(10) | Source system route identifier (e.g. 'RT-042') |

#### dw.dim_shipment_type — PK: ShipmentTypeID (IDENTITY)

| Column | Type | Description |
|--------|------|-------------|
| ShipmentTypeID | INT | Surrogate key |
| ShipmentType | NVARCHAR(50) | Shipment classification |

#### dw.dim_delivery_status — PK: DeliveryStatusID (IDENTITY)

| Column | Type | Description |
|--------|------|-------------|
| DeliveryStatusID | INT | Surrogate key |
| DeliveryStatus | NVARCHAR(50) | Delivery status. **Canonical late value: `'LATE'` (all caps)** |

#### dw.dim_exception_type — PK: ExceptionTypeID (IDENTITY)

| Column | Type | Description |
|--------|------|-------------|
| ExceptionTypeID | INT | Surrogate key |
| ExceptionType | NVARCHAR(100) | Exception category |

#### dw.dim_priority_flag — PK: PriorityFlagID (IDENTITY)

| Column | Type | Description |
|--------|------|-------------|
| PriorityFlagID | INT | Surrogate key |
| PriorityFlag | BIT | 0 = standard, 1 = high priority |

---

### Fact Tables

All fact tables have explicit `PRIMARY KEY` constraints. All foreign key columns are `INT` surrogate keys referencing dimension tables.

#### dw.fact_sales — PK: SalesID
Grain: One row per sales transaction.

| Column | Type | Description |
|--------|------|-------------|
| SalesID | INT NOT NULL | Natural key; PK of this fact table |
| DeliveryID | INT NOT NULL | Business key linking sale to its delivery |
| DateKey | INT NOT NULL | FK → dim_date (YYYYMMDD) |
| ProductTypeID | INT NOT NULL | FK → dim_product_type |
| RegionID | INT NOT NULL | FK → dim_region |
| UnitsSold | INT NOT NULL | Units sold in this transaction |
| SalesAmount | DECIMAL(18,2) NOT NULL | Total transaction revenue |

#### dw.fact_deliveries — PK: DeliveryID
Grain: One row per delivery.

> **v2.0 change:** Single `DateKey` replaced by two date surrogate keys for late-delivery analysis.

| Column | Type | Description |
|--------|------|-------------|
| DeliveryID | INT NOT NULL | Natural key; PK of this fact table |
| RouteID | INT NOT NULL | FK → dim_route |
| DriverID | INT NOT NULL | FK → dim_driver |
| ShipmentTypeID | INT NOT NULL | FK → dim_shipment_type |
| DeliveryDateKey | INT NOT NULL | FK → dim_date; the actual delivery date |
| ExpectedDeliveryDateKey | INT NOT NULL | FK → dim_date; the planned delivery date |
| DeliveryStatusID | INT NOT NULL | FK → dim_delivery_status |
| PriorityFlagID | INT NOT NULL | FK → dim_priority_flag |

#### dw.fact_routes — PK: (RouteID, DriverID) composite
Grain: One row per route + driver combination.

> **v2.0 additions:** EfficiencyRatio and StopVariance carried from clean.vw_routes.

| Column | Type | Description |
|--------|------|-------------|
| RouteID | INT NOT NULL | FK → dim_route; part of composite PK |
| DriverID | INT NOT NULL | FK → dim_driver; part of composite PK |
| PlannedStops | INT NOT NULL | Planned number of stops |
| ActualStops | INT NOT NULL | Actual stops completed |
| PlannedHours | DECIMAL(10,2) NOT NULL | Planned route duration |
| ActualHours | DECIMAL(10,2) NOT NULL | Actual route duration |
| RegionID | INT NOT NULL | FK → dim_region |
| EfficiencyRatio | DECIMAL(10,4) NULL | ActualHours / PlannedHours; > 1.0 = over schedule |
| StopVariance | INT NULL | ActualStops - PlannedStops; positive = more stops than planned |

#### dw.fact_exceptions — PK: ExceptionID
Grain: One row per exception record.

> **v2.0 addition:** IsDateCorrected flag carried from clean.vw_exceptions.

| Column | Type | Description |
|--------|------|-------------|
| ExceptionID | INT NOT NULL | Natural key; PK of this fact table |
| DeliveryID | INT NOT NULL | Business key linking exception to its delivery |
| ExceptionTypeID | INT NOT NULL | FK → dim_exception_type |
| DateKey | INT NOT NULL | FK → dim_date; DateReported as YYYYMMDD |
| ResolutionTimeHours | DECIMAL(10,2) NULL | Hours to resolve; NULL = open exception |
| IsDateCorrected | BIT NOT NULL | 1 = clean layer corrected an out-of-order ResolvedDate |
| PriorityFlagID | INT NOT NULL | FK → dim_priority_flag |
| RegionID | INT NOT NULL | FK → dim_region |

---

## 7. Reporting Layer

**Schema:** `reporting`
**Script:** `12_reporting_views.sql`

All four reporting views are flat row-level joins. They contain no `GROUP BY` and no aggregations. Each view has the same grain as its source fact table (one row per fact row). All aggregations, measures, and calculated columns are defined in Power BI.

| View | Source Fact | Dimensions Joined | Key Columns |
|------|-------------|-------------------|-------------|
| vw_sales_summary | fact_sales | dim_product_type, dim_region, dim_date | SalesID, SaleDate, SaleYear, SaleQuarter, SaleMonth, ProductType, Region, UnitsSold, SalesAmount |
| vw_delivery_performance | fact_deliveries | dim_route, dim_driver, dim_shipment_type, dim_delivery_status, dim_priority_flag, dim_date ×2 | DeliveryID, Route (code), Driver (code), ShipmentType, DeliveryDate, ExpectedDeliveryDate, DeliveryStatus, IsPriority |
| vw_exception_dashboard | fact_exceptions | dim_exception_type, dim_priority_flag, dim_region, dim_date | ExceptionID, DeliveryID, ExceptionType, DateReported, ResolutionTimeHours, IsPriority, IsDateCorrected, Region |
| vw_route_efficiency | fact_routes | dim_route, dim_driver, dim_region | Route (code), Driver (code), Region, PlannedStops, ActualStops, PlannedHours, ActualHours, EfficiencyRatio, StopVariance |

> `Route` and `Driver` columns expose source system codes (`RouteCode`, `DriverCode`) — not display names. Power BI can apply friendly labels via a lookup table if needed.

---

## 8. Validation Strategy

Three hard gates enforce data quality across the pipeline. All use `THROW`, not `RAISERROR` severity 10. All begin with an empty-table guard — the most common failure mode is a silent bulk load leaving empty tables, which causes all subsequent NULL and range checks to pass vacuously.

### Gate 1 — Staging (`03_etl_staging_validation.sql`)

| Check Category | What It Catches |
|----------------|-----------------|
| Empty table guard | Silent BULK INSERT failure; prevents all other checks from giving false passes |
| NULL primary keys | Missing SalesID, DeliveryID, ExceptionID, RouteID/DriverID |
| NULL required fields | Missing DateKey, DeliveryDate, DateReported, DeliveryStatus, Region |
| Negative/zero values | UnitsSold ≤ 0, SalesAmount ≤ 0, stops/hours ≤ 0, ResolutionTimeHours < 0 |
| Referential integrity | DeliveryIDs in sales/exceptions with no match in staging_deliveries |
| Date range sanity | Dates before 2000 or after today; future ExpectedDeliveryDate beyond 1 year |
| Date chronology | ResolvedDate before DateReported |

### Gate 2 — Clean (`07_clean_validation_gate.sql`)

| Check | What It Catches |
|-------|-----------------|
| Empty view guard | Failed bulk load or view filter removing all rows |
| Sales required fields | NULL DateKey, SalesAmount, UnitsSold in vw_sales |
| Delivery required fields | NULL DeliveryID, RouteID, DriverID in vw_deliveries |
| Late delivery flag | Any row where DeliveryDate > ExpectedDeliveryDate but DeliveryStatus ≠ `'LATE'` |
| PriorityFlag normalization | Any PriorityFlag outside {0, 1} |
| Route hours (vs staging) | Checked against staging_routes directly — checking vw_routes would always pass because the view already filters invalid rows |
| Referential integrity — sales | vw_sales DeliveryIDs with no match in vw_deliveries |
| Referential integrity — exceptions | vw_exceptions DeliveryIDs with no match in vw_deliveries |

### Gate 3 — DW (`09_dw_validation.sql`)

| Check | What It Catches |
|-------|-----------------|
| Empty table guard | Failed or rolled-back DW load |
| Row count comparison | DW fact counts vs clean view counts; reveals rows dropped by failed surrogate key joins |
| NULL surrogate keys | Explicit per-table column lists (avoids vacuous passes from LIKE '%ID' patterns) |
| Duplicate PKs | Belt-and-suspenders check; PKs should prevent this but confirms constraint is active |
| FK integrity | NOT EXISTS fact-to-dimension checks for every relationship; never fact-to-fact |
| Business metric sanity | SalesAmount > 0, UnitsSold > 0, route hours > 0 |
| Delivery date logic | Non-LATE deliveries: DeliveryDateKey should not exceed ExpectedDeliveryDateKey |

### Informational Scripts (non-blocking)

| Script | When to Run |
|--------|-------------|
| `05_clean_layer_data_profiling_v2_0.sql` | After clean views created; review before DW load |
| `06_clean_layer_validation_v2_0.sql` | Human review of clean view quality before DW load |
| `10_dw_health_audit.sql` | Scheduled production monitoring (daily/weekly) |
| `11_dw_column_profile.sql` | Deep investigation of NULL rates and data distribution |

---

## 9. ETL Logging Framework

**Script:** `13_etl_logging_setup.sql`

### Tables

**dw.etl_run_log** — One row per pipeline execution.

| Column | Type | Description |
|--------|------|-------------|
| RunID | INT IDENTITY | PK; returned via OUTPUT from usp_start_etl_run |
| PipelineName | NVARCHAR(200) | Pipeline name (e.g. 'DW Full Load') |
| StartTime | DATETIME2 | UTC; set when run opens |
| EndTime | DATETIME2 | UTC; set when run closes; NULL while running |
| Status | VARCHAR(20) | CHECK: RUNNING, SUCCESS, FAILED, SKIPPED |
| ErrorMessage | NVARCHAR(MAX) | Populated on FAILED; NULL otherwise |

**dw.etl_step_log** — One row per ETL step within a run.

| Column | Type | Description |
|--------|------|-------------|
| StepLogID | INT IDENTITY | PK |
| RunID | INT | FK → etl_run_log(RunID); orphan records prevented |
| StepName | VARCHAR(200) | Step description (e.g. 'Load fact_sales') |
| StartTime | DATETIME2 | UTC; captured by caller before step begins |
| EndTime | DATETIME2 | UTC; set by procedure at insert time |
| DurationSec | Computed | DATEDIFF(SECOND, StartTime, EndTime) |
| RowsProcessed | INT | @@ROWCOUNT from the step |
| Status | VARCHAR(20) | CHECK: RUNNING, SUCCESS, FAILED, SKIPPED |
| ErrorMessage | NVARCHAR(MAX) | Populated on FAILED; NULL otherwise |

### Stored Procedures

| Procedure | Purpose |
|-----------|---------|
| `dw.usp_start_etl_run` | Opens a run record (Status = RUNNING); returns @RunID via OUTPUT |
| `dw.usp_log_etl_step` | Inserts a completed step with caller-supplied @StartTime and procedure-captured EndTime |
| `dw.usp_end_etl_run` | Closes the run with final Status; call in both TRY and CATCH |

### Usage Pattern

```sql
DECLARE @RunID INT;
EXEC dw.usp_start_etl_run @PipelineName = 'DW Full Load', @RunID = @RunID OUTPUT;

DECLARE @StepStart DATETIME2 = SYSUTCDATETIME();
-- ... ETL step work ...
EXEC dw.usp_log_etl_step
    @RunID = @RunID, @StepName = 'Load fact_sales',
    @StartTime = @StepStart, @RowsProcessed = @@ROWCOUNT, @Status = 'SUCCESS';

EXEC dw.usp_end_etl_run @RunID = @RunID, @Status = 'SUCCESS';
```

> All timestamps use `SYSUTCDATETIME()` (UTC) for consistency across time zones and SQL Agent environments.

---

## 10. Data Lineage Summary

For full column-level lineage and transformation details see `data_lineage.md`. Summary flow:

```
Raw CSV Files
    │  BULK INSERT (single transaction, UTF-8, error files)
    ▼
staging schema — raw data, minimal transformation, explicit NOT NULL constraints
    │  Gate 1: empty tables, NULL keys, negative values, RI, date sanity
    ▼
clean schema — SQL views; business rules, text normalization, quality flags
    │  New columns: IsBadDateKey, IsDateCorrected, EfficiencyRatio, StopVariance
    │  Gate 2: LATE flag, PriorityFlag, NULL fields, referential integrity
    ▼
dw schema — star schema; single-transaction load; surrogate key joins
    │  dim_date loaded first; facts loaded last
    │  DeliveryDateKey + ExpectedDeliveryDateKey (two date keys in fact_deliveries)
    │  Gate 3: empty tables, row count drops, NULL surrogates, FK orphans
    ▼
reporting schema — flat SQL views; one row per fact row; no GROUP BY
    │  EfficiencyRatio and StopVariance from fact_routes
    │  IsDateCorrected from fact_exceptions for audit use
    ▼
Power BI — all measures and aggregations defined here
```

**Key lineage notes:**
- Data quality is enforced at the clean layer (business rules, NULL checks, chronology)
- Surrogate keys in the DW prevent FK violations and decouple source system codes from analytics
- The reporting layer depends entirely on the DW — it never reads from staging or clean directly
- `'LATE'` (all caps) is the canonical delivery status value through every layer from clean to Power BI

---

## 11. v2.0 Schema Changes

These changes were introduced during the full pipeline review. All documentation, scripts, and Power BI datasets must reflect these changes.

| Change | Affected Objects | Reason |
|--------|-----------------|--------|
| `staging_routes` composite PK `(RouteID, DriverID)` | 02, 03 | Prevented silent duplicate route+driver rows causing double-counting |
| `ResolutionTimeHours` changed from `INT` to `DECIMAL(6,2)` | 02, staging_exceptions | Fractional hours (e.g. 1.5h) were silently truncated |
| `IsBadDateKey` flag added to `vw_sales` | 04, clean layer | Invalid DateKey rows retained for audit instead of silently dropped |
| `IsDateCorrected` flag added to `vw_exceptions` and `fact_exceptions` | 04, 08, 12 | Clean layer was silently correcting out-of-order dates with no audit trail |
| `EfficiencyRatio` and `StopVariance` added to `vw_routes` and `fact_routes` | 04, 08, 12 | Derived once at clean layer; carried through to avoid recomputation in every downstream consumer |
| `fact_deliveries` split `DateKey` into `DeliveryDateKey` + `ExpectedDeliveryDateKey` | 08, 09, 12 | Single date key made late-delivery analysis by expected date impossible |
| `dim_driver.DriverName` renamed to `DriverCode` | 08, 12 | Column stored source system code, not a display name; misleading naming |
| `dim_route.RouteName` renamed to `RouteCode` | 08, 12 | Same reason as DriverCode |
| All fact tables given explicit `PRIMARY KEY` constraints | 08, 09 | Without PKs, duplicate rows could load silently and DW validation PK checks passed vacuously |
| All validation gates use `THROW` not `RAISERROR` severity 10 | 03, 07, 09 | Severity 10 is informational; pipeline previously continued through all failed checks |
| Empty-table guard added as first check in all gates | 03, 07, 09 | Without it, a silent empty-table load causes all downstream checks to pass vacuously |
| Full DW load wrapped in single transaction | 08 | Partial load failure left DW in inconsistent mixed state |
| `BULK INSERT` wrapped in transaction with `ERRORFILE` per table | 02 | Partial load and silent encoding errors were undetectable |
| Reporting views stripped of vacuous `GROUP BY` / aggregations | 12 | Grouping by fact PK always produces single-row groups; SUM/COUNT always return 1 |
| `TotalRevenue = UnitsSold * SalesAmount` removed | 12 | SalesAmount is already the transaction total; multiplication double-counted revenue |
| Two validation files merged into one `09_dw_validation.sql` | 09 | Duplicate files with different prefixes created ordering ambiguity |

---

*Update this document whenever tables, views, business rules, or script filenames change. Treat column renames as breaking changes and update all affected layers simultaneously.*
