# Data Lineage Documentation
**Project:** FedEx Operations Data Warehouse
**Version:** 2.0
**Last Updated:** 2026

This document traces the complete flow of data from raw CSV source files through every pipeline layer to Power BI dashboards. It includes transformations applied at each stage, validation gates, and the scripts responsible for each step.

---

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Source Layer](#2-source-layer)
3. [Staging Layer](#3-staging-layer)
4. [Staging Validation Gate](#4-staging-validation-gate)
5. [Clean Layer](#5-clean-layer)
6. [Clean Layer Validation Gate](#6-clean-layer-validation-gate)
7. [Data Warehouse Layer](#7-data-warehouse-layer)
8. [DW Validation Gate](#8-dw-validation-gate)
9. [Reporting Layer](#9-reporting-layer)
10. [Power BI](#10-power-bi)
11. [ETL Logging](#11-etl-logging)
12. [Column-Level Lineage: Key Transformations](#12-column-level-lineage-key-transformations)
13. [Data Flow Diagram](#13-data-flow-diagram)

---

## 1. Pipeline Overview

The pipeline follows a linear four-layer architecture with a validation gate between each layer. No layer loads unless the previous gate passes.

| Step | Script | Layer | Action |
|------|--------|-------|--------|
| 1 | `01_initialize_database.sql` | — | Create database with explicit collation and compatibility level |
| 2 | `02_etl_staging_setup.sql` | Staging | Create schemas and tables; BULK INSERT CSV data |
| 3 | `03_etl_staging_validation.sql` | Staging Gate | Validate raw data; THROW on failure to block clean layer load |
| 4 | `04_clean_layer_views_v2_0.sql` | Clean | Create or replace clean layer views |
| 5 | `05_clean_layer_data_profiling_v2_0.sql` | Clean | Informational profiling; row counts, NULL rates, range checks |
| 6 | `06_clean_layer_validation_v2_0.sql` | Clean | Informational validation; surfaces issues for human review |
| 7 | `07_clean_validation_gate.sql` | Clean Gate | Hard pipeline gate; THROW on any failed check |
| 8 | `08_dw_load.sql` | DW | Drop, recreate, and reload all DW tables within a single transaction |
| 9 | `09_dw_validation.sql` | DW Gate | Hard pipeline gate; THROW on any failed check |
| 10 | `10_dw_health_audit.sql` | DW | Ongoing production monitoring; informational only |
| 11 | `11_dw_column_profile.sql` | DW | Deep column-level NULL profiling; informational only |
| 12 | `12_reporting_views.sql` | Reporting | Create or replace reporting layer views |
| 13 | `13_etl_logging_setup.sql` | Logging | Create ETL logging tables and stored procedures |
| 14 | `14_generate_data_dictionary.sql` | Metadata | Generate column catalog, PK catalog, FK catalog |

---

## 2. Source Layer

Raw operational data arrives as four CSV files. These are loaded directly into the `staging` schema via `BULK INSERT`.

| Source File | Destination Table | Description |
|-------------|-------------------|-------------|
| `sales.csv` | `staging.staging_sales` | Sales transactions tied to deliveries |
| `deliveries.csv` | `staging.staging_deliveries` | Delivery records with route, driver, and date info |
| `routes.csv` | `staging.staging_routes` | Planned vs actual route performance per driver |
| `exceptions.csv` | `staging.staging_exceptions` | Operational issues affecting deliveries |

**Load method:** `BULK INSERT` with `CODEPAGE = '65001'` (UTF-8), `MAXERRORS = 0`, and an `ERRORFILE` per table. All four loads run inside a single transaction — if any load fails, all four tables are rolled back.

**Path configuration:** The data folder path is set once via `@DataPath` at the top of `02_etl_staging_setup.sql`. It does not need to be edited in four places.

---

## 3. Staging Layer

**Schema:** `staging`
**Script:** `02_etl_staging_setup.sql`

Staging tables store raw source data with minimal transformation. Data types are aligned to the source, but no business rules are applied. All required identifier and date columns are declared `NOT NULL`.

| Table | Rows From | Primary Key | Notes |
|-------|-----------|-------------|-------|
| `staging.staging_sales` | `sales.csv` | `SalesID` | SalesAmount is total transaction revenue, not per-unit price |
| `staging.staging_deliveries` | `deliveries.csv` | `DeliveryID` | ExpectedDeliveryDate is nullable (not all deliveries are scheduled) |
| `staging.staging_exceptions` | `exceptions.csv` | `ExceptionID` | ResolutionTimeHours is DECIMAL(6,2) to support fractional hours; ResolvedDate NULL = open |
| `staging.staging_routes` | `routes.csv` | `(RouteID, DriverID)` composite | Composite PK prevents silent duplicate route+driver rows |

---

## 4. Staging Validation Gate

**Script:** `03_etl_staging_validation.sql`
**Type:** Hard gate — uses `THROW` to halt pipeline on failure.

Checks run in order. The pipeline stops at the first category of failure:

| Check | Tables | What It Catches |
|-------|--------|-----------------|
| Empty table guard | All four | Failed BULK INSERT that left tables empty; prevents vacuous passes on all other checks |
| NULL primary keys | All four | Missing SalesID, DeliveryID, ExceptionID, RouteID/DriverID |
| NULL required fields | All four | Missing DateKey, DeliveryDate, DateReported, DeliveryStatus, Region |
| Negative/zero values | Sales, Routes, Exceptions | UnitsSold ≤ 0, SalesAmount ≤ 0, stops/hours ≤ 0, ResolutionTimeHours < 0 |
| Referential integrity | Sales, Exceptions | DeliveryIDs that do not exist in staging_deliveries |
| Date range sanity | Sales, Deliveries, Exceptions | Dates before 2000 or after today; future ExpectedDeliveryDate beyond 1 year |
| Date chronology | Exceptions | ResolvedDate before DateReported |

---

## 5. Clean Layer

**Schema:** `clean`
**Script:** `04_clean_layer_views_v2_0.sql`
**Type:** SQL views (no stored data)

Each view selects from one staging table and applies standardizations and business rules. The clean layer is the single trusted source for all DW loads — the DW load script reads only from clean views, never from staging directly.

### clean.vw_sales ← staging.staging_sales

| Transformation | Detail |
|---------------|--------|
| DateKey range validation | Valid range: 1900-01-01 to 2100-12-31. Invalid → NULL with IsBadDateKey = 1 |
| IsBadDateKey flag | New audit column; 1 = invalid date retained for review |
| Text normalization | UPPER(LTRIM(RTRIM())) on ProductType, Region |
| Row filter | Excludes rows where SalesID, DeliveryID, DateKey are NULL or UnitsSold/SalesAmount ≤ 0 |

### clean.vw_deliveries ← staging.staging_deliveries

| Transformation | Detail |
|---------------|--------|
| Late delivery rule | If DeliveryDate > ExpectedDeliveryDate → DeliveryStatus = 'LATE' (forced, all caps) |
| Text normalization | UPPER(LTRIM(RTRIM())) on Region, ShipmentType, DeliveryStatus |
| PriorityFlag normalization | Direct BIT comparison → INT 0 or 1 |
| Row filter | Excludes rows where DeliveryID, RouteID, DriverID, DeliveryDate, ExpectedDeliveryDate are NULL |

### clean.vw_exceptions ← staging.staging_exceptions

| Transformation | Detail |
|---------------|--------|
| Chronology correction | If ResolvedDate < DateReported → ResolvedDate set to DateReported as floor |
| IsDateCorrected flag | New audit column; 1 = ResolvedDate was out of order and corrected |
| ResolutionTimeHours derivation | If stored value is negative but dates are valid → derived from DATEDIFF(HOUR, DateReported, ResolvedDate) |
| Text normalization | UPPER(LTRIM(RTRIM())) on ExceptionType, Region |
| PriorityFlag normalization | Direct BIT comparison → INT 0 or 1 |
| Row filter | Excludes rows where ExceptionID, DeliveryID, DateReported are NULL |

### clean.vw_routes ← staging.staging_routes

| Transformation | Detail |
|---------------|--------|
| Text normalization | UPPER(LTRIM(RTRIM())) on Region |
| EfficiencyRatio | Derived: ROUND(ActualHours / PlannedHours, 4) |
| StopVariance | Derived: ActualStops - PlannedStops |
| Row filter | Excludes rows where RouteID, DriverID are NULL or any stops/hours value ≤ 0 |

---

## 6. Clean Layer Validation Gate

**Script:** `07_clean_validation_gate.sql`
**Type:** Hard gate — uses `THROW` to halt pipeline on failure.

| Check | What It Catches |
|-------|-----------------|
| Empty view guard | All clean views must return rows before any other check runs |
| Sales required fields | NULL DateKey, SalesAmount, UnitsSold in vw_sales |
| Delivery required fields | NULL DeliveryID, RouteID, DriverID in vw_deliveries |
| Late delivery flag | Any row where DeliveryDate > ExpectedDeliveryDate but DeliveryStatus ≠ 'LATE' |
| PriorityFlag normalization | Any PriorityFlag value outside {0, 1} |
| Route hours (staging) | Checks staging_routes directly — vw_routes already filters bad rows, so checking the view would always pass vacuously |
| Referential integrity — sales | vw_sales DeliveryIDs with no match in vw_deliveries |
| Referential integrity — exceptions | vw_exceptions DeliveryIDs with no match in vw_deliveries |

---

## 7. Data Warehouse Layer

**Schema:** `dw`
**Script:** `08_dw_load.sql`
**Transaction:** Entire load runs inside `BEGIN TRANSACTION / TRY / CATCH`. Failure at any step rolls back all changes.

### Drop Order (prevents FK constraint errors)

Facts dropped first (they hold FK references), then dimensions.

```
fact_sales → fact_deliveries → fact_routes → fact_exceptions
→ dim_date → dim_product_type → dim_region → dim_driver
→ dim_route → dim_shipment_type → dim_delivery_status
→ dim_exception_type → dim_priority_flag
```

### Dimension Load Order

`dim_date` is loaded first (it is referenced by all four fact tables). All other dimensions load next. Facts load last.

| Dimension | Source | Key Conversion |
|-----------|--------|----------------|
| dim_date | UNION of DateKey (vw_sales), DeliveryDate + ExpectedDeliveryDate (vw_deliveries), DateReported (vw_exceptions) | DATE → CONVERT(INT, CONVERT(VARCHAR(8), d, 112)) |
| dim_product_type | vw_sales only | DISTINCT ProductType |
| dim_region | UNION of all four clean views | DISTINCT Region |
| dim_driver | vw_deliveries ∪ vw_routes | DISTINCT DriverID stored as DriverCode |
| dim_route | vw_deliveries ∪ vw_routes | DISTINCT RouteID stored as RouteCode |
| dim_shipment_type | vw_deliveries | DISTINCT ShipmentType |
| dim_delivery_status | vw_deliveries | DISTINCT DeliveryStatus |
| dim_exception_type | vw_exceptions | DISTINCT ExceptionType |
| dim_priority_flag | vw_deliveries ∪ vw_exceptions | DISTINCT PriorityFlag |

### Fact Load

All joins are INNER JOINs. A clean row that cannot resolve a surrogate key (e.g. a date missing from dim_date) is silently excluded. Post-load row count comparison in `09_dw_validation.sql` detects any such drops.

| Fact Table | Source View | Date Key Conversion |
|------------|-------------|---------------------|
| fact_sales | clean.vw_sales | CONVERT(INT, CONVERT(VARCHAR(8), DateKey, 112)) |
| fact_deliveries | clean.vw_deliveries | Same conversion on both DeliveryDate and ExpectedDeliveryDate |
| fact_routes | clean.vw_routes | No date column in routes |
| fact_exceptions | clean.vw_exceptions | Same conversion on DateReported |

---

## 8. DW Validation Gate

**Script:** `09_dw_validation.sql`
**Type:** Hard gate — uses `THROW` to halt pipeline on failure. Also serves as the authoritative validation script (the two original v1/v2 files were merged here).

| Check | What It Catches |
|-------|-----------------|
| Empty table guard | All DW tables must have rows; failed load or rolled-back transaction |
| Row count comparison | DW fact counts vs clean view counts; identifies rows dropped by failed surrogate key joins |
| NULL surrogate keys | Explicit per-fact-table column list; avoids the vacuous-pass from LIKE '%ID' patterns |
| Duplicate PKs | Belt-and-suspenders check on SalesID, DeliveryID, ExceptionID, (RouteID+DriverID) |
| FK integrity | NOT EXISTS checks for every FK relationship; fact-to-dimension only, never fact-to-fact |
| Business metric sanity | SalesAmount > 0, UnitsSold > 0, route hours > 0 |
| Delivery date logic | Non-LATE deliveries: DeliveryDateKey should not exceed ExpectedDeliveryDateKey |

---

## 9. Reporting Layer

**Schema:** `reporting`
**Script:** `12_reporting_views.sql`
**Type:** SQL views (no stored data)

All four views are flat row-level joins — one output row per fact row. No `GROUP BY`. No aggregations. Power BI creates `SUM`, `COUNT`, and ratio measures from these detail rows.

| View | Source Fact | Dimensions Joined |
|------|-------------|-------------------|
| vw_sales_summary | fact_sales | dim_product_type, dim_region, dim_date |
| vw_delivery_performance | fact_deliveries | dim_route, dim_driver, dim_shipment_type, dim_delivery_status, dim_priority_flag, dim_date (×2) |
| vw_exception_dashboard | fact_exceptions | dim_exception_type, dim_priority_flag, dim_region, dim_date |
| vw_route_efficiency | fact_routes | dim_route, dim_driver, dim_region |

---

## 10. Power BI

Power BI connects directly to the `reporting` schema views. All aggregations (totals, percentages, ratios) are implemented as Power BI measures using `SUM`, `DIVIDE`, `CALCULATE`, etc. — not as columns in the SQL views.

**Supported analyses:**
- Sales by region, product type, and time period
- On-time vs late delivery rate
- Priority shipment volume and impact
- Exception counts, types, and average resolution time
- Route efficiency vs plan by driver and region

---

## 11. ETL Logging

**Script:** `13_etl_logging_setup.sql`

Every pipeline run should be wrapped with the three logging procedures:

```
usp_start_etl_run   → opens a run record; returns @RunID via OUTPUT
usp_log_etl_step    → records each step with real StartTime (caller-captured) and EndTime (procedure-captured)
usp_end_etl_run     → closes the run with final SUCCESS or FAILED status
```

`dw.etl_run_log` tracks overall pipeline runs. `dw.etl_step_log` tracks individual steps with computed `DurationSec`. Both tables enforce `CHECK` constraints on the Status column: `RUNNING`, `SUCCESS`, `FAILED`, `SKIPPED`.

---

## 12. Column-Level Lineage: Key Transformations

This table traces the most important columns through every layer from source to reporting.

| Column | Source CSV | Staging | Clean | DW | Reporting |
|--------|------------|---------|-------|----|-----------|
| DeliveryStatus | Raw string | NVARCHAR(20) as-sourced | Forced to 'LATE' if DeliveryDate > ExpectedDeliveryDate; otherwise UPPER/TRIM | Stored in dim_delivery_status | Joined back as DeliveryStatus text in vw_delivery_performance |
| DateKey (sales) | Date string in CSV | DATE column | Validated range; IsBadDateKey flag added | CONVERT(INT, CONVERT(VARCHAR(8), DateKey, 112)) = YYYYMMDD INT in dim_date | FullDate, Year, Quarter, Month exposed via dim_date join |
| DeliveryDate | Date string in CSV | DATE column | Passed through | CONVERT to INT → DeliveryDateKey FK in fact_deliveries | DeliveryDate (FullDate from dim_date) in vw_delivery_performance |
| ExpectedDeliveryDate | Date string in CSV | DATE column | Passed through | CONVERT to INT → ExpectedDeliveryDateKey FK in fact_deliveries | ExpectedDeliveryDate in vw_delivery_performance (enables late analysis) |
| DriverID | String code | NVARCHAR(50) | Passed through | Stored as DriverCode in dim_driver; surrogate DriverID (INT) assigned | DriverCode exposed as Driver in vw_delivery_performance and vw_route_efficiency |
| RouteID | String code | NVARCHAR(10) | Passed through | Stored as RouteCode in dim_route; surrogate RouteID (INT) assigned | RouteCode exposed as Route in vw_delivery_performance and vw_route_efficiency |
| ResolutionTimeHours | Numeric | DECIMAL(6,2) | Corrected if negative (derived from dates); NULL if unresolvable | Carried to fact_exceptions | Exposed in vw_exception_dashboard |
| IsDateCorrected | Not in source | Not in staging | Derived in vw_exceptions (1 if ResolvedDate corrected) | Carried to fact_exceptions | Exposed in vw_exception_dashboard for audit use |
| EfficiencyRatio | Not in source | Not in staging | Derived in vw_routes: ActualHours / PlannedHours | Carried to fact_routes | Exposed in vw_route_efficiency |
| StopVariance | Not in source | Not in staging | Derived in vw_routes: ActualStops - PlannedStops | Carried to fact_routes | Exposed in vw_route_efficiency |

---

## 13. Data Flow Diagram

```
Raw CSV Files (sales, deliveries, routes, exceptions)
│
│  BULK INSERT — single transaction, CODEPAGE UTF-8,
│  ERRORFILE per table, @DataPath variable
▼
Staging Tables (staging schema)
│  staging_sales          PK: SalesID
│  staging_deliveries     PK: DeliveryID
│  staging_exceptions     PK: ExceptionID
│  staging_routes         PK: (RouteID, DriverID) composite
│
│  ▼ GATE: 03_etl_staging_validation.sql
│    THROW on: empty tables, NULL keys, negative values,
│    referential integrity failures, date range/chronology errors
│
▼
Clean Views (clean schema)
│  vw_sales       — DateKey validated, text normalized, IsBadDateKey flag
│  vw_deliveries  — 'LATE' rule applied, PriorityFlag normalized
│  vw_exceptions  — ResolvedDate corrected, IsDateCorrected flag, ResolutionTime derived
│  vw_routes      — EfficiencyRatio and StopVariance derived
│
│  ▼ GATE: 07_clean_validation_gate.sql
│    THROW on: empty views, NULL fields, 'LATE' flag mismatch,
│    referential integrity failures
│
▼
Data Warehouse (dw schema)  — single transaction, ROLLBACK on failure
│
│  Dimensions (loaded first)
│  dim_date               DateKey = YYYYMMDD INT (converted from DATE sources)
│  dim_product_type       sourced from vw_sales only
│  dim_region             union of all four clean views
│  dim_driver             DriverCode = source NVARCHAR DriverID
│  dim_route              RouteCode = source NVARCHAR RouteID
│  dim_shipment_type
│  dim_delivery_status    canonical 'LATE' value stored here
│  dim_exception_type
│  dim_priority_flag
│
│  Facts (loaded after dimensions)
│  fact_sales             PK: SalesID
│  fact_deliveries        PK: DeliveryID  — DeliveryDateKey + ExpectedDeliveryDateKey
│  fact_routes            PK: (RouteID, DriverID)  — EfficiencyRatio, StopVariance
│  fact_exceptions        PK: ExceptionID  — IsDateCorrected
│
│  ▼ GATE: 09_dw_validation.sql
│    THROW on: empty tables, row count drops, NULL surrogate keys,
│    duplicate PKs, FK orphans, metric sanity, date logic
│
▼
Reporting Views (reporting schema)
│  vw_sales_summary          flat join, one row per SalesID
│  vw_delivery_performance   flat join, one row per DeliveryID
│  vw_exception_dashboard    flat join, one row per ExceptionID
│  vw_route_efficiency       flat join, one row per (RouteID, DriverID)
│
▼
Power BI Dashboards
   Aggregations and measures defined in Power BI, not in SQL views
```

---

*Update this document whenever a transformation, business rule, or table structure changes. Keep the column-level lineage table and flow diagram in sync with the SQL scripts.*
