# FedEx Operations Data Warehouse
**Version:** 2.0
**Author:** Dexter Boyd
**LinkedIn:** [linkedin.com/in/dexter-boyd](https://www.linkedin.com/in/dexter-boyd)

An end-to-end SQL Server data warehouse for FedEx delivery operations, built on Kimball-style dimensional modeling. The pipeline ingests raw operational CSV data, applies staged validation and cleansing, loads a star-schema warehouse, and serves Power BI dashboards through a reporting layer.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Repository Structure](#3-repository-structure)
4. [Star Schema Design](#4-star-schema-design)
5. [ETL Pipeline](#5-etl-pipeline)
6. [Validation Strategy](#6-validation-strategy)
7. [ETL Execution Guide](#7-etl-execution-guide)
8. [Reporting and BI](#8-reporting-and-bi)
9. [ETL Logging](#9-etl-logging)
10. [Key Design Decisions (v2.0)](#10-key-design-decisions-v20)
11. [Technologies](#11-technologies)
12. [Documentation](#12-documentation)

---

## 1. Project Overview

**Objective:** Design, build, and validate a production-grade SQL Server data warehouse that enables accurate and timely business intelligence across sales performance, delivery operations, route efficiency, and exception management.

**Business questions answered:**
- What is total sales revenue by region, product type, and time period?
- Which routes and drivers are meeting planned vs actual performance targets?
- What percentage of deliveries are on time, and where are late deliveries concentrated?
- Which exception types have the longest resolution times?
- How do priority shipments affect operational performance by region?

**Scope:**
- Four CSV source files ingested via BULK INSERT
- Four staging tables → four clean views → nine dimensions + four facts → four reporting views
- Hard validation gates between each layer that halt execution on failures
- ETL logging framework for run-level and step-level audit trails

---

## 2. Architecture

The pipeline follows a strict linear flow. No layer loads unless the preceding validation gate passes. Each gate uses `THROW` (not `RAISERROR` severity 10) to ensure failures are fatal.

```
Raw CSV Files
     │
     ▼
Staging Layer  ──► Gate 1: staging_validation
     │
     ▼
Clean Layer    ──► Gate 2: clean_validation_gate
     │
     ▼
DW Layer       ──► Gate 3: dw_validation
     │
     ▼
Reporting Layer
     │
     ▼
Power BI Dashboards
```

**Key architectural properties:**
- **Transaction safety:** The entire DW load runs inside a single `BEGIN TRANSACTION / TRY / CATCH`. A failure at any step rolls back all tables to a clean empty state.
- **Single source of truth:** The DW load reads exclusively from clean views, never from staging tables directly.
- **Flat reporting views:** Reporting views contain no `GROUP BY` or aggregations. They are row-level joins over fact + dimension tables. All measures are defined in Power BI.

---

## 3. Repository Structure

```
/datasets/raw/
    sales.csv
    deliveries.csv
    routes.csv
    exceptions.csv

/scripts/
    01_initialize_database.sql        Database creation with collation, compat level, recovery model
    02_etl_staging_setup.sql          Schema creation, table definitions, BULK INSERT
    03_etl_staging_validation.sql     Hard gate: validates staging data before clean layer load
    04_clean_layer_views_v2_0.sql     Clean layer views with business rules and quality flags
    05_clean_layer_data_profiling_v2_0.sql  Informational profiling; row counts, NULL rates
    06_clean_layer_validation_v2_0.sql     Informational validation for human review
    07_clean_validation_gate.sql      Hard gate: validates clean views before DW load
    08_dw_load.sql                    Transactional drop/create/load of all DW tables
    09_dw_validation.sql              Hard gate: validates DW after load; merged from v1 + v2
    10_dw_health_audit.sql            Ongoing production monitoring; informational
    11_dw_column_profile.sql          Deep column-level NULL profiling; informational
    12_reporting_views.sql            Reporting layer views for Power BI
    13_etl_logging_setup.sql          ETL logging tables and stored procedures
    14_generate_data_dictionary.sql   Auto-generates column catalog, PK catalog, FK catalog

/docs/
    README.md                         This file
    data_dictionary.md                Complete column reference for all layers
    data_lineage.md                   End-to-end data flow with transformation details
    ETL_Methodology_Guide.docx        Beginner-oriented guide to ETL design principles
```

---

## 4. Star Schema Design

The DW layer implements a standard star schema: fact tables at the center, surrounded by dimension tables connected via integer surrogate keys.

### Dimension Tables

| Table | Surrogate Key | Source | Notes |
|-------|--------------|--------|-------|
| dim_date | DateKey (INT, YYYYMMDD) | All clean views | Covers all date columns across the pipeline |
| dim_product_type | ProductTypeID | vw_sales | |
| dim_region | RegionID | All four clean views | Union of all regional values |
| dim_driver | DriverID | vw_deliveries, vw_routes | `DriverCode` stores source system ID (not display name) |
| dim_route | RouteID | vw_deliveries, vw_routes | `RouteCode` stores source system ID (not display name) |
| dim_shipment_type | ShipmentTypeID | vw_deliveries | |
| dim_delivery_status | DeliveryStatusID | vw_deliveries | Canonical late value: `'LATE'` (all caps) |
| dim_exception_type | ExceptionTypeID | vw_exceptions | |
| dim_priority_flag | PriorityFlagID | vw_deliveries, vw_exceptions | Values: 0 = standard, 1 = high priority |

### Fact Tables

| Table | Primary Key | Grain | Measures |
|-------|-------------|-------|---------|
| fact_sales | SalesID | One row per sales transaction | UnitsSold, SalesAmount |
| fact_deliveries | DeliveryID | One row per delivery | — (all descriptive via FK) |
| fact_routes | (RouteID, DriverID) | One row per route + driver | PlannedStops, ActualStops, PlannedHours, ActualHours, EfficiencyRatio, StopVariance |
| fact_exceptions | ExceptionID | One row per exception | ResolutionTimeHours, IsDateCorrected |

**v2.0 schema changes from v1:**
- `fact_deliveries` now has two date keys: `DeliveryDateKey` and `ExpectedDeliveryDateKey` (was a single `DateKey`)
- `fact_routes` now includes `EfficiencyRatio` and `StopVariance` derived columns
- `fact_exceptions` now includes `IsDateCorrected` audit flag
- `dim_driver.DriverName` renamed to `DriverCode`
- `dim_route.RouteName` renamed to `RouteCode`
- All fact tables now have explicit `PRIMARY KEY` constraints

---

## 5. ETL Pipeline

### Stage 1: Staging Load (`02_etl_staging_setup.sql`)

- Creates the four pipeline schemas (`staging`, `clean`, `dw`, `reporting`) using individual `IF NOT EXISTS / EXEC('CREATE SCHEMA ...')` blocks
- Creates staging tables with `NOT NULL` constraints on all required columns
- Loads data from CSVs using `BULK INSERT` inside a single transaction
- Configuration: set `@DataPath` at the top of the script once — no need to edit four separate file paths

### Stage 2: Clean Layer (`04_clean_layer_views_v2_0.sql`)

Four `CREATE OR ALTER VIEW` statements. Views are stateless — re-running this script immediately updates all downstream consumers. Key transformations:

- **vw_sales:** DateKey range validation + `IsBadDateKey` audit flag; text normalization
- **vw_deliveries:** Late delivery business rule (DeliveryStatus forced to `'LATE'`); PriorityFlag normalization
- **vw_exceptions:** Chronology correction + `IsDateCorrected` flag; ResolutionTimeHours derivation; negative duration handling
- **vw_routes:** `EfficiencyRatio` and `StopVariance` derived columns

### Stage 3: DW Load (`08_dw_load.sql`)

Single-transaction load. Steps:
1. Drop facts first (FK constraint order), then dimensions
2. Create dimensions, then facts (with explicit PKs and FKs)
3. Load `dim_date` first (required before all fact loads)
4. Load remaining dimensions
5. Load facts using INNER JOIN surrogate key lookups

All `DATE → INT` conversions use the canonical formula:
`CONVERT(INT, CONVERT(VARCHAR(8), <date_col>, 112))`

### Stage 4: Reporting Views (`12_reporting_views.sql`)

Four flat `CREATE OR ALTER VIEW` statements. No `GROUP BY`. No aggregations. Power BI reads these views and defines all measures internally.

---

## 6. Validation Strategy

Three hard gates block pipeline progression on data quality failures. Each gate uses `THROW` (not `RAISERROR` severity 10) and begins with an empty-table guard.

### Gate 1 — Staging (`03_etl_staging_validation.sql`)

Validates raw data before the clean layer is used. Checks: empty tables, NULL primary keys, NULL required fields, negative/zero numeric values, referential integrity (DeliveryIDs), date range sanity, date chronology.

### Gate 2 — Clean (`07_clean_validation_gate.sql`)

Validates clean views before the DW load. Checks: empty views, NULL fields, `'LATE'` flag correctness, PriorityFlag values, route hours in staging (checked at source, not in the view which already filters bad rows), referential integrity.

### Gate 3 — DW (`09_dw_validation.sql`)

Validates the DW after load. Checks: empty tables, row count comparison against clean views, NULL surrogate keys (explicit column lists — no `LIKE '%ID'` patterns), duplicate PKs, fact-to-dimension FK integrity (NOT EXISTS — never fact-to-fact), business metric sanity, delivery date logic.

### Informational Scripts (non-blocking)

| Script | Purpose |
|--------|---------|
| `05_clean_layer_data_profiling_v2_0.sql` | Row counts, NULL rates, referential integrity, date sanity |
| `06_clean_layer_validation_v2_0.sql` | Human-readable validation results for pre-load review |
| `10_dw_health_audit.sql` | Ongoing monitoring: row counts, duplicate keys, actual NULL counts, storage size, orphan detection |
| `11_dw_column_profile.sql` | Deep per-column NULL profiling with quality scorecard |

---

## 7. ETL Execution Guide

Run scripts in numeric order. Each script is idempotent (safe to re-run) except `01_initialize_database.sql`, which drops and recreates the database.

```
-- First-time setup only (DESTRUCTIVE — drops the database)
01_initialize_database.sql

-- Pipeline run (run in this order every load)
02_etl_staging_setup.sql          -- set @DataPath at the top first
03_etl_staging_validation.sql     -- stops here if staging data is bad
04_clean_layer_views_v2_0.sql
07_clean_validation_gate.sql      -- stops here if clean views are bad
08_dw_load.sql
09_dw_validation.sql              -- stops here if DW load has issues
12_reporting_views.sql

-- Optional: run after pipeline for monitoring and documentation
05_clean_layer_data_profiling_v2_0.sql
06_clean_layer_validation_v2_0.sql
10_dw_health_audit.sql
11_dw_column_profile.sql
14_generate_data_dictionary.sql

-- One-time setup: ETL logging framework
13_etl_logging_setup.sql
```

**Before first run:**
1. Set `@DataPath` in `02_etl_staging_setup.sql` to the folder containing your CSV files
2. Confirm SQL Server version and update `COMPATIBILITY_LEVEL` in `01_initialize_database.sql` if needed (default: 150 = SQL Server 2019)
3. Confirm the `reporting` schema exists before running `12_reporting_views.sql`

---

## 8. Reporting and BI

### Reporting Views

| View | Source Fact | Description |
|------|-------------|-------------|
| `reporting.vw_sales_summary` | fact_sales | One row per transaction; date attributes, product type, region, units, amount |
| `reporting.vw_delivery_performance` | fact_deliveries | One row per delivery; route, driver, shipment type, status, actual + expected dates |
| `reporting.vw_exception_dashboard` | fact_exceptions | One row per exception; type, region, resolution time, priority, date correction flag |
| `reporting.vw_route_efficiency` | fact_routes | One row per route+driver; planned vs actual stops and hours, efficiency ratio, stop variance |

### Power BI Connection

Connect Power BI to the `reporting` schema views using DirectQuery or Import mode. All aggregations (`SUM`, `DIVIDE`, `CALCULATE`, percentage calculations) are implemented as Power BI measures.

### Example KPIs

| KPI | Source View | Formula in Power BI |
|-----|-------------|---------------------|
| Total Revenue | vw_sales_summary | `SUM(SalesAmount)` |
| Late Delivery % | vw_delivery_performance | `DIVIDE(COUNTROWS(FILTER(table, DeliveryStatus = "LATE")), COUNTROWS(table))` |
| Avg Resolution Time | vw_exception_dashboard | `AVERAGE(ResolutionTimeHours)` |
| Route Efficiency | vw_route_efficiency | `AVERAGE(EfficiencyRatio)` |
| Stop Completion Rate | vw_route_efficiency | `DIVIDE(SUM(ActualStops), SUM(PlannedStops))` |

---

## 9. ETL Logging

**Script:** `13_etl_logging_setup.sql`

Three stored procedures provide a complete logging framework:

```sql
-- Open a run record; capture the RunID
DECLARE @RunID INT;
EXEC dw.usp_start_etl_run @PipelineName = 'DW Full Load', @RunID = @RunID OUTPUT;

-- Log each step (capture start time before the step begins)
DECLARE @StepStart DATETIME2 = SYSUTCDATETIME();
-- ... ETL work ...
EXEC dw.usp_log_etl_step
    @RunID = @RunID, @StepName = 'Load fact_sales',
    @StartTime = @StepStart, @RowsProcessed = @@ROWCOUNT, @Status = 'SUCCESS';

-- Close the run (call in both TRY and CATCH)
EXEC dw.usp_end_etl_run @RunID = @RunID, @Status = 'SUCCESS';
```

All timestamps are UTC (`SYSUTCDATETIME()`). `etl_step_log` has a computed `DurationSec` column. Both tables enforce `CHECK` constraints on `Status`: `RUNNING`, `SUCCESS`, `FAILED`, `SKIPPED`.

---

## 10. Key Design Decisions (v2.0)

These changes were introduced during the full ETL project review:

| Decision | Reason |
|----------|--------|
| Composite PK `(RouteID, DriverID)` on staging_routes | Prevents silent duplicate rows that would cause double-counting in aggregations |
| `ResolutionTimeHours` as `DECIMAL(6,2)` instead of `INT` | Fractional hours (e.g. 1.5) were silently truncated with INT |
| `BULK INSERT` in a single transaction | Partial load failure previously left staging in an inconsistent mixed state |
| `THROW` instead of `RAISERROR` severity 10 in all gates | Severity 10 is informational — it never halts execution. Severity 10 warnings were previously treated as passing gates |
| Empty-table guard first in every validation gate | Without it, all downstream NULL checks pass vacuously on an empty dataset after a failed bulk load |
| `dim_driver.DriverCode` / `dim_route.RouteCode` rename | Columns stored source identifier codes, not display names. The old names (`DriverName`, `RouteName`) were actively misleading |
| Two date keys in `fact_deliveries` | A single `DateKey` made late-delivery analysis by expected date impossible |
| `EfficiencyRatio` and `StopVariance` in `fact_routes` | Avoids each downstream consumer recomputing the same formula independently |
| `IsDateCorrected` flag in `vw_exceptions` and `fact_exceptions` | The clean layer was silently correcting out-of-order dates with no indication to consumers |
| `IsBadDateKey` flag in `vw_sales` | Rows with invalid DateKey are retained for audit rather than silently dropped |
| No `GROUP BY` in reporting views | Three of four views were grouping by their fact table's PK, producing trivially single-row groups where SUM and COUNT always return 1 — meaningless aggregation |
| `SalesAmount` passed through without multiplication | The original `TotalRevenue = UnitsSold * SalesAmount` double-counted revenue because SalesAmount is already the transaction total |
| `QUOTENAME()` in all dynamic SQL | Reserved-word column names (`[Year]`, `[Month]`, `[Day]`) in `dim_date` caused syntax errors in dynamic SQL without it |
| `SYSUTCDATETIME()` in ETL logging | `GETDATE()` is local server time; unreliable across time zones and daylight saving boundaries |

---

## 11. Technologies

- **Database:** SQL Server 2019 (compatible with 2017–2022)
- **Language:** T-SQL
- **Modeling:** Kimball-style star schema dimensional modeling
- **BI Layer:** Power BI (DirectQuery or Import mode)
- **Version Control:** Git

---

## 12. Documentation

| Document | Description |
|----------|-------------|
| `README.md` | This file — project overview, architecture, execution guide |
| `data_dictionary.md` | Complete column-level reference for all tables and views |
| `data_lineage.md` | End-to-end data flow, transformations, and column-level lineage |
| `ETL_Methodology_Guide.docx` | Beginner-oriented guide explaining the design principles behind every pipeline decision |

All documentation reflects the **v2.0 schema**. Update when columns, tables, business rules, or script filenames change.
