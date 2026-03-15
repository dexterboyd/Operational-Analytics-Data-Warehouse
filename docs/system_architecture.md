# System Architecture and Data Model
**Project:** FedEx Operations Data Warehouse
**Version:** 2.0
**Last Updated:** 2026

This document describes the system architecture, schema design, star schema model, pipeline execution model, and key design decisions for the FedEx Operations Data Warehouse.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Schema Architecture](#3-schema-architecture)
4. [Pipeline Execution Model](#4-pipeline-execution-model)
5. [Star Schema Data Model](#5-star-schema-data-model)
6. [Dimension Table Design](#6-dimension-table-design)
7. [Fact Table Design](#7-fact-table-design)
8. [Key Design Decisions](#8-key-design-decisions)
9. [Validation and Pipeline Safety](#9-validation-and-pipeline-safety)
10. [ETL Monitoring and Logging](#10-etl-monitoring-and-logging)
11. [Reporting and BI Layer](#11-reporting-and-bi-layer)
12. [v2.0 Architecture Changes](#12-v20-architecture-changes)

---

## 1. System Overview

The system implements a layered analytical data platform on SQL Server, designed to transform raw FedEx operational CSV data into validated, structured, and business-ready datasets for Power BI reporting.

**Core components:**

- Four-layer ETL pipeline (staging → clean → DW → reporting) with hard validation gates at each boundary
- Kimball-style star schema dimensional model in the DW layer
- SQL view-based clean and reporting layers (no stored data outside staging and DW)
- ETL logging framework for run-level and step-level audit trails
- Automated metadata generation (column catalog, PK/FK catalog, extended property descriptions)

**Technology stack:**

| Component | Technology |
|-----------|------------|
| Database engine | SQL Server 2019 (compat level 150) |
| Language | T-SQL |
| DW modeling | Kimball-style star schema |
| BI tool | Power BI (DirectQuery or Import) |
| Version control | Git |

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│  sales.csv  deliveries.csv  routes.csv  exceptions.csv  │
└──────────────────────┬──────────────────────────────────┘
                       │  BULK INSERT
                       │  Single transaction | UTF-8 | Error files
                       ▼
┌─────────────────────────────────────────────────────────┐
│                  STAGING LAYER                          │
│  staging.staging_sales                                  │
│  staging.staging_deliveries                             │
│  staging.staging_routes                                 │
│  staging.staging_exceptions                             │
└──────────────────────┬──────────────────────────────────┘
                       │  GATE 1: 03_etl_staging_validation.sql
                       │  THROW on failure
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   CLEAN LAYER                           │
│  clean.vw_sales        (business rules + IsBadDateKey)  │
│  clean.vw_deliveries   ('LATE' rule + PriorityFlag)     │
│  clean.vw_exceptions   (date correction + IsDateCorrected│
│                         + ResolutionTimeHours derivation)│
│  clean.vw_routes       (EfficiencyRatio + StopVariance) │
│                                                         │
│  [SQL VIEWS — no stored data]                           │
└──────────────────────┬──────────────────────────────────┘
                       │  GATE 2: 07_clean_validation_gate.sql
                       │  THROW on failure
                       ▼
┌─────────────────────────────────────────────────────────┐
│              DATA WAREHOUSE (STAR SCHEMA)               │
│                                                         │
│  DIMENSIONS                    FACTS                    │
│  dw.dim_date              ←─  dw.fact_sales             │
│  dw.dim_product_type      ←─  dw.fact_deliveries        │
│  dw.dim_region            ←─  dw.fact_routes            │
│  dw.dim_driver            ←─  dw.fact_exceptions        │
│  dw.dim_route                                           │
│  dw.dim_shipment_type     Single transaction load       │
│  dw.dim_delivery_status   ROLLBACK on any failure       │
│  dw.dim_exception_type                                  │
│  dw.dim_priority_flag                                   │
└──────────────────────┬──────────────────────────────────┘
                       │  GATE 3: 09_dw_validation.sql
                       │  THROW on failure
                       ▼
┌─────────────────────────────────────────────────────────┐
│                 REPORTING LAYER                         │
│  reporting.vw_sales_summary                             │
│  reporting.vw_delivery_performance                      │
│  reporting.vw_exception_dashboard                       │
│  reporting.vw_route_efficiency                          │
│                                                         │
│  [SQL VIEWS — flat joins, one row per fact row, no       │
│   GROUP BY, no aggregations]                            │
└──────────────────────┬──────────────────────────────────┘
                       │  DirectQuery or Import
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   POWER BI                              │
│  All aggregations and measures defined here (DAX)       │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Schema Architecture

The SQL Server database is organized into five schemas enforcing strict separation of responsibility. Each schema represents one layer of the pipeline.

| Schema | Layer | Type | Responsibility |
|--------|-------|------|----------------|
| `staging` | Ingestion | Base tables | Stores raw CSV data with minimal transformation; enforces NOT NULL on required columns |
| `clean` | Transformation | SQL views | Applies business rules, text normalization, quality flags, and derived metrics; no stored data |
| `dw` | Warehouse | Base tables | Star-schema dimensional model with IDENTITY surrogate keys, explicit PKs, and FK constraints |
| `reporting` | Presentation | SQL views | Flat joins over DW tables; business-readable column names; no aggregations; consumed by Power BI |
| *(dbo)* | *(not used)* | — | Data is loaded directly to `staging` via `BULK INSERT`; the `dbo` schema is not used in this pipeline |

**Schema creation:** Each schema is created in its own `IF NOT EXISTS / EXEC('CREATE SCHEMA ...')` block. `CREATE SCHEMA` must be the first statement in its batch — a dynamic SQL loop was used in v1.0 and was replaced in v2.0 for reliability.

---

## 4. Pipeline Execution Model

### Execution Order

Scripts are numbered `01` through `14` with unique prefixes. No two scripts share a prefix number. This ensures unambiguous execution order whether scripts are run manually or via a SQL Agent job.

### Gate-Based Flow Control

The pipeline uses a gate model: each validation script must succeed before the next load step runs. Gates use `THROW` (not `RAISERROR` severity 10) to halt execution with a fatal error when checks fail.

```
02 Load staging ──► 03 GATE ──► 04 Create clean views
                                     │
                                05/06 Profiling (informational)
                                     │
                               07 GATE ──► 08 Load DW
                                               │
                                          10/11 Monitoring (informational)
                                               │
                                          09 GATE ──► 12 Create reporting views
```

### Transaction Safety

**BULK INSERT (staging):** All four table loads run inside a single transaction. If any file fails, all four tables roll back — staging is never left in a partial state.

**DW Load:** The entire load (drop + create + all inserts) runs inside a single `BEGIN TRANSACTION / TRY / CATCH`. Any failure at any step rolls back all DW tables. The DW is always either fully loaded or completely empty after a run.

### Database Initialization

`01_initialize_database.sql` is a one-time destructive script. It:
- Drops the existing database (force-disconnects all active sessions first)
- Creates a fresh database with explicit `COLLATE Latin1_General_CI_AS`
- Sets `COMPATIBILITY_LEVEL = 150` (SQL Server 2019)
- Sets `RECOVERY SIMPLE` (dev/test; change to FULL for production)

---

## 5. Star Schema Data Model

The DW implements a classic Kimball-style star schema. Fact tables sit at the center; dimension tables surround them, connected via integer surrogate keys.

### Schema Diagram

```
                          ┌─────────────┐
                          │  dim_date   │
                          │  DateKey PK │
                          └──────┬──────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
   ┌──────┴───────┐     ┌────────┴────────┐    ┌────────┴──────────┐
   │  fact_sales  │     │fact_deliveries  │    │ fact_exceptions   │
   │  SalesID PK  │     │ DeliveryID PK   │    │ ExceptionID PK    │
   └──────┬───────┘     └────────┬────────┘    └────────┬──────────┘
          │                      │                      │
   ┌──────┴───────┐       ┌──────┴──────┐      ┌────────┴───────┐
   │dim_product_  │       │  dim_route  │      │dim_exception_  │
   │    type      │       │  dim_driver │      │    type        │
   └──────────────┘       │dim_shipment_│      └────────────────┘
                          │    type     │
   ┌──────────────┐       │dim_delivery_│      ┌────────────────┐
   │  dim_region  │       │   status    │      │dim_priority_   │
   └──────────────┘       └─────────────┘      │    flag        │
                                               └────────────────┘

   ┌─────────────────────────────────────────────────────────────┐
   │                      fact_routes                            │
   │              PK: (RouteID, DriverID) composite              │
   │     dim_route ◄──┤ RouteID                                  │
   │    dim_driver ◄──┤ DriverID                                 │
   │    dim_region ◄──┤ RegionID                                 │
   │                  │ EfficiencyRatio, StopVariance            │
   └─────────────────────────────────────────────────────────────┘
```

### DateKey Convention

`dim_date.DateKey` is stored as `INT` in `YYYYMMDD` format (e.g. 20241231). This format:
- Is fast to join and partition on (integer comparison)
- Is human-readable without conversion
- Requires an explicit conversion from source `DATE` columns during load

The canonical conversion formula used in every location:
```sql
CONVERT(INT, CONVERT(VARCHAR(8), <date_col>, 112))
```

`fact_deliveries` stores **two** DateKey columns pointing to `dim_date`:
- `DeliveryDateKey` — the actual delivery date
- `ExpectedDeliveryDateKey` — the planned delivery date

This enables late-delivery analysis by either date independently in Power BI.

---

## 6. Dimension Table Design

| Dimension | PK | Source | Key Column Notes |
|-----------|-----|--------|-----------------|
| dim_date | DateKey (INT) | All date columns across clean views | Natural key in YYYYMMDD format; not an IDENTITY surrogate |
| dim_product_type | ProductTypeID (IDENTITY) | clean.vw_sales only | Not sourced from vw_deliveries (no ProductType column there) |
| dim_region | RegionID (IDENTITY) | UNION of all four clean views | Covers every region code across the entire pipeline |
| dim_driver | DriverID (IDENTITY) | vw_deliveries ∪ vw_routes | `DriverCode` column stores source system code, not a display name |
| dim_route | RouteID (IDENTITY) | vw_deliveries ∪ vw_routes | `RouteCode` column stores source system code, not a display name |
| dim_shipment_type | ShipmentTypeID (IDENTITY) | vw_deliveries | — |
| dim_delivery_status | DeliveryStatusID (IDENTITY) | vw_deliveries | Canonical late value is `'LATE'` (all caps throughout every layer) |
| dim_exception_type | ExceptionTypeID (IDENTITY) | vw_exceptions | — |
| dim_priority_flag | PriorityFlagID (IDENTITY) | vw_deliveries ∪ vw_exceptions | Values: 0 = standard, 1 = high priority |

**Dimension load order:** `dim_date` must be loaded first — it is referenced by FK constraints in all four fact tables. Other dimensions load next. Facts load last.

**Surrogate vs natural keys:** All dimensions except `dim_date` use IDENTITY surrogate keys. `dim_date` uses a natural integer key (YYYYMMDD) because the date value itself is the lookup key in fact table joins.

---

## 7. Fact Table Design

All fact tables have explicit `PRIMARY KEY` constraints. Foreign keys reference dimension surrogate keys only — never source business keys.

### fact_sales — PK: SalesID

Grain: one row per sales transaction.

| Measures | Type | Notes |
|----------|------|-------|
| UnitsSold | INT | Units in this transaction |
| SalesAmount | DECIMAL(18,2) | Total transaction revenue — not per-unit price |

### fact_deliveries — PK: DeliveryID

Grain: one row per delivery. No numeric measures — all analytical value comes from dimension joins.

| Date Keys | Notes |
|-----------|-------|
| DeliveryDateKey | Actual delivery date → dim_date |
| ExpectedDeliveryDateKey | Planned delivery date → dim_date |

Both date keys are required for late-delivery analysis in Power BI.

### fact_routes — PK: (RouteID, DriverID) composite

Grain: one row per route + driver combination.

| Measures | Type | Notes |
|----------|------|-------|
| PlannedStops | INT | Planned number of stops |
| ActualStops | INT | Actual stops completed |
| PlannedHours | DECIMAL(10,2) | Planned route duration |
| ActualHours | DECIMAL(10,2) | Actual route duration |
| EfficiencyRatio | DECIMAL(10,4) | ActualHours / PlannedHours; derived in clean layer |
| StopVariance | INT | ActualStops - PlannedStops; derived in clean layer |

### fact_exceptions — PK: ExceptionID

Grain: one row per exception record.

| Measures / Flags | Type | Notes |
|-----------------|------|-------|
| ResolutionTimeHours | DECIMAL(10,2) | NULL = open or unresolvable exception |
| IsDateCorrected | BIT | 1 = clean layer corrected an out-of-order ResolvedDate |

---

## 8. Key Design Decisions

These decisions reflect findings from the full pipeline review. Each addresses a specific category of failure risk.

### 8.1 Composite PK on staging_routes

`staging_routes` uses a composite primary key `(RouteID, DriverID)`. A table without a primary key allows silent duplicate rows. For routes, the natural uniqueness is the route+driver combination — a single-column surrogate would not prevent logical duplicates.

### 8.2 DECIMAL(6,2) for ResolutionTimeHours

Fractional hours (e.g. 1.5h, 72.25h) are valid operational values. Storing this as `INT` silently truncates all fractional values without any error.

### 8.3 Two Date Keys in fact_deliveries

A single `DateKey` in `fact_deliveries` made late-delivery analysis by expected date impossible. Both dates are stored as separate INT surrogate keys pointing to `dim_date`. Power BI can join either key to `dim_date` independently for time-sliced late delivery analysis.

### 8.4 DriverCode and RouteCode Column Names

The v1.0 names `DriverName` and `RouteName` implied human-readable display names. These columns actually store source system identifier codes (e.g. `'DRV-047'`). Misleading names propagate to Power BI report labels and cause confusion for report consumers.

### 8.5 Derived Metrics in the Clean Layer

`EfficiencyRatio` and `StopVariance` are derived once in `clean.vw_routes` and stored in `fact_routes`. If these were computed in the reporting view or in Power BI instead, every consumer would re-implement the formula independently, and any formula change would require updating multiple locations.

### 8.6 Flat Reporting Views with No Aggregations

Three of the four original reporting views grouped by the fact table's own primary key. Grouping by a PK produces single-row groups where every `SUM` and `COUNT` always returns 1 — meaningless aggregation with non-trivial CPU cost. Reporting views are flat detail views; aggregations belong in Power BI.

### 8.7 SalesAmount Is Already a Total

`SalesAmount` is the total transaction revenue (documented in staging as "Total revenue for the transaction"). The original `TotalRevenue = UnitsSold * SalesAmount` column multiplied a total by a quantity, producing a number 10–100× larger than the actual revenue. Power BI creates `SUM(SalesAmount)` measures directly.

### 8.8 Canonical LATE Value (All Caps)

The clean layer forces `DeliveryStatus` to `'LATE'` (all caps) when the business rule fires. Every downstream comparison — validation gates, health audits, reporting views, Power BI filters — must use this exact string. A single character difference (`'Late'` vs `'LATE'`) in a case-insensitive collation appears to work but silently fails in a case-sensitive one. Consistency is required regardless of collation.

---

## 9. Validation and Pipeline Safety

### The Vacuous Pass Problem

The most dangerous validation failure mode is a check that passes not because data is correct, but because there is nothing to check against. The most common trigger: a silent BULK INSERT failure leaves empty tables. All NULL checks, range checks, and referential integrity checks then pass because there are no rows to fail.

**Defence:** The first check in every gate is an empty-table guard. If any table or view is empty, the gate throws immediately before any other check runs.

### THROW vs RAISERROR

`RAISERROR` with severity 10 is informational — it prints a message and execution continues. All three validation gates use `THROW` instead, which halts execution and propagates an error to the calling context (SQL Agent job, orchestration layer, or caller batch). Combined with `SET XACT_ABORT ON`, any open transaction is also rolled back.

### Vacuous Pass from View Filters

Validation checks against views that already filter out bad rows always pass. For example: checking `vw_routes` for invalid hours will always find zero rows because the view's own `WHERE` clause already excludes them. Gate 2 routes this check against `staging.staging_routes` directly to avoid the vacuous pass.

### Fact-to-Dimension vs Fact-to-Fact Joins

Referential integrity checks in the DW gate use `NOT EXISTS` to find fact rows with no matching dimension row. They never join two fact tables on a shared business key — that is a fact-to-fact join, which tests whether the same business event appears in two different facts, not whether a surrogate key is resolvable.

---

## 10. ETL Monitoring and Logging

**Tables:** `dw.etl_run_log`, `dw.etl_step_log`
**Script:** `13_etl_logging_setup.sql`

### Architecture

```
usp_start_etl_run
    │  INSERT into etl_run_log (Status = 'RUNNING')
    │  Returns @RunID via OUTPUT parameter
    │
    │  For each ETL step:
    │  DECLARE @StepStart DATETIME2 = SYSUTCDATETIME()  ← before step
    │  ... step work ...
    │  usp_log_etl_step(@RunID, @StepName, @StepStart, @@ROWCOUNT, 'SUCCESS')
    │      INSERT into etl_step_log (EndTime = SYSUTCDATETIME())
    │      DurationSec = computed column from StartTime and EndTime
    │
usp_end_etl_run
    │  UPDATE etl_run_log SET EndTime, Status
    │  Called in both TRY (SUCCESS) and CATCH (FAILED)
```

### Design Constraints

| Constraint | Reason |
|------------|--------|
| `@StartTime` captured by caller before step | If procedure captured it internally, duration would always be 0 or milliseconds |
| `SYSUTCDATETIME()` not `GETDATE()` | Local time is unreliable across DST changes and multi-server deployments |
| FK from etl_step_log to etl_run_log | Prevents orphaned step records from polluting the log when RunID is invalid |
| CHECK on Status column | Prevents arbitrary string values being inserted; enforces the valid status set |
| Computed `DurationSec` column | No storage cost; always accurate from StartTime and EndTime |

---

## 11. Reporting and BI Layer

### Reporting View Design Principles

1. **One row per fact row** — grain matches the source fact table's PK
2. **No GROUP BY** — aggregations belong in Power BI DAX measures
3. **No recomputed metrics** — use stored fact table columns (EfficiencyRatio, StopVariance) rather than re-deriving them
4. **No fabricated totals** — SalesAmount is the transaction total; do not multiply it by UnitsSold
5. **Accurate column names** — Route and Driver expose codes (RouteCode, DriverCode), not fabricated display names

### Power BI Connection

Power BI connects to the `reporting` schema views. All analytical measures are defined in DAX:

| KPI | DAX Pattern |
|-----|-------------|
| Total Revenue | `SUM(vw_sales_summary[SalesAmount])` |
| Late Delivery % | `DIVIDE(COUNTROWS(FILTER(table, [DeliveryStatus] = "LATE")), COUNTROWS(table))` |
| Avg Resolution Time | `AVERAGE(vw_exception_dashboard[ResolutionTimeHours])` |
| Route Efficiency | `AVERAGE(vw_route_efficiency[EfficiencyRatio])` |
| Stop Completion Rate | `DIVIDE(SUM([ActualStops]), SUM([PlannedStops]))` |
| Date Corrected Exceptions | `COUNTROWS(FILTER(table, [IsDateCorrected] = 1))` |

---

## 12. v2.0 Architecture Changes

| Area | v1.0 | v2.0 | Impact |
|------|------|------|--------|
| Schema creation | Dynamic SQL loop | Four individual IF NOT EXISTS blocks | More reliable; avoids CREATE SCHEMA batch constraint issues |
| Staging PK (routes) | No PK on staging_routes | Composite PK (RouteID, DriverID) | Prevents silent duplicate rows |
| ResolutionTimeHours | INT | DECIMAL(6,2) | Fractional hours no longer silently truncated |
| BULK INSERT | Hardcoded paths, no error files, no transaction | @DataPath variable, ERRORFILE per table, single transaction | Load failures produce diagnostic output; partial loads impossible |
| DateKey validation | Integer BETWEEN comparison | DATE literal BETWEEN comparison | Correct type matching; no implicit conversion risk |
| Audit flags | None | IsBadDateKey (vw_sales), IsDateCorrected (vw_exceptions, fact_exceptions) | Invalid and corrected rows visible for audit |
| Derived metrics | None in clean layer | EfficiencyRatio, StopVariance in vw_routes + fact_routes | Single source of truth; no formula duplication |
| fact_deliveries dates | Single DateKey | DeliveryDateKey + ExpectedDeliveryDateKey | Late-delivery analysis by either date is now possible |
| dim_driver column | DriverName | DriverCode | Column name accurately reflects its content |
| dim_route column | RouteName | RouteCode | Column name accurately reflects its content |
| Fact table PKs | None | Explicit PRIMARY KEY on all four facts | Duplicate rows detectable; DW validation PK checks no longer vacuous |
| Validation gates | RAISERROR severity 10 | THROW + SET XACT_ABORT ON | Failures are fatal; pipeline actually halts |
| Empty-table guard | Absent | First check in all three gates | Prevents vacuous passes after silent load failures |
| DW load transaction | No transaction | BEGIN TRANSACTION / TRY / CATCH | Partial loads impossible; DW always in a consistent state |
| Drop order | STRING_AGG (unordered) | Explicit ordered drops (facts first) | FK constraint errors on drop eliminated |
| Reporting view grain | GROUP BY PK (vacuous) | Flat joins, no GROUP BY | Removes meaningless single-row aggregations |
| TotalRevenue column | UnitsSold × SalesAmount | Removed | SalesAmount is already total; multiplication double-counted revenue |
| DW validation scripts | Two files with same 08_ prefix | Merged into single 09_dw_validation.sql | No ordering ambiguity; one authoritative script |
| Script numbering | Multiple 05_ and 08_ prefixes | Unique prefix 01–14 | Unambiguous execution order in any runner |

---

*Update this document when the system architecture, schema design, or pipeline execution model changes. Keep the star schema diagram, dimension/fact table tables, and v2.0 changes section in sync with the SQL scripts.*
