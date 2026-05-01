# FedEx Operations Data Warehouse
### Star Schema Design, ETL Pipeline & Operational Analytics in SQL Server

**Author:** Dexter Boyd · [LinkedIn](www.linkedin.com/in/dexter-boyd)  
**Version:** 2.0 · **Database:** SQL Server 2019 (Compatibility Level 150)

---

## Project Overview

This project demonstrates the design and implementation of a production-grade dimensional data warehouse for delivery operations, sales performance, route efficiency, and exception management reporting.

The solution follows **Kimball-style dimensional modeling** principles using a star schema design with a fully validated, layered ETL pipeline. Each pipeline layer has a hard validation gate — no layer loads unless the preceding gate passes.

The warehouse enables analysis of:

- Sales performance by region, product type, and time period
- Delivery on-time performance vs planned route metrics
- Exception trends, types, and resolution times
- Priority shipment impact on operational performance
- Route efficiency (planned vs actual stops and hours)

---

## Architecture

The pipeline follows a strict 4-layer linear flow:

```
Raw CSV Files (sales, deliveries, routes, exceptions)
        │
        │  Python ETL — single transaction, error handling,
        │  load logging, transaction rollback on failure
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
        │  THROW on: empty views, NULL fields, IsLate flag
        │  mismatch, truncated values, referential integrity
        ▼
DW Layer  (dw schema — single transaction, ROLLBACK on failure)
        │
        ├── Dimensions: dim_date, dim_product_type, dim_region,
        │   dim_driver, dim_route, dim_shipment_type,
        │   dim_delivery_status, dim_exception_type, dim_priority_flag
        │
        └── Facts: fact_sales, fact_deliveries,
                   fact_routes, fact_exceptions
        │
        │  GATE — 09_dw_validation.sql
        │  THROW on: empty tables, row count drops, NULL surrogate
        │  keys, duplicate PKs, FK orphans, metric sanity
        ▼
Reporting Layer  (reporting schema — SQL views, no stored data)
        │
        ▼
Power BI Dashboards
        (all aggregations and measures defined in Power BI, not SQL)
```

---

## Star Schema Design

### Dimension Tables

All dimensions use **surrogate keys** (IDENTITY) for referential integrity, decoupling source system codes from analytics.

| Dimension Table | Surrogate Key | Source |
|---|---|---|
| `dim_date` | DateKey | Generated calendar spine (2023–2025) |
| `dim_region` | RegionSK | Region from all staging tables |
| `dim_driver` | DriverSK | DriverID from staging_deliveries, staging_routes |
| `dim_route` | RouteSK | RouteID from staging_routes |
| `dim_product_type` | ProductSK | ProductType from staging_sales |
| `dim_shipment_type` | ShipmentTypeSK | ShipmentType from staging_deliveries |
| `dim_delivery_status` | DeliveryStatusSK | DeliveryStatus from staging_deliveries |
| `dim_exception_type` | ExceptionTypeSK | ExceptionType from staging_exceptions |
| `dim_priority_flag` | PriorityFlagSK | PriorityFlag from deliveries + exceptions |

> `dim_driver` includes an `IsUnknown` flag and a dedicated `Unknown` row to handle the ~1,010 NULL DriverID values in the source data without dropping records.

### Fact Tables

| Fact Table | Grain | Key Measures |
|---|---|---|
| `fact_sales` | One row per sales transaction | UnitsSold, SalesAmount, RevenuePerUnit |
| `fact_deliveries` | One row per delivery | DeliveryDateKey, ExpectedDeliveryDateKey, IsLate, DaysVariance |
| `fact_routes` | One row per route run | PlannedStops, ActualStops, PlannedHours, ActualHours, StopVariance, EfficiencyRatio |
| `fact_exceptions` | One row per exception | ResolutionTimeHours, ResolutionDays, IsResolved, IsDateCorrected |

> `fact_deliveries` uses two separate date keys — `DeliveryDateKey` and `ExpectedDeliveryDateKey` — to support late delivery analysis by expected date. All fact tables have explicit PRIMARY KEY constraints.

---

## Script Inventory

All scripts are numbered for unambiguous execution order.

| # | Script | Type | Description |
|---|---|---|---|
| 01 | `01_initialize_database.sql` | Setup | Create database: collation Latin1_General_CI_AS, compatibility 150, SIMPLE recovery |
| 02 | `02_etl_staging_setup.sql` | Setup | Create all schemas and staging tables; idempotent (safe to re-run) |
| 03 | `03_load_staging.py` | Load | Python ETL: truncate → load CSVs → log row counts; full transaction rollback on failure |
| 04 | `04_clean_layer_views.sql` | Views | Create/replace all four clean layer transformation views |
| 05 | `05_clean_layer_data_profiling.sql` | Informational | Row counts, NULL rates, referential integrity, descriptive statistics |
| 06 | `06_clean_layer_validation.sql` | Informational | Human-readable validation results for pre-load review |
| 07 | `07_clean_validation_gate.sql` | **Hard Gate** | 11-check validation gate; THROW halts pipeline on any failure |
| 08 | `08_dw_layer.sql` | Load | Drop/create/load all DW tables in a single transaction with ROLLBACK |
| 09 | `09_dw_validation.sql` | **Hard Gate** | Post-load DW validation; THROW halts on empty tables, orphans, NULL surrogates |
| 10 | `10_etl_staging_validation.sql` | **Hard Gate** | Staging gate; THROW on NULL keys, row count drops, date range violations |
| 11 | `11_dw_column_profile.sql` | Monitoring | Deep column-level NULL profiling with data quality scorecard |
| 12 | `12_dw_health_audit.sql` | Monitoring | Production monitoring: row counts, duplicate PKs, storage size, orphan detection |
| 13 | `13_reporting_layer.sql` | Views | Create/replace all reporting layer views for Power BI |
| 14 | `14_reporting_validation_gate.sql` | **Hard Gate** | Reporting layer validation before Power BI connection |

---

## ETL Pipeline Detail

### Staging Layer
Raw CSV data is loaded via `03_load_staging.py` using **pandas + SQLAlchemy** with `fast_executemany=True`. Each run truncates staging tables before load to prevent duplicates. All loads execute within a single transaction — any failure triggers a full rollback and no tables are partially loaded. Load row counts are recorded in `staging.load_log` for downstream validation.

### Clean Layer
The clean layer is entirely **view-based** — no data is copied or stored. Views apply:
- Truncated value expansion (e.g. `'L.'` → `'Large Package'`)
- NULL handling (`DriverID` NULL → `'Unknown'`)
- Derived business flags (`IsLate`, `IsResolved`, `IsBadDateKey`, `IsDateCorrected`)
- Derived metrics (`DaysVariance`, `StopVariance`, `HourVariance`, `EfficiencyRatio`, `RevenuePerUnit`)
- Time dimension extraction (`Year`, `Month`, `Quarter` from date columns)

### DW Layer
The full DW load executes in a **single transaction**. Dimensions are loaded before facts. Surrogate keys are resolved via joins at load time. If any step fails, the entire transaction rolls back, leaving the DW in a consistent state.

### Reporting Layer
Reporting views are flat, one-row-per-fact-row SQL views with no `GROUP BY` or aggregations. All analytical measures are defined in Power BI, not in SQL, to maintain a single source of truth for business logic.

---

## Validation Strategy

Three hard pipeline gates use `THROW` (not `RAISERROR`) so failures are fatal and execution halts immediately.

### Gate 1 — Staging (`10_etl_staging_validation.sql`)
Catches empty tables, NULL primary keys, negative metric values, referential integrity violations, and date range/chronology errors before the clean layer is built.

### Gate 2 — Clean Layer (`07_clean_validation_gate.sql`)
11 automated checks covering empty view guard, required field NULLs, `IsLate` flag accuracy, `PriorityFlag` normalization, truncation resolution, referential integrity, and route source data validity.

### Gate 3 — DW (`09_dw_validation.sql`)
Validates post-load DW state: empty table guard, row count comparison vs clean views, NULL surrogate keys, duplicate PK detection, FK orphan checks across all fact-to-dimension relationships, business metric sanity, and delivery date logic.

### Informational Scripts (non-blocking)
| Script | When to Run |
|---|---|
| `05_clean_layer_data_profiling.sql` | After clean views created; review before DW load |
| `06_clean_layer_validation.sql` | Human review of clean view quality |
| `11_dw_column_profile.sql` | Deep investigation of NULL rates and data distribution |
| `12_dw_health_audit.sql` | Scheduled production monitoring (daily/weekly) |

---

## Business Questions Answered

- What is total sales revenue by region, product type, and month?
- Which routes are meeting planned vs actual stop and hour targets?
- Which exception types have the longest resolution times?
- What percentage of deliveries are on time by region and shipment type?
- How do priority shipments compare to standard shipments in on-time performance?
- What is the trend in open vs resolved exceptions over time?

---

## Technologies Used

| Category | Tools |
|---|---|
| Database | SQL Server 2019, T-SQL, SSMS |
| ETL / Python | Python 3, pandas, SQLAlchemy, pyodbc |
| Modeling | Kimball Star Schema, Dimensional Modeling |
| BI / Reporting | Power BI, DAX, Power Query |
| Validation | Pipeline gates with THROW, dynamic SQL profiling |

---

## Key Concepts Demonstrated

- Kimball-style dimensional modeling with surrogate key implementation
- Multi-layer ETL pipeline with hard validation gates at each boundary
- View-based clean layer for zero-copy transformation
- Python ETL with transactional integrity and audit logging
- Dynamic SQL for automated column-level NULL profiling
- Referential integrity enforcement across all fact-to-dimension relationships
- Separation of SQL reporting layer from Power BI analytical measures

---

## v2.0 Schema Changes

| Change | Reason |
|---|---|
| `staging_routes` composite PK `(RouteID, DriverID)` | Prevented silent duplicate route+driver rows causing double-counting |
| `ResolutionTimeHours` changed from `INT` to `DECIMAL(6,2)` | Fractional hours were silently truncated |
| `fact_deliveries` split into `DeliveryDateKey` + `ExpectedDeliveryDateKey` | Single date key made late-delivery analysis by expected date impossible |
| `IsDateCorrected` flag added to `fact_exceptions` | Clean layer was silently correcting out-of-order dates with no audit trail |
| `EfficiencyRatio` and `StopVariance` derived at clean layer | Avoided recomputation in every downstream consumer |
| All fact tables given explicit PRIMARY KEY constraints | Without PKs, duplicate rows could load silently |
| All gates use `THROW` not `RAISERROR` severity 10 | Severity 10 is informational — pipeline previously continued through failures |
| Empty-table guard added as first check in all gates | Without it, a silent empty load causes all downstream checks to pass vacuously |
| Full DW load wrapped in single transaction | Partial failure previously left DW in inconsistent mixed state |
| `TotalRevenue = UnitsSold * SalesAmount` removed from reporting views | SalesAmount is already the transaction total; multiplication double-counted revenue |

---

*Update this README whenever tables, views, business rules, or script filenames change. Treat column renames as breaking changes and update all affected layers simultaneously.*
