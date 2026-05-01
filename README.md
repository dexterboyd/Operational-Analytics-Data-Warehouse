# Express Operations Data Warehouse
### Star Schema Design, ETL Pipeline & Operational Analytics in SQL Server

**Author:** Dexter Boyd
**Version:** 2.0
**Database:** SQL Server 2022 16.0
---

## Project Overview

This project demonstrates the design and implementation of a production-grade dimensional data warehouse for delivery operations, sales performance, route efficiency, and exception management reporting.
The solution follows **Kimball-style dimensional modeling** using a star schema design with a validated and layered ETL pipeline. Each pipeline layer has a hard validation gate where no layer loads unless the preceding gate passes.

Data Warehouse analysis:
- Sales performance by region, product type, and time period
- Delivery on-time performance vs planned route metrics
- Exception trends, types, and resolution times
- Priority shipment impact on operational performance
- Route efficiency (planned vs actual stops and hours)

---

## Architecture:

The pipeline follows a 4-layer linear flow:

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
        │  keys, duplicate PKs, FK orphans, metrics sanity
        ▼
Reporting Layer  (reporting schema — SQL views)
        │
        ▼
Power BI Dashboards
        (all aggregations and measures defined in Power BI)
```

## Star Schema Design

### Dimension Tables

All dimensions use **surrogate keys** for referential integrity to decouple source system code from analytics.

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

### Fact Tables

| Fact Table | Grain | Key Measures |
|---|---|---|
| `fact_sales` | One row per sales transaction | UnitsSold, SalesAmount, RevenuePerUnit |
| `fact_deliveries` | One row per delivery | DeliveryDateKey, ExpectedDeliveryDateKey, IsLate, DaysVariance |
| `fact_routes` | One row per route run | PlannedStops, ActualStops, PlannedHours, ActualHours, StopVariance, EfficiencyRatio |
| `fact_exceptions` | One row per exception | ResolutionTimeHours, ResolutionDays, IsResolved, IsDateCorrected |

> `fact_deliveries` uses two separate date keys — `DeliveryDateKey` and `ExpectedDeliveryDateKey` — to support late delivery analysis by expected date.

---

## Script Inventory:

Scripts are numbered for execution order.

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

## ETL Pipeline:

### Staging Layer
Raw CSV data is loaded via `03_load_staging.py` using **pandas + SQLAlchemy**. Each run truncates staging tables before load to prevent duplicates. All loads execute within a single transaction. Any failure triggers a full rollback and no tables are loaded.

### Clean Layer
The clean layer is **view-based** where no data is copied or stored.
- Truncated value expansion (e.g. `'L.'` → `'Large Package'`)
- NULL handling (`DriverID` NULL → `'Unknown'`)
- Derived business flags (`IsLate`, `IsResolved`, `IsBadDateKey`, `IsDateCorrected`)
- Derived metrics (`DaysVariance`, `StopVariance`, `HourVariance`, `EfficiencyRatio`, `RevenuePerUnit`)
- Time dimension extraction (`Year`, `Month`, `Quarter` from date columns)

### Data Warehouse Layer
The full DW load executes in a **single transaction**. Dimensions are loaded before facts. Surrogate keys are resolved via joins at load time. If any step fails, the entire transaction rolls back, leaving the DW in a consistent state.

### Reporting Layer
Reporting views are flat SQL views with no aggregations. All analytical measures are defined in Power BI to maintain a single source for business logic.

---

## Validation Strategy:

### Gate 1 — Staging (`10_etl_staging_validation.sql`)
Catches empty tables, NULL primary keys, negative metric values, referential integrity violations, and date range/chronology errors before the clean layer is built.

### Gate 2 — Clean Layer (`07_clean_validation_gate.sql`)
11 automated checks covering empty view guard, required field NULLs, `IsLate` flag accuracy, `PriorityFlag` normalization, truncation resolution, referential integrity, and route source data validity.

### Gate 3 — DW (`09_dw_validation.sql`)
Validates post-load DW state: empty table guard, row count comparison vs clean views, NULL surrogate keys, duplicate PK detection, FK orphan checks across all fact-to-dimension relationships, business metric sanity, and delivery date logic.

### Informational Scripts:
| Script | When to Run |
|---|---|
| `05_clean_layer_data_profiling.sql` | After clean views created; review before DW load |
| `06_clean_layer_validation.sql` | Human review of clean view quality |
| `11_dw_column_profile.sql` | Deep investigation of NULL rates and data distribution |
| `12_dw_health_audit.sql` | Scheduled production monitoring (daily/weekly) |

---

## Business Questions:
- What is total sales revenue by region, product type, and month?
- Which routes are meeting planned vs actual stop and hour targets?
- Which exception types have the longest resolution times?
- What percentage of deliveries are on time by region and shipment type?
- How do priority shipments compare to standard shipments in on-time performance?
- What is the trend in open vs resolved exceptions over time?

---

## Technologies:

| Category | Tools |
|---|---|
| Database | SQL Server 2019, T-SQL, SSMS |
| ETL / Python | Python 3, pandas, SQLAlchemy, pyodbc |
| Modeling | Kimball Star Schema, Dimensional Modeling |
| BI / Reporting | Power BI, DAX, Power Query |
| Validation | Pipeline gates with THROW, dynamic SQL profiling |

---

## Key Concepts:
- Kimball-style dimensional modeling with surrogate key implementation
- Multi-layer ETL pipeline with hard validation gates at each boundary
- View-based clean layer for zero-copy transformation
- Python ETL with transactional integrity and audit logging
- Dynamic SQL for automated column-level NULL profiling
- Referential integrity enforcement across all fact-to-dimension relationships
- Separation of SQL reporting layer from Power BI analytical measures

---

*Update this README whenever tables, views, business rules, or script filenames change.
