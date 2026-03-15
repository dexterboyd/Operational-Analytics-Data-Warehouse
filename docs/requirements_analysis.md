# Requirements Analysis
**Project:** FedEx Operations Data Warehouse
**Version:** 2.0
**Last Updated:** 2026

---

## Project Overview

This project designs, builds, and validates a SQL Server-based end-to-end data warehouse and business intelligence solution for FedEx delivery operations. The system ingests raw CSV operational data, processes it through a layered ETL pipeline with hard validation gates at each boundary, and produces analytics-ready datasets consumed by Power BI dashboards.

The architecture follows a multi-layer data engineering design with strict separation of responsibility at each stage:

```
Raw CSV Data → Staging → Clean Layer → Data Warehouse → Reporting Layer → Power BI
```

The solution is divided into two major components:

1. **Building the Data Warehouse** — Data Engineering
2. **BI Analytics and Reporting** — Data Analysis

---

## Part 1: Building the Data Warehouse (Data Engineering)

### Objective

Design and implement a scalable, reliable, and auditable ETL pipeline that ingests raw operational data and transforms it into a validated star-schema data warehouse optimized for analytical workloads.

The warehouse must ensure:

- Data consistency and integrity across all pipeline layers
- Efficient query performance via a star-schema dimensional model
- Clean, validated datasets with documented business rule application
- Traceable ETL processes with run-level and step-level logging
- A centralized, trusted data source for all business analytics
- Pipeline safety: any validation failure halts execution before the next layer loads

---

### Specifications

#### Data Sources

The system ingests four structured datasets from CSV files representing raw operational records:

| File | Content |
|------|---------|
| `sales.csv` | Sales transactions tied to deliveries |
| `deliveries.csv` | Delivery records with route, driver, date, and status |
| `routes.csv` | Planned vs actual route performance per driver |
| `exceptions.csv` | Operational issues and incidents affecting deliveries |

These datasets require full transformation before analytical use. No business rules are applied at the source.

---

#### Data Ingestion Requirements

- All four CSV files are loaded via `BULK INSERT` into the `staging` schema
- The load path is configured once via a single `@DataPath` variable — not hardcoded in four separate statements
- `CODEPAGE = '65001'` (UTF-8) must be specified to prevent silent encoding corruption of non-ASCII characters
- `MAXERRORS = 0` with an `ERRORFILE` per table ensures any malformed row produces a diagnostic file rather than silently aborting the load
- All four loads run inside a single transaction — if any file fails, all four tables are rolled back to a clean empty state
- The staging layer preserves the original structure of source data while enforcing `NOT NULL` on all required identifier and date columns

---

#### Data Architecture

The warehouse follows a five-layer architecture with clear responsibilities at each layer:

| Layer | Schema | Purpose |
|-------|--------|---------|
| Source | *(file system)* | Raw CSV files before import |
| Staging | `staging` | Raw structured data loaded from CSVs; minimal transformation; NOT NULL on required fields |
| Clean | `clean` | SQL views that apply business rules, data type validation, text normalization, and quality flags |
| Data Warehouse | `dw` | Star-schema dimensional model with IDENTITY surrogate keys and explicit PK/FK constraints |
| Reporting | `reporting` | Flat SQL views over DW tables; one row per fact row; no aggregations; consumed by Power BI |

---

#### Data Transformation Requirements

Transformations are applied as SQL views in the clean layer. The DW load reads exclusively from clean views. The following transformations are required:

| Category | Requirement |
|----------|-------------|
| Text normalization | All text columns must be `UPPER(LTRIM(RTRIM()))` to prevent duplicate dimension values |
| Date validation | `DateKey` in sales must be validated within a plausible range using `DATE` literals (not integer bounds) |
| Date audit flag | Rows with invalid DateKey are retained with `IsBadDateKey = 1` for audit; not silently dropped |
| Late delivery rule | If `DeliveryDate > ExpectedDeliveryDate`, `DeliveryStatus` must be forced to `'LATE'` (all caps); this exact string must be used in all downstream comparisons |
| Priority normalization | `PriorityFlag` BIT must be normalized to strict INT 0 or 1 via direct comparison — no VARCHAR casting |
| Chronology correction | If `ResolvedDate` precedes `DateReported`, set `ResolvedDate` to `DateReported` as floor value; flag row with `IsDateCorrected = 1` |
| ResolutionTimeHours derivation | If stored value is negative but dates are valid, derive from `DATEDIFF(HOUR, DateReported, ResolvedDate)` rather than returning NULL |
| Route metrics | `EfficiencyRatio` (ActualHours / PlannedHours) and `StopVariance` (ActualStops - PlannedStops) must be derived once in the clean layer and carried through to fact_routes |
| Row exclusion | Rows missing required identifiers or containing invalid metrics (UnitsSold ≤ 0, SalesAmount ≤ 0, stops/hours ≤ 0) must be excluded |

---

#### Data Warehouse Schema Requirements

##### Dimension Tables

Dimension tables provide descriptive context for fact records. All dimensions use IDENTITY surrogate keys.

| Dimension | Source | Key Requirement |
|-----------|--------|----------------|
| dim_date | All date columns across clean views | DateKey as INT in YYYYMMDD format; loaded first before all facts |
| dim_product_type | clean.vw_sales only | Not from vw_deliveries — that view has no ProductType column |
| dim_region | UNION of all four clean views | All regional values across every pipeline entity |
| dim_driver | vw_deliveries ∪ vw_routes | Column named `DriverCode` (stores source ID code, not a display name) |
| dim_route | vw_deliveries ∪ vw_routes | Column named `RouteCode` (stores source ID code, not a display name) |
| dim_shipment_type | vw_deliveries | — |
| dim_delivery_status | vw_deliveries | Canonical late value must be `'LATE'` (all caps) |
| dim_exception_type | vw_exceptions | — |
| dim_priority_flag | vw_deliveries ∪ vw_exceptions | Values: 0 = standard, 1 = high priority |

##### Fact Tables

Fact tables store measurable business events. All fact tables must have explicit `PRIMARY KEY` constraints.

| Fact Table | Primary Key | Key Requirement |
|------------|-------------|----------------|
| fact_sales | SalesID | One row per transaction; SalesAmount is total revenue, not per-unit price |
| fact_deliveries | DeliveryID | Must store two date keys: `DeliveryDateKey` AND `ExpectedDeliveryDateKey` — both pointing to dim_date — to enable late-delivery analysis |
| fact_routes | (RouteID, DriverID) composite | Must include `EfficiencyRatio` and `StopVariance` columns from clean layer |
| fact_exceptions | ExceptionID | Must include `IsDateCorrected` flag from clean layer for audit use |

##### Date Key Conversion

All `DATE` columns from clean views must be converted to `INT` (YYYYMMDD) using the canonical formula:
```sql
CONVERT(INT, CONVERT(VARCHAR(8), <date_col>, 112))
```
This conversion must be applied identically in every location it appears.

##### Load Safety

- The entire DW load (drop, create, populate) must run inside a single `BEGIN TRANSACTION / TRY / CATCH`
- Facts must be dropped before dimensions (FK dependency order) and loaded after dimensions (same reason)
- Any load failure must roll back all changes and surface a clear error message

---

#### Validation and Quality Control Requirements

The pipeline must include three hard validation gates. Each gate must:

1. Start with an empty-table guard — a silent load failure leaves empty tables that cause all downstream checks to pass vacuously
2. Use `THROW` to halt execution on failure — `RAISERROR` severity 10 is informational only and does not stop the pipeline
3. Include `SET XACT_ABORT ON` at the top for safe transactional use

| Gate | Script | Validates |
|------|--------|-----------|
| Staging Gate | `03_etl_staging_validation.sql` | Empty tables, NULL keys, negative values, referential integrity, date range/chronology |
| Clean Gate | `07_clean_validation_gate.sql` | Empty views, NULL fields, `'LATE'` flag, PriorityFlag, referential integrity |
| DW Gate | `09_dw_validation.sql` | Empty tables, row count drops vs clean views, NULL surrogate keys, duplicate PKs, FK orphans, metric sanity, date logic |

In addition, non-blocking informational scripts run after each gate for human review:
- `05_clean_layer_data_profiling_v2_0.sql` — profiling before DW load
- `06_clean_layer_validation_v2_0.sql` — readable validation results
- `10_dw_health_audit.sql` — ongoing production monitoring
- `11_dw_column_profile.sql` — deep NULL profiling

---

#### ETL Logging Requirements

The pipeline must include an ETL logging framework to track execution at the run level and step level.

| Requirement | Detail |
|-------------|--------|
| Run log | One record per pipeline execution with start/end times, status, and error message |
| Step log | One record per ETL step with real duration (not zero — StartTime must be captured before the step, EndTime at insert) |
| UTC timestamps | All timestamps must use `SYSUTCDATETIME()` — not `GETDATE()` (local time is unreliable across time zones) |
| Status constraint | Both tables must enforce `CHECK (Status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED'))` |
| FK constraint | `etl_step_log.RunID` must have an active FK to `etl_run_log(RunID)` — orphaned step records must not be possible |
| Stored procedures | Three procedures required: `usp_start_etl_run` (returns RunID), `usp_log_etl_step`, `usp_end_etl_run` |

---

#### Metadata and Documentation Requirements

The project must include automated metadata generation and living documentation:

| Deliverable | Script / File |
|-------------|--------------|
| Column catalog | `14_generate_data_dictionary.sql` — all columns with types, nullability, ordinal position |
| PK catalog | Same script — filtered strictly to PRIMARY KEY constraints (not UNIQUE or FK) |
| FK catalog | Same script — FK relationships with source column, referenced table, referenced column |
| Extended property descriptions | Same script — surfaces `MS_Description` properties via `sys.extended_properties` |
| Data dictionary | `data_dictionary.md` — complete column reference for all layers |
| Data lineage | `data_lineage.md` — end-to-end flow with column-level transformation detail |
| Architecture | `system_architecture.md` — system design, schema model, star schema design |
| Master documentation | `ETL_and_DW_Master_Documentation.md` — consolidated pipeline reference |
| Methodology guide | `ETL_Methodology_Guide.docx` — design principles for developers and analysts |

---

### ETL Script Execution Order

Scripts must be numbered and executed in this exact order. No two scripts may share the same number prefix.

```
01_initialize_database.sql          (one-time setup — DESTRUCTIVE)
02_etl_staging_setup.sql
03_etl_staging_validation.sql       ← Gate 1
04_clean_layer_views_v2_0.sql
05_clean_layer_data_profiling_v2_0.sql
06_clean_layer_validation_v2_0.sql
07_clean_validation_gate.sql        ← Gate 2
08_dw_load.sql
09_dw_validation.sql                ← Gate 3
10_dw_health_audit.sql
11_dw_column_profile.sql
12_reporting_views.sql
13_etl_logging_setup.sql            (one-time setup)
14_generate_data_dictionary.sql
```

---

## Part 2: BI Analytics and Reporting (Data Analysis)

### Objective

Transform validated warehouse data into actionable business insights through Power BI dashboards and analytical reporting. The BI layer must:

- Enable stakeholders to monitor operational performance in real time
- Identify trends, anomalies, and performance gaps across sales, deliveries, routes, and exceptions
- Surface KPIs for late delivery rate, route efficiency, exception resolution time, and priority shipment impact
- Be decoupled from warehouse query performance — all BI queries must target reporting views, not raw DW tables

---

### Reporting Layer Requirements

Reporting views must be flat joins over fact + dimension tables with no aggregations in SQL. The grain of each view must match its source fact table.

| View | Grain | Business Purpose |
|------|-------|-----------------|
| `vw_sales_summary` | One row per sales transaction | Sales analysis by product type, region, and time period |
| `vw_delivery_performance` | One row per delivery | On-time performance; late delivery tracking by route, driver, and region |
| `vw_exception_dashboard` | One row per exception | Exception monitoring by type, region, and resolution time; audit of corrected dates |
| `vw_route_efficiency` | One row per route+driver | Route performance vs plan; efficiency ratio and stop variance by driver and region |

**Reporting view design rules:**
- No `GROUP BY` — aggregations belong in Power BI, not SQL views
- No derived aggregates (no `SUM`, `COUNT`, `AVG` columns)
- `EfficiencyRatio` and `StopVariance` are read directly from `fact_routes` — not recomputed in the view
- `SalesAmount` is passed through directly — `UnitsSold × SalesAmount` must not be calculated in the view (SalesAmount is already the total transaction amount)
- `Route` and `Driver` columns expose source codes (`RouteCode`, `DriverCode`), not fabricated display names

---

### Power BI Requirements

| Requirement | Detail |
|-------------|--------|
| Connection | DirectQuery or Import from `reporting` schema views |
| All measures in Power BI | SUM, DIVIDE, CALCULATE, percentage calculations — all defined as DAX measures |
| Late delivery metric | `DIVIDE(COUNTROWS(FILTER(table, DeliveryStatus = "LATE")), COUNTROWS(table))` |
| Revenue metric | `SUM(SalesAmount)` — not `SUM(UnitsSold * SalesAmount)` |
| Route efficiency | `AVERAGE(EfficiencyRatio)` from vw_route_efficiency |
| Stop completion rate | `DIVIDE(SUM(ActualStops), SUM(PlannedStops))` |
| Date intelligence | Use `SaleYear`, `SaleQuarter`, `SaleMonth` columns from `vw_sales_summary` |

---

### Key Business Questions the BI Layer Must Answer

| Question | Source View |
|----------|-------------|
| What is total sales revenue by region, product type, and month? | vw_sales_summary |
| Which deliveries were late, and what routes/drivers had the most? | vw_delivery_performance |
| What percentage of deliveries were on time overall and by region? | vw_delivery_performance |
| Which exception types have the longest average resolution time? | vw_exception_dashboard |
| Which routes are running over their planned hours? | vw_route_efficiency |
| Which drivers are completing fewer stops than planned? | vw_route_efficiency |
| How many exceptions had their dates corrected by the clean layer? | vw_exception_dashboard |
| What is the priority shipment rate and how does it vary by region? | vw_delivery_performance |

---

*Update this document when business requirements change, new data sources are added, or pipeline stages are modified.*
