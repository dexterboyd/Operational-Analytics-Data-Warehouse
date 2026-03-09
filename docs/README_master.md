# Data Warehouse & BI Project

This repository contains all scripts, documentation, and metadata for the **End-to-End Data Warehouse & BI solution**.  
It covers **raw data ingestion, staging, cleaning, warehouse loading, reporting, and analytics**.

---

## Table of Contents

1. [Project Overview](#project-overview)  
2. [Architecture](#architecture)  
3. [Requirements Analysis](#requirements-analysis)  
4. [ETL Pipeline](#etl-pipeline)  
5. [Data Dictionary](#data-dictionary)  
6. [Data Lineage](#data-lineage)  
7. [Validation & Quality Checks](#validation--quality-checks)  
8. [ETL Execution Steps](#etl-execution-steps)  
9. [Reporting & BI](#reporting--bi)  

---

## Project Overview

**Objective:**  
Design, build, and deploy a SQL Server-based data warehouse to enable accurate, timely, and actionable business intelligence for sales, delivery operations, routes, and exceptions tracking.

**Scope:**  
- Ingest CSV source data  
- Standardize and validate staging data  
- Build a clean layer for business rules & transformations  
- Load dimensional and fact tables into the DW  
- Enable reporting via views and BI tools (Power BI)

---

## Architecture

CSV Sources (dbo)
│
▼
Staging Layer (staging schema)
│
▼
Clean Layer (clean views)
│
▼
Data Warehouse (dw schema)
├─ Dimensions
└─ Facts
│
▼
Reporting Layer (reporting views)
│
▼
Power BI Dashboards


**Features:**  
- Safe schema creation and object transfers  
- Surrogate key management to prevent FK violations  
- Logging of all ETL actions  
- Validation and profiling at each layer  

---

## Requirements Analysis

### 1. Building the Data Warehouse (Data Engineering)
**Objective:**  
Create a structured, normalized, and validated data warehouse to support analytics.

**Specifications:**  
- Staging tables mirror source CSV data  
- Clean views apply business rules and data quality logic  
- DW tables include dimensions and facts with surrogate keys  
- Referential integrity enforced via foreign keys  

### 2. BI: Analytics and Reporting (Data Analysis)
**Objective:**  
Enable fast, accurate reporting for operational KPIs and metrics.

**Specifications:**  
- Reporting views summarize sales, delivery, routes, and exceptions  
- Power BI dashboards consume reporting views  
- Metrics include late deliveries, priority shipments, route efficiency, and exceptions  

---

## ETL Pipeline

| Step | Layer | Description |
|------|-------|-------------|
| 1 | Staging | Move CSV source data from `dbo` to `staging` schema |
| 2 | Clean | Apply cleansing, validation, and business rules via `clean` views |
| 3 | DW Dimensions | Load dimension tables from clean views with surrogate keys |
| 4 | DW Facts | Load fact tables from clean views using dimensional surrogate keys |
| 5 | Reporting | Build reporting views for analytics and dashboards |
| 6 | Validation | Execute row counts, null checks, business rule checks, and profiling |

---

## Data Dictionary

Refer to [data_dictionary.md](./data_dictionary.md) for a complete listing of:

- Staging tables  
- Clean views  
- DW dimension & fact tables  
- Reporting views  

---

## Data Lineage

Refer to [data_lineage.md](./data_lineage.md) for a visual and tabular description of **data flow** from sources to Power BI dashboards.

---

## Validation & Quality Checks

**Key validations included:**

- Staging Layer: row counts, NULL checks on critical columns  
- Clean Layer: row count comparison, NULL checks, business rule validation, optional data profiling  
- DW Layer: surrogate key consistency, referential integrity  

**Scripts:**  
- `staging_validation.sql`  
- `clean_layer_validation.sql`  
- `dw_data_quality_audit.sql`  

---

## ETL Execution Steps

1. **Schema Setup & Object Transfer**  
   - `schema_setup.sql` → creates schemas (`staging`, `clean`, `dw`, `reporting`) and moves objects.  

2. **Clean Layer Views**  
   - `clean_layer_views.sql` → standardizes and validates staging tables.  

3. **Staging & Clean Validation**  
   - Run `staging_validation.sql` → checks staging row counts and nulls.  
   - Run `clean_layer_validation.sql` → validates clean views.  

4. **DW Load**  
   - `dw_load.sql` → loads dimensions and facts from clean views.  
   - `dw_data_quality_audit.sql` → validates DW tables, row counts, nulls, and business rules.  

5. **Reporting Views**  
   - `reporting_views.sql` → prepares summary and KPI views for BI consumption.  

6. **Power BI Dashboards**  
   - Connect dashboards to `reporting` views.  

---

## Reporting & BI

- Summary Views: `vw_sales_summary`, `vw_delivery_performance`, `vw_exception_dashboard`, `vw_route_efficiency`  
- KPIs:  
  - Total sales by region/product  
  - Late delivery percentage  
  - Exceptions count & resolution time  
  - Route performance metrics  
- Dashboards: interactive, drillable, and live-connected to the warehouse  

---

**Notes:**  

- This README serves as **master documentation** for ETL execution, validation, and BI usage.  
- Update when new sources, tables, views, or business rules are added.  