# Data Lineage Documentation

This document describes the **flow of data** from source files to analytical outputs, including transformations at each stage.

---

## 1. Data Source

- CSV files imported into SQL Server **dbo schema**
- Source tables include:
  - `sales.csv` → `dbo.staging_sales`
  - `deliveries.csv` → `dbo.staging_deliveries`
  - `exceptions.csv` → `dbo.staging_exceptions`
  - `routes.csv` → `dbo.staging_routes`

---

## 2. Staging Layer

- Raw data is moved from `dbo` to `staging` schema
- Minimal transformations:
  - Data types aligned
  - Null checks for critical columns
  - Preliminary row validation
- Tables:
  - `staging.staging_sales`
  - `staging.staging_deliveries`
  - `staging.staging_exceptions`
  - `staging.staging_routes`

---

## 3. Clean Layer

- **Views transform and cleanse staging data**
- Standardizations include:
  - Trimming text fields
  - Normalizing numeric fields
  - Flagging late deliveries
  - Normalizing priority flags
  - Correcting negative or invalid values
- Views:
  - `clean.vw_sales`
  - `clean.vw_deliveries`
  - `clean.vw_exceptions`
  - `clean.vw_routes`

---

## 4. Data Warehouse Layer

- Clean views populate **dimensional tables** (DW)
- Dimension loading:
  - `dim_date`, `dim_region`, `dim_product_type`, `dim_driver`, `dim_route`, `dim_shipment_type`, `dim_delivery_status`, `dim_exception_type`, `dim_priority_flag`
- Fact tables:
  - `fact_sales`, `fact_deliveries`, `fact_routes`, `fact_exceptions`
- Surrogate keys used to enforce relationships

---

## 5. Reporting Layer

- Analytical views built on DW tables
- Simplifies querying for BI tools
- Examples:
  - `reporting.vw_sales_summary`
  - `reporting.vw_delivery_performance`
  - `reporting.vw_exception_dashboard`
  - `reporting.vw_route_efficiency`

---

## 6. Power BI Dashboards

- Connect directly to **reporting views**
- Support interactive metrics and visualizations:
  - Sales trends
  - Delivery performance
  - Exceptions tracking
  - Route efficiency

---

## 7. Lineage Flow Diagram

CSV Sources (dbo)
│
▼
Staging Tables (staging)
│
▼
Clean Views (clean)
│
▼
DW Tables (dw)
├─ Dimensions
└─ Facts
│
▼
Reporting Views (reporting)
│
▼
Power BI Dashboards


**Notes:**

- Each arrow represents **ETL transformation or load**
- Business rules applied at the **Clean Layer** ensure warehouse data integrity
- Surrogate keys enforce dimensional relationships
- Reporting views are **read-only** for BI consumption