# Data Lineage - ETL Pipeline

This document traces the flow of data from raw sources through the ETL pipeline to the final reporting layer.

---

## **1. Source Layer**
| Source | Format | Description |
|--------|--------|-------------|
| Raw CSV files | CSV | Original sales, delivery, exception, and route data |

**Notes:**
- CSVs are loaded into the database `dbo` schema for initial import.
- Data quality checks at this stage are minimal; only import validation (e.g., file integrity, date parsing).

---

## **2. Staging Layer**
| Table | Source | Purpose |
|-------|--------|---------|
| staging.staging_sales | dbo CSV imports | Raw sales transactions with minimal transformations |
| staging.staging_deliveries | dbo CSV imports | Raw deliveries data |
| staging.staging_routes | dbo CSV imports | Raw route data |
| staging.staging_exceptions | dbo CSV imports | Raw exceptions and issue tracking data |

**Notes:**
- Staging ensures all imported tables are in a consistent schema.
- Optional validation checks include row counts, NULLs for critical columns, and basic type enforcement.
- No business rules applied at this stage.

---

## **3. Clean Layer (Views)**
| View | Source | Purpose |
|------|--------|---------|
| clean.vw_sales | staging.staging_sales | Cleans sales data, enforces numeric types, trims text, removes invalid records |
| clean.vw_deliveries | staging.staging_deliveries | Flags late deliveries, normalizes priority flags |
| clean.vw_routes | staging.staging_routes | Validates route metrics, prevents negative/zero values |
| clean.vw_exceptions | staging.staging_exceptions | Cleans exception data, prevents invalid resolution dates |

**Notes:**
- Clean layer standardizes data for DW load.
- Primary validation occurs here: NULL checks, business rules, optional profiling.
- Acts as a single source of truth for DW dimension and fact table loading.

---

## **4. Data Warehouse (dw schema)**
| Table | Source | Purpose |
|-------|--------|---------|
| dw.dim_date | clean.vw_sales | Stores calendar date surrogate keys |
| dw.dim_product_type | clean.vw_sales | Stores product type surrogate keys |
| dw.dim_region | clean views (sales/deliveries/exceptions/routes) | Stores region surrogate keys |
| dw.dim_driver | clean.vw_deliveries, clean.vw_routes | Stores driver surrogate keys |
| dw.dim_route | clean.vw_deliveries, clean.vw_routes | Stores route surrogate keys |
| dw.dim_shipment_type | clean.vw_deliveries | Stores shipment type surrogate keys |
| dw.dim_delivery_status | clean.vw_deliveries | Stores delivery status surrogate keys |
| dw.dim_exception_type | clean.vw_exceptions | Stores exception type surrogate keys |
| dw.dim_priority_flag | clean.vw_deliveries, clean.vw_exceptions | Stores priority flags |

| Table | Source | Purpose |
|-------|--------|---------|
| dw.fact_sales | clean.vw_sales + dimension lookups | Fact table for sales transactions |
| dw.fact_deliveries | clean.vw_deliveries + dimension lookups | Fact table for deliveries |
| dw.fact_routes | clean.vw_routes + dimension lookups | Fact table for route metrics |
| dw.fact_exceptions | clean.vw_exceptions + dimension lookups | Fact table for exceptions |

**Notes:**
- DW tables enforce referential integrity via surrogate keys.
- Facts join to dimensions for BI analysis.
- Clean layer is the trusted source for all DW loads.

---

## **5. Reporting Layer**
| Object | Source | Purpose |
|--------|--------|---------|
| reporting.vw_sales_summary | DW fact_sales, dim_product_type, dim_region, dim_date | Aggregated sales metrics and revenue for Power BI dashboards; business-friendly columns and calculations |
| reporting.vw_delivery_performance | DW fact_deliveries, dim_route, dim_driver, dim_shipment_type, dim_delivery_status, dim_priority_flag, dim_date | KPIs for delivery operations; includes late deliveries, priority delivery counts, total deliveries per route and driver |
| reporting.vw_exception_dashboard | DW fact_exceptions, dim_exception_type, dim_priority_flag, dim_region, dim_date | Tracks exceptions and resolution times; aggregates priority exceptions and provides regional breakdowns |
| reporting.vw_route_efficiency | DW fact_routes, dim_route, dim_driver, dim_region | Evaluates route performance and efficiency; computes stop completion ratio and hour efficiency ratio per route and driver |

**Notes:**
- Reporting layer can contain materialized views or live queries.
- Designed for end-user consumption via Power BI or other BI tools.

---

## **6. Data Flow Diagram (Textual)**

Raw CSV (dbo)
│
▼
Staging Tables (staging schema)
│
▼
Clean Views (clean schema)
│
▼
DW Tables (dw schema) → Dimensions & Facts
│
▼
Reporting Views (reporting schema)
│
▼
Power BI / Analytics


---

## **7. Key Points**
- Data validation occurs at staging (basic) and clean layers (full).
- Surrogate keys in DW prevent FK violations and allow integration of multiple sources.
- Reporting layer depends entirely on DW fact tables.
- All transformations are documented in clean views and DW load scripts.