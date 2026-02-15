# Operational Analytics Data Warehouse – Star Schema Design & ETL in SQL Server

## 📌 Project Overview

This project demonstrates the design and implementation of a dimensional data warehouse for delivery operations, sales performance, and exception management reporting.

The solution follows Kimball-style dimensional modeling principles using a Star Schema design, with separate staging and warehouse layers.

The warehouse enables analysis of:

- Sales performance by region and product type
- Delivery performance vs planned route metrics
- On-time delivery tracking
- Exception trends and resolution times
- Priority-based operational reporting

---

## 🏗 Architecture

The solution consists of:

- **Staging Layer** – Raw operational data
- **Dimension Tables** – Descriptive attributes
- **Fact Tables** – Measurable business metrics
- **ETL Scripts** – Data transformation and loading logic

Star schema design ensures optimized analytical queries and simplified reporting.

---

## ⭐ Star Schema Design

### Dimension Tables

- `dim_region`
- `dim_driver`
- `dim_route`
- `dim_date`
- `dim_product_type`
- `dim_shipment_type`
- `dim_delivery_status`
- `dim_exception_type`
- `dim_priority_flag`

All dimensions use **surrogate keys** for referential integrity.

**Dimension Tables (Dims)**

| Dim Table               | Surrogate Key     | Source Columns / Notes                                      |
| ----------------------- | ----------------- | ----------------------------------------------------------- |
| **dim_region**          | RegionKey         | Region (from all staging tables)                            |
| **dim_driver**          | DriverKey         | DriverID, DriverName (NULL), HireDate (NULL), Status (NULL) |
| **dim_route**           | RouteKey          | RouteID, Region (optional if route fixed)                   |
| **dim_date**            | DateKey           | FullDate, Year, Month, Quarter, Week, DayOfWeek             |
| **dim_product_type**    | ProductTypeKey    | ProductType (from `staging_sales`)                          |
| **dim_shipment_type**   | ShipmentTypeKey   | ShipmentType (from `staging_deliveries`)                    |
| **dim_delivery_status** | DeliveryStatusKey | DeliveryStatus (from `staging_deliveries`)                  |
| **dim_exception_type**  | ExceptionTypeKey  | ExceptionType (from `staging_exceptions`)                   |
| **dim_priority_flag**   | PriorityFlagKey   | PriorityFlag (from deliveries + exceptions)                 |
---

### Fact Tables

- `fact_sales`
- `fact_routes`
- `fact_deliveries`
- `fact_exceptions`

**Fact Tables**

| Fact Table          | Measures / Metrics
| ------------------- | -----------------------------------------------------
| **fact_sales**      | UnitsSold, SalesAmount
| **fact_routes**     | PlannedStops, ActualStops, PlannedHours, ActualHours
| **fact_deliveries** | all descriptive via FKs
| **fact_exceptions** | ResolutionTimeHours

Fact tables store measurable metrics and reference dimension surrogate keys.

---

## 🔄 ETL Process

1. Load raw data into staging tables
2. Populate dimension tables using DISTINCT business keys
3. Transform and cleanse data (TRIM, NULL handling)
4. Load fact tables using surrogate key lookups
5. Enforce referential integrity

---

## 📊 Example Business Questions Answered

- What is total sales by region and month?
- Are routes meeting planned vs actual targets?
- Which exception types have the longest resolution times?
- What percentage of deliveries are on time?
- How do priority shipments impact operational performance?

---

## 🛠 Technologies Used

- SQL Server
- T-SQL
- Star Schema Modeling
- Dimensional Data Modeling
- ETL Design Principles

---

## 📈 Key Concepts Demonstrated

- Dimensional modeling (Kimball methodology)
- Surrogate key implementation
- Fact vs Dimension table design
- Data cleansing in ETL
- Referential integrity management
- Analytical query optimization

---

## 📎 Author

Dexter Boyd
www.linkedin.com/in/dexter-boyd


