System Architecture & Data Model Documentation
1. System Overview

The system implements a layered data architecture designed to transform raw operational data into a structured analytical platform. The architecture separates data ingestion, transformation, storage, and reporting to ensure scalability, maintainability, and reliability.

The system consists of the following major components:

• Data ingestion from raw CSV files

• A multi-stage ETL pipeline

• A dimensional data warehouse

• A reporting layer optimized for BI tools

• Interactive dashboards built in Power BI
-------------------------------------------------------------------

2. High-Level Architecture

The system follows a layered data pipeline architecture.

CSV Source Files
        │
        ▼
SQL Server (dbo schema)
Raw data import
        │
        ▼
Staging Layer
Initial structured storage
        │
        ▼
Clean Layer
Data transformation and validation
        │
        ▼
Data Warehouse (dw schema)
Star schema for analytics
        │
        ▼
Reporting Layer
Business-friendly reporting views
        │
        ▼
Power BI
Dashboards and analytics

* Each layer has a clearly defined responsibility within the pipeline.
-------------------------------------------------------------------

3. Data Pipeline Architecture

The ETL pipeline orchestrates data processing across several stages.

Pipeline Execution Flow
1. Schema Setup
2. Data Ingestion (CSV → SQL Server)
3. Staging Layer Processing
4. Clean Layer Transformations
5. Data Warehouse Load
6. Data Validation
7. Data Quality Auditing
8. Reporting Layer View Creation
9. Power BI Data Refresh

* A master ETL controller script coordinates execution to ensure that each step runs in the correct order.
-------------------------------------------------------------------

4. Schema Architecture

The SQL Server database is organized into multiple schemas to enforce separation of responsibilities.

| Schema        | Purpose                                  |
| ------------- | ---------------------------------------- |
| **dbo**       | Raw imported source data                 |
| **staging**   | Intermediate storage for source datasets |
| **clean**     | Data transformation and validation layer |
| **dw**        | Dimensional data warehouse               |
| **reporting** | Analytical views for BI tools            |


* This layered approach improves maintainability and makes the ETL process easier to manage.
-------------------------------------------------------------------

5. Data Warehouse Model

The warehouse uses a star schema design, which is widely used in analytical databases due to its performance and simplicity.

Fact Tables

Fact tables store measurable business events.

Examples include:

• fact_sales – records sales transactions

• fact_deliveries – records delivery performance

• fact_routes – route operational metrics

• fact_exceptions – operational exceptions and incidents

* Fact tables contain foreign keys linking to dimensions along with measurable metrics.

Dimension Tables

Dimension tables store descriptive attributes used for filtering and grouping data.

Examples include:

• dim_date

• dim_region

• dim_product

• dim_driver

• dim_route

• dim_shipment_type

• dim_delivery_status

• dim_exception_type

• dim_priority_flag

* Dimensions provide contextual information for fact records.
-------------------------------------------------------------------

6. Star Schema Diagram

The conceptual star schema structure is shown below.

                dim_date
                    │
                    │
                    ▼
dim_region ─── fact_sales ─── dim_product
                    │
                    │
                    ▼
              dim_priority_flag


For delivery analytics:

              dim_driver
                  │
                  ▼
dim_route ── fact_deliveries ── dim_shipment_type
                  │
                  ▼
           dim_delivery_status

* Fact tables serve as the central nodes connecting multiple dimensions.
-------------------------------------------------------------------

7. ETL Monitoring and Logging

To support operational reliability, the system includes ETL logging and monitoring tables.

These track:

• ETL execution start and end times

• Pipeline success or failure status

• Rows processed per pipeline step

• Error messages for failed runs

Example tables:

dw.etl_run_log
dw.etl_step_log

* This monitoring system provides visibility into pipeline health and performance.
-------------------------------------------------------------------

8. Data Quality and Validation

The pipeline includes automated quality checks to ensure data reliability.

Quality checks include:

• Row count validation between pipeline stages

• Required field validation

• Referential integrity checks

• Duplicate key detection

• Null percentage profiling

• Data distribution analysis

* These checks help prevent invalid or incomplete data from reaching the reporting layer.
-------------------------------------------------------------------

9. Reporting Layer

The reporting layer provides business-ready views built on top of the warehouse.

Examples include:

• vw_sales_summary

• vw_route_efficiency

• vw_exception_dashboard

• vw_delivery_performance

* These views simplify the warehouse structure and provide datasets optimized for BI tools.

* Power BI dashboards connect directly to these views rather than querying raw warehouse tables.
-------------------------------------------------------------------

10. Business Intelligence Layer

Power BI dashboards enable users to analyze operational and sales performance through visualizations such as:

• Sales trends over time

• Sales by region or product

• Delivery performance metrics

• Exception tracking dashboards

• Operational efficiency metrics

* The BI layer transforms warehouse data into actionable insights for decision-makers.
