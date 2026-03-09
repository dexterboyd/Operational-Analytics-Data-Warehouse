Requirements Analysis

Project Overview
This project builds an end-to-end Business Intelligence (BI) solution consisting of a data warehouse pipeline and analytical reporting layer. The system ingests raw CSV datasets, processes them through a structured ETL pipeline, and produces analytics-ready datasets used for reporting and business insights.

The architecture follows a layered data engineering design:

Raw Data → Staging → Clean Layer → Data Warehouse → Reporting Layer → BI Dashboards

The solution is divided into two major components:

1. Building the Data Warehouse (Data Engineering)

2. BI Analytics and Reporting (Data Analysis)
-------------------------------------------------------------------

1. Building the Data Warehouse (Data Engineering)
Objective

The objective of the Data Engineering component is to design and implement a scalable, reliable data pipeline that ingests raw operational data and transforms it into a structured star schema data warehouse optimized for analytical workloads.

The warehouse ensures:

✔ Data consistency and integrity

✔ Efficient query performance

✔ Clean and validated datasets

✔ Traceable ETL processes

✔ A centralized data source for business analytics
-------------------------------------------------------------------

Specifications

Data Sources:
The system ingests structured datasets stored as CSV files, including operational records such as:

• Sales transactions

• Delivery operations

• Route and logistics information

• Exception and operational event records

These datasets represent raw operational data that require transformation before analytical use.
-------------------------------------------------------------------

Data Ingestion

Raw CSV data is imported into SQL Server into the dbo schema and then moved into the staging layer for initial processing.

The staging layer preserves the original structure of the source data while preparing it for transformation.
-------------------------------------------------------------------

Data Architecture

The warehouse follows a multi-layer architecture:

| Layer         | Purpose                                          |
| ------------- | ------------------------------------------------ |
| **dbo (raw)** | Initial import of CSV source files               |
| **staging**   | Stores raw structured data for ETL processing    |
| **clean**     | Applies data validation and transformation logic |
| **dw**        | Stores the dimensional data warehouse schema     |
| **reporting** | Provides curated views for BI tools              |

-------------------------------------------------------------------

Data Transformation

Data transformations are implemented through SQL views and ETL scripts that perform:

• Data type normalization

• Removal of invalid or incomplete records

• Standardization of categorical fields

• Derivation of calculated fields

• Validation of required fields

This process produces clean, analysis-ready datasets.
-------------------------------------------------------------------

Data Warehouse Schema
The warehouse implements a star schema design, consisting of:

Dimension Tables
Dimension tables provide descriptive attributes used for analysis.

Examples include:

• Date dimension

• Region dimension

• Product dimension

• Driver dimension

• Route dimension

• Shipment type dimension

• Delivery status dimension

• Exception type dimension

• Priority flag dimension
-------------------------------------------------------------------

Fact Tables
Fact tables store measurable business events.

Examples include:

• Sales transactions

• Delivery performance metrics

• Route operations

• Operational exceptions

Fact tables reference dimension tables through surrogate keys, enabling efficient analytical queries.
-------------------------------------------------------------------

Data Validation and Quality Control
Multiple validation stages ensure data quality throughout the pipeline:

1. Staging validation
   Verifies row counts and schema integrity after ingestion.

2. Clean layer validation
   Ensures required fields are populated and transformations are applied correctly.

3. Data warehouse validation
   Confirms referential integrity between fact and dimension tables.

4. Data quality audit
   Automatically profiles tables for null values, duplicates, and data distribution anomalies.
-------------------------------------------------------------------

ETL Orchestration
The pipeline execution is coordinated through a master ETL controller script that ensures correct execution order across all stages:

1.Schema setup

2. Staging validation

3. Clean layer transformations

4. Data warehouse loading

5. Data warehouse validation

6. Data quality auditing

7. Reporting layer generation

The pipeline also includes ETL logging and monitoring to track:

Pipeline execution time

• Success or failure status

• Rows processed per step

• Error messages

Metadata and Documentation
The project includes automated metadata generation and documentation:

• Data dictionary for all warehouse tables and columns

• Data lineage documentation describing pipeline flow

• Warehouse architecture diagrams

This ensures transparency and maintainability of the data platform.
-------------------------------------------------------------------

2. BI: Analytics and Reporting (Data Analysis)
Objective

The objective of the BI component is to transform warehouse data into actionable business insights through interactive dashboards and analytical reporting.

The reporting layer provides curated datasets designed for efficient consumption by business intelligence tools such as Power BI.

The analytics solution enables stakeholders to:

• Monitor operational performance

• Identify trends and anomalies

• Evaluate sales and logistics performance

• Track delivery efficiency

• Analyze operational exceptions

The BI layer is built on reporting views derived from the data warehouse, ensuring that analytical queries do not directly impact warehouse performance.

These views provide simplified datasets optimized for visualization and reporting tools.