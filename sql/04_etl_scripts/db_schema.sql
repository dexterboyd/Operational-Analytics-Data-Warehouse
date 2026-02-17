CREATE SCHEMA staging;
CREATE SCHEMA clean;
CREATE SCHEMA dw;
CREATE SCHEMA reporting;

ALTER SCHEMA staging TRANSFER dbo.staging_sales;
ALTER SCHEMA staging TRANSFER dbo.staging_deliveries;
ALTER SCHEMA staging TRANSFER dbo.staging_routes;
ALTER SCHEMA staging TRANSFER dbo.staging_exceptions;

ALTER SCHEMA dw TRANSFER dbo.fact_sales;
ALTER SCHEMA dw TRANSFER dbo.fact_routes;
ALTER SCHEMA dw TRANSFER dbo.fact_exceptions;
ALTER SCHEMA dw TRANSFER dbo.fact_deliveries;
ALTER SCHEMA dw TRANSFER dbo.dim_shipment_type;
ALTER SCHEMA dw TRANSFER dbo.dim_route;
ALTER SCHEMA dw TRANSFER dbo.dim_date;
ALTER SCHEMA dw TRANSFER dbo.dim_region;
ALTER SCHEMA dw TRANSFER dbo.dim_product;
ALTER SCHEMA dw TRANSFER dbo.dim_priority_flag;
ALTER SCHEMA dw TRANSFER dbo.dim_exception_type;
ALTER SCHEMA dw TRANSFER dbo.dim_driver;

-----------------------------
SELECT name
FROM sys.schemas
WHERE name = 'clean';

SELECT name
FROM sys.schemas
ORDER BY name;

SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'staging_sales';

-----------------------------------