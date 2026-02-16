-- =============================================
-- FACT TABLES
-- Fedex_Ops_DB_Prod
-- =============================================

select *
from fact_deliveries

select *
from fact_exceptions

select *
from fact_sales

-- =============================================
-- DIM TABLES
-- =============================================

select *
from dim_exception_type

select *
from dim_product

select *
from dim_shipment_type

select *
from dim_driver

select *
from dim_route

select *
from dim_region

select *
from dim_date

-- =============================================
-- ADVANCED ENTERPRISE LOGIC
-- =============================================

-- Update South Region (realistic pattern)
UPDATE d
SET DeliveryStatus = 'Late'
FROM fact_deliveries d
JOIN dim_region r ON d.RegionKey = r.RegionKey
WHERE r.RegionName = 'South'
  AND ABS(CHECKSUM(NEWID())) % 10 < 3;

-- Holiday season delays (Nov–Dec)
UPDATE d
SET DeliveryMinutes = DeliveryMinutes + 800
FROM fact_deliveries d
JOIN dim_date dt ON d.DateKey = dt.DateKey
WHERE dt.Month IN (11, 12);

-- Junior drivers slower
UPDATE d
SET DeliveryMinutes = DeliveryMinutes + 300
FROM fact_deliveries d
JOIN dim_driver dr ON d.DriverKey = dr.DriverKey
WHERE dr.ExperienceLevel = 'Junior';

-- =============================================

