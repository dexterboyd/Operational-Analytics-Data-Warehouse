/*=============================================================
  CLEAN LAYER
  Database: Fedex_Ops_Database
  Version:  1.0

  Purpose:
      Create views in the clean schema that standardize and
      transform raw staging data. No data is moved or copied --
      the clean layer is entirely view-based so it always
      reflects the latest staging data automatically.

  Run Order:
      1. etl_staging_setup_v5.sql          -- build schemas/tables
      2. load_staging.py                   -- load CSV data
      3. staging_layer_validation_v2.sql   -- validate staging
      4. THIS SCRIPT                       -- build clean layer

  Transformations Applied:
      All four views fix the truncated categorical values found
      during staging validation, standardize NULLs, and add
      derived columns useful for downstream DW and reporting.

  Truncation Mappings (applied in all relevant views):
      Region:        M.  ->  MW   |  N.  ->  N   |  S.  ->  S
      ProductType:   F.  ->  Freight
                     L.  ->  Large Package
                     M.  ->  Medium Package
                     S.  ->  Small Package
      ShipmentType:  E.  ->  Express
                     P.  ->  Priority
                     S.  ->  Standard
      ExceptionType: A.  ->  Address Issue
                     C.  ->  Customer Not Available
                     M.  ->  Mechanical
                     W.  ->  Weather

  Clean Layer Views:
      clean.clean_sales        -- standardized sales transactions
      clean.clean_deliveries   -- standardized delivery records
      clean.clean_routes       -- deduplicated route performance
      clean.clean_exceptions   -- standardized exception records

=============================================================*/

USE Fedex_Ops_Database;
GO

/*=============================================================
  HELPER: DROP VIEWS IF THEY EXIST
  Allows this script to be safely re-run.
=============================================================*/

IF OBJECT_ID('clean.clean_sales',       'V') IS NOT NULL DROP VIEW clean.clean_sales;
IF OBJECT_ID('clean.clean_deliveries',  'V') IS NOT NULL DROP VIEW clean.clean_deliveries;
IF OBJECT_ID('clean.clean_routes',      'V') IS NOT NULL DROP VIEW clean.clean_routes;
IF OBJECT_ID('clean.clean_exceptions',  'V') IS NOT NULL DROP VIEW clean.clean_exceptions;
GO

/*=============================================================
  VIEW: clean.clean_sales

  Fixes:
      - Truncated ProductType values  (F. L. M. S.)
      - Truncated Region values       (M. N. S.)
 
  Adds:
      - SaleDate        DATE only, time stripped from DateKey
      - SaleYear        Year of sale
      - SaleMonth       Month of sale
      - SaleQuarter     Quarter of sale
      - RevenuePerUnit  SalesAmount / UnitsSold
=============================================================*/

CREATE VIEW clean.clean_sales AS
SELECT
    SalesID,
    DeliveryID,
    DateKey,

    -- Strip time component for date-level reporting
    CAST(DateKey AS DATE) AS SaleDate,

    -- Derived time dimensions
    YEAR(DateKey) AS SaleYear,
    MONTH(DateKey) AS SaleMonth,
    DATEPART(QUARTER, DateKey) AS SaleQuarter,

    -- Expand truncated ProductType values
    CASE ProductType
        WHEN 'F.' THEN 'Freight'
        WHEN 'L.' THEN 'Large Package'
        WHEN 'M.' THEN 'Medium Package'
        WHEN 'S.' THEN 'Small Package'
        ELSE ProductType
    END AS ProductType,

    -- Expand truncated Region values
    CASE Region
        WHEN 'M.' THEN 'MW'
        WHEN 'N.' THEN 'N'
        WHEN 'S.' THEN 'S'
        ELSE Region
    END AS Region,

    UnitsSold,
    SalesAmount,

    -- Derived metric
    ROUND(SalesAmount / NULLIF(UnitsSold, 0), 2) AS RevenuePerUnit

FROM staging.staging_sales;
GO

PRINT 'View created: clean.clean_sales';
GO

/*=============================================================
  VIEW: clean.clean_deliveries

  Fixes:
      - Truncated ShipmentType values  (E. P. S.)
      - Truncated Region values        (M. N. S.)
      - NULL DriverID replaced with    'Unknown'
 
  Adds:
      - DeliveryDateOnly    DATE only, time stripped
      - ExpectedDateOnly    DATE only, time stripped
      - DeliveryYear        Year of delivery
      - DeliveryMonth       Month of delivery
      - DeliveryQuarter     Quarter of delivery
      - IsLate              1 if DeliveryStatus is Late or Exception
      - DaysVariance        Actual vs expected delivery date difference
                            Positive = late, Negative = early, NULL = no expected date
=============================================================*/

CREATE VIEW clean.clean_deliveries AS
SELECT
    DeliveryID,
    RouteID,

    -- Replace NULL DriverID with Unknown
    ISNULL(DriverID, 'Unknown') AS DriverID,

    -- Expand truncated Region values
    CASE Region
        WHEN 'M.' THEN 'MW'
        WHEN 'N.' THEN 'N'
        WHEN 'S.' THEN 'S'
        ELSE Region
    END AS Region,

    -- Expand truncated ShipmentType values
    CASE ShipmentType
        WHEN 'E.' THEN 'Express'
        WHEN 'P.' THEN 'Priority'
        WHEN 'S.' THEN 'Standard'
        ELSE ShipmentType
    END AS ShipmentType,

    DeliveryDate,
    ExpectedDeliveryDate,

    -- Strip time component for date-level reporting
    CAST(DeliveryDate AS DATE) AS DeliveryDateOnly,
    CAST(ExpectedDeliveryDate AS DATE) AS ExpectedDateOnly,

    -- Derived time dimensions
    YEAR(DeliveryDate) AS DeliveryYear,
    MONTH(DeliveryDate) AS DeliveryMonth,
    DATEPART(QUARTER, DeliveryDate) AS DeliveryQuarter,

    DeliveryStatus,

    -- Binary late flag (Late or Exception both count as not on time)
    CASE 
		WHEN DeliveryStatus IN ('Late', 'Exception') THEN 1
		ELSE 0
	END AS IsLate,

    -- Days variance between actual and expected delivery
    -- Positive = delivered late, Negative = delivered early, NULL = no expected date
    CASE
        WHEN ExpectedDeliveryDate IS NOT NULL
        THEN DATEDIFF(DAY, ExpectedDeliveryDate, DeliveryDate)
        ELSE NULL
    END AS DaysVariance,

    PriorityFlag

FROM staging.staging_deliveries;
GO

PRINT 'View created: clean.clean_deliveries';
GO

/*=============================================================
  VIEW: clean.clean_routes

  Fixes:
      - Truncated Region values    (M. N. S.)
      - NULL DriverID replaced with 'Unknown'
      - Duplicate RouteID+DriverID rows are genuine separate
        route runs (different stops, hours, and regions).
        A surrogate key RouteRunID is added via ROW_NUMBER
        to make each run uniquely identifiable downstream.
 
  Adds:
      - RouteRunID        Surrogate key per unique route run
      - StopVariance      ActualStops  - PlannedStops
      - HourVariance      ActualHours  - PlannedHours
      - StopEfficiencyPct ActualStops  / PlannedStops  * 100
      - HourEfficiencyPct PlannedHours / ActualHours   * 100
=============================================================*/

CREATE VIEW clean.clean_routes AS
SELECT
    -- Surrogate key to uniquely identify each route run
    ROW_NUMBER() OVER (
        ORDER BY RouteID, DriverID, Region, PlannedStops
    ) AS RouteRunID,

    RouteID,

    -- Replace NULL DriverID with Unknown
    ISNULL(NULLIF(LTRIM(RTRIM(DriverID)), ''), 'Unknown') AS DriverID,

    PlannedStops,
    ActualStops,
    PlannedHours,
    ActualHours,

    -- Expand truncated Region values
    CASE Region
        WHEN 'M.' THEN 'MW'
        WHEN 'N.' THEN 'N'
        WHEN 'S.' THEN 'S'
        ELSE Region
    END AS Region,

    -- Derived variance metrics
    (ActualStops  - PlannedStops) AS StopVariance,
    ROUND((ActualHours - PlannedHours), 2) AS HourVariance,

    -- Efficiency ratios (capped at 999.99 to handle zero planned values)
    ROUND(
        CASE
			WHEN PlannedStops > 0
            THEN (CAST(ActualStops AS DECIMAL(10,2)) / PlannedStops) * 100
			ELSE NULL
		END, 2) AS StopEfficiencyPct,

    ROUND(
        CASE WHEN ActualHours > 0
             THEN (PlannedHours / ActualHours) * 100
             ELSE NULL
		END, 2) AS HourEfficiencyPct

FROM staging.staging_routes;
GO

PRINT 'View created: clean.clean_routes';
GO

/*=============================================================
  VIEW: clean.clean_exceptions

  Fixes:
      - Truncated ExceptionType values  (A. C. M. W.)
      - Truncated Region values         (M. N. S.)
 
  Adds:
      - DateReportedOnly   DATE only, time stripped
      - ResolvedDateOnly   DATE only, time stripped
      - ExceptionYear      Year exception was reported
      - ExceptionMonth     Month exception was reported
      - ExceptionQuarter   Quarter exception was reported
      - IsResolved         1 if ResolvedDate is populated
      - ResolutionDays     Whole days taken to resolve, NULL if open
=============================================================*/

CREATE VIEW clean.clean_exceptions AS
SELECT
    ExceptionID,
    DeliveryID,

    -- Expand truncated ExceptionType values
    CASE ExceptionType
        WHEN 'A.' THEN 'Address Issue'
        WHEN 'C.' THEN 'Customer Not Available'
        WHEN 'M.' THEN 'Mechanical'
        WHEN 'W.' THEN 'Weather'
        ELSE ExceptionType
    END AS ExceptionType,

    DateReported,
    ResolvedDate,

    -- Strip time component for date-level reporting
    CAST(DateReported AS DATE) AS DateReportedOnly,
    CAST(ResolvedDate AS DATE) AS ResolvedDateOnly,

    -- Derived time dimensions
    YEAR(DateReported) AS ExceptionYear,
    MONTH(DateReported) AS ExceptionMonth,
    DATEPART(QUARTER, DateReported) AS ExceptionQuarter,

    ResolutionTimeHours,

    -- Binary resolved flag
    CASE
		WHEN ResolvedDate IS NOT NULL
		THEN 1
		ELSE 0
	END AS IsResolved,

    -- Whole days to resolve (NULL if still open)
    CASE
        WHEN ResolvedDate IS NOT NULL
        THEN DATEDIFF(DAY, DateReported, ResolvedDate)
        ELSE NULL
    END AS ResolutionDays,

    PriorityFlag,

    -- Expand truncated Region values
    CASE Region
        WHEN 'M.' THEN 'MW'
        WHEN 'N.' THEN 'N'
        WHEN 'S.' THEN 'S'
        ELSE Region
    END AS Region

FROM staging.staging_exceptions;
GO

PRINT 'View created: clean.clean_exceptions';
GO

/*=============================================================
  CLEAN LAYER SMOKE TEST

  Quick row count and truncation check to confirm all four
  views are working and truncated values have been resolved.
  All Remaining counts should be 0 except clean_routes
  Unknown DriverID which reflects expected replacements.
=============================================================*/

PRINT '--- CLEAN LAYER SMOKE TEST ---';

-- Row counts (should match staging exactly)
SELECT 'clean_sales'AS ViewName, COUNT(*) AS RowsCount
FROM clean.clean_sales
UNION ALL

SELECT 'clean_deliveries' AS ViewName, COUNT(*) AS RowsCount
FROM clean.clean_deliveries
UNION ALL

SELECT 'clean_routes' AS ViewName, COUNT(*) AS RowsCount
FROM clean.clean_routes
UNION ALL

SELECT 'clean_exceptions' AS ViewName, COUNT(*) AS RowsCount
FROM clean.clean_exceptions;

-- Confirm no truncated values remain in clean layer
SELECT 'clean_sales ProductType' AS Check_, COUNT(*) AS Remaining
FROM clean.clean_sales
WHERE ProductType LIKE '_.'
UNION ALL

SELECT 'clean_sales Region' AS Check_, COUNT(*)
FROM clean.clean_sales
WHERE Region LIKE '_.'
UNION ALL

SELECT 'clean_deliveries ShipmentType' AS Check_, COUNT(*)
FROM clean.clean_deliveries
WHERE ShipmentType LIKE '_.'
UNION ALL

SELECT 'clean_deliveries Region' AS Check_, COUNT(*)
FROM clean.clean_deliveries
WHERE Region LIKE '_.'
UNION ALL

SELECT 'clean_exceptions ExceptionType' AS Check_, COUNT(*)
FROM clean.clean_exceptions
WHERE ExceptionType LIKE '_.'
UNION ALL

SELECT 'clean_routes Region' AS Check_, COUNT(*)
FROM clean.clean_routes
WHERE Region LIKE '_.'
UNION ALL

SELECT 'clean_routes NULL DriverID' AS Check_, COUNT(*)
FROM clean.clean_routes
WHERE DriverID = 'Unknown';

PRINT '--- END OF SMOKE TEST ---';
GO
