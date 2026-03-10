/*==============================================================
  CLEAN LAYER VIEWS
  Schema: clean

  PURPOSE
  -------
  The Clean layer transforms raw staging data into standardized,
  validated datasets that are safe for loading into the Data
  Warehouse (dw schema).

  This layer prevents ETL failures and improves data quality.

  RESPONSIBILITIES
  ----------------
  • Standardize text formatting
  • Validate and enforce data types
  • Apply basic business rules
  • Remove invalid records
  • Normalize values for dimension matching
  • Provide metadata for ETL monitoring

  DATA PIPELINE FLOW
  ------------------
  staging  →  clean  →  dw  →  reporting  →  Power BI
==============================================================*/

/*==============================================================
  VIEW: clean.vw_sales
  DESCRIPTION
  -----------
  Cleans and standardizes sales transaction data.

  TRANSFORMATIONS
  ---------------
  • Validates DateKey format
  • Normalizes product and region text
  • Converts numeric fields safely
  • Removes invalid transactions
  • Adds ETL load timestamp
==============================================================*/

CREATE OR ALTER VIEW clean.vw_sales
AS

SELECT
    SalesID,
    DeliveryID,

    ---------------------------------------------------------
    -- DATE KEY VALIDATION
    -- Ensures DateKey follows YYYYMMDD warehouse format
    ---------------------------------------------------------
    CASE
        WHEN DateKey BETWEEN 19000101 AND 21001231
            THEN DateKey
        ELSE NULL
    END AS DateKey,

    ---------------------------------------------------------
    -- DATA QUALITY MONITORING FLAG
    -- Identifies invalid DateKey values for auditing
    ---------------------------------------------------------
    CASE
        WHEN DateKey BETWEEN 19000101 AND 21001231
            THEN 0
        ELSE 1
    END AS IsBadDateKey,

    ---------------------------------------------------------
    -- NORMALIZE TEXT VALUES
    -- Prevents duplicate dimension records
    ---------------------------------------------------------
    UPPER(LTRIM(RTRIM(ProductType))) AS ProductType,
    UPPER(LTRIM(RTRIM(Region))) AS Region,

    ---------------------------------------------------------
    -- SAFE NUMERIC CONVERSION
    -- Prevents ETL failures from bad data
    ---------------------------------------------------------
    TRY_CONVERT(INT, UnitsSold) AS UnitsSold,
    TRY_CONVERT(DECIMAL(18,2), SalesAmount) AS SalesAmount,

    ---------------------------------------------------------
    -- ETL METADATA
    ---------------------------------------------------------
    GETDATE() AS CleanLoadDate

FROM staging.staging_sales

---------------------------------------------------------
-- DATA QUALITY FILTERS
-- Removes invalid or incomplete transactions
---------------------------------------------------------
WHERE UnitsSold > 0
  AND SalesAmount > 0
  AND SalesID IS NOT NULL
  AND DeliveryID IS NOT NULL
  AND DateKey IS NOT NULL;

GO



/*==============================================================
  VIEW: clean.vw_deliveries
  DESCRIPTION
  -----------
  Standardizes delivery operational data and applies
  business rules related to shipment tracking.

  TRANSFORMATIONS
  ---------------
  • Normalize region and shipment text
  • Identify late deliveries
  • Normalize priority flags
  • Remove invalid records
==============================================================*/

CREATE OR ALTER VIEW clean.vw_deliveries
AS

SELECT
    DeliveryID,
    RouteID,
    DriverID,

    ---------------------------------------------------------
    -- TEXT NORMALIZATION
    ---------------------------------------------------------
    UPPER(LTRIM(RTRIM(Region))) AS Region,
    UPPER(LTRIM(RTRIM(ShipmentType))) AS ShipmentType,

    DeliveryDate,
    ExpectedDeliveryDate,

    ---------------------------------------------------------
    -- BUSINESS RULE
    -- Automatically flag late deliveries
    ---------------------------------------------------------
    CASE
        WHEN DeliveryDate > ExpectedDeliveryDate THEN 'LATE'
        ELSE UPPER(LTRIM(RTRIM(DeliveryStatus)))
    END AS DeliveryStatus,

    ---------------------------------------------------------
    -- NORMALIZE BOOLEAN PRIORITY FLAG
    ---------------------------------------------------------
    CASE
        WHEN UPPER(CAST(PriorityFlag AS VARCHAR(10)))
             IN ('TRUE','1','YES','Y')
        THEN 1
        ELSE 0
    END AS PriorityFlag,

    ---------------------------------------------------------
    -- ETL METADATA
    ---------------------------------------------------------
    GETDATE() AS CleanLoadDate

FROM staging.staging_deliveries

---------------------------------------------------------
-- DATA QUALITY RULES
---------------------------------------------------------
WHERE DeliveryID IS NOT NULL
  AND RouteID IS NOT NULL
  AND DriverID IS NOT NULL
  AND DeliveryDate IS NOT NULL
  AND ExpectedDeliveryDate IS NOT NULL;

GO



/*==============================================================
  VIEW: clean.vw_exceptions
  DESCRIPTION
  -----------
  Cleans operational exception data related to deliveries.

  TRANSFORMATIONS
  ---------------
  • Normalize exception text
  • Fix invalid resolution chronology
  • Remove negative durations
  • Normalize priority flags
==============================================================*/

CREATE OR ALTER VIEW clean.vw_exceptions
AS

SELECT
    ExceptionID,
    DeliveryID,

    ---------------------------------------------------------
    -- NORMALIZE TEXT
    ---------------------------------------------------------
    UPPER(LTRIM(RTRIM(ExceptionType))) AS ExceptionType,

    DateReported,

    ---------------------------------------------------------
    -- CHRONOLOGY VALIDATION
    -- Prevents ResolvedDate before DateReported
    ---------------------------------------------------------
    CASE
        WHEN ResolvedDate IS NULL THEN NULL
        WHEN ResolvedDate < DateReported THEN DateReported
        ELSE ResolvedDate
    END AS ResolvedDate,

    ---------------------------------------------------------
    -- PREVENT NEGATIVE RESOLUTION DURATIONS
    ---------------------------------------------------------
    CASE
        WHEN ResolutionTimeHours < 0 THEN NULL
        ELSE ResolutionTimeHours
    END AS ResolutionTimeHours,

    ---------------------------------------------------------
    -- NORMALIZE PRIORITY FLAG
    ---------------------------------------------------------
    CASE
        WHEN UPPER(CAST(PriorityFlag AS VARCHAR(10)))
             IN ('TRUE','1','YES','Y')
        THEN 1
        ELSE 0
    END AS PriorityFlag,

    ---------------------------------------------------------
    -- STANDARDIZE REGION VALUES
    ---------------------------------------------------------
    UPPER(LTRIM(RTRIM(Region))) AS Region,

    ---------------------------------------------------------
    -- ETL METADATA
    ---------------------------------------------------------
    GETDATE() AS CleanLoadDate

FROM staging.staging_exceptions

---------------------------------------------------------
-- DATA QUALITY FILTERS
---------------------------------------------------------
WHERE ExceptionID IS NOT NULL
  AND DeliveryID IS NOT NULL
  AND DateReported IS NOT NULL;

GO



/*==============================================================
  VIEW: clean.vw_routes
  DESCRIPTION
  -----------
  Validates route operational metrics and ensures
  numeric fields are safe for warehouse loading.

  TRANSFORMATIONS
  ---------------
  • Safe numeric conversions
  • Region normalization
  • Removes invalid operational records
==============================================================*/

CREATE OR ALTER VIEW clean.vw_routes
AS

SELECT
    RouteID,
    DriverID,

    ---------------------------------------------------------
    -- SAFE NUMERIC CONVERSION
    ---------------------------------------------------------
    TRY_CONVERT(INT, PlannedStops) AS PlannedStops,
    TRY_CONVERT(INT, ActualStops) AS ActualStops,

    TRY_CONVERT(DECIMAL(10,2), PlannedHours) AS PlannedHours,
    TRY_CONVERT(DECIMAL(10,2), ActualHours) AS ActualHours,

    ---------------------------------------------------------
    -- NORMALIZE REGION VALUES
    ---------------------------------------------------------
    UPPER(LTRIM(RTRIM(Region))) AS Region,

    ---------------------------------------------------------
    -- ETL METADATA
    ---------------------------------------------------------
    GETDATE() AS CleanLoadDate

FROM staging.staging_routes

---------------------------------------------------------
-- DATA QUALITY RULES
---------------------------------------------------------
WHERE PlannedStops >= 0
  AND ActualStops >= 0
  AND PlannedHours > 0
  AND ActualHours > 0
  AND RouteID IS NOT NULL
  AND DriverID IS NOT NULL;

GO


/*
/*==============================================================
  CLEAN LAYER VIEWS
  Purpose:
      Standardize, validate, and cleanse staging data
      before loading into Data Warehouse (dw schema).

  Responsibilities:
      • Remove invalid records
      • Normalize text values
      • Enforce data types
      • Apply business rules
      • Prevent ETL failures
      • Produce warehouse-ready datasets

  Layer Flow:
      staging → clean → dw → reporting → Power BI
==============================================================*/


/*==============================================================
  View: clean.vw_sales
  Description:
      Cleans and standardizes sales transactions.
==============================================================*/

CREATE OR ALTER VIEW clean.vw_sales
AS
SELECT
    SalesID,
    DeliveryID,

	/*
    -- Safe DateKey conversion (prevents ETL failure)
    TRY_CONVERT(INT, FORMAT(DateKey,'yyyyMMdd')) AS DateKey,
	*/

	-- DATE KEY VALIDATION
    -- Ensure DateKey follows valid YYYYMMDD warehouse format
    ---------------------------------------------------------
    CASE
        WHEN DateKey BETWEEN 19000101 AND 21001231
            THEN DateKey
        ELSE NULL
    END AS DateKey,

    ---------------------------------------------------------
    -- DATA QUALITY MONITORING FLAG
    -- Identifies invalid DateKey values for auditing
    ---------------------------------------------------------
    CASE
        WHEN DateKey BETWEEN 19000101 AND 21001231
            THEN 0
        ELSE 1
    END AS IsBadDateKey,

    -- Normalize text fields (prevents dimension duplication)
    UPPER(LTRIM(RTRIM(ProductType))) AS ProductType,
    UPPER(LTRIM(RTRIM(Region))) AS Region,

    -- Enforce numeric typing
    TRY_CONVERT(INT, UnitsSold) AS UnitsSold,
    TRY_CONVERT(DECIMAL(18,2), SalesAmount) AS SalesAmount,

    -- ETL metadata tracking
    GETDATE() AS CleanLoadDate

FROM staging.staging_sales

-- Remove invalid or incomplete transactions
WHERE UnitsSold > 0
  AND SalesAmount > 0
  AND SalesID IS NOT NULL
  AND DeliveryID IS NOT NULL
  AND DateKey IS NOT NULL;
GO


/*==============================================================
  View: clean.vw_deliveries
  Description:
      Standardizes delivery operational data.
==============================================================*/

CREATE OR ALTER VIEW clean.vw_deliveries
AS
SELECT
    DeliveryID,
    RouteID,
    DriverID,

    UPPER(LTRIM(RTRIM(Region))) AS Region,
    UPPER(LTRIM(RTRIM(ShipmentType))) AS ShipmentType,

    DeliveryDate,
    ExpectedDeliveryDate,

    -- Business rule: automatically flag late deliveries
    CASE
        WHEN DeliveryDate > ExpectedDeliveryDate THEN 'LATE'
        ELSE UPPER(LTRIM(RTRIM(DeliveryStatus)))
    END AS DeliveryStatus,

    -- Normalize boolean priority flag
    CASE
        WHEN UPPER(CAST(PriorityFlag AS VARCHAR(10)))
             IN ('TRUE','1','YES','Y')
        THEN 1
        ELSE 0
    END AS PriorityFlag,

    GETDATE() AS CleanLoadDate

FROM staging.staging_deliveries

-- Prevent invalid delivery records
WHERE DeliveryID IS NOT NULL
  AND RouteID IS NOT NULL
  AND DriverID IS NOT NULL
  AND DeliveryDate IS NOT NULL
  AND ExpectedDeliveryDate IS NOT NULL;
GO


/*==============================================================
  View: clean.vw_exceptions
  Description:
      Cleans exception tracking data.
==============================================================*/

CREATE OR ALTER VIEW clean.vw_exceptions
AS
SELECT
    ExceptionID,
    DeliveryID,

    UPPER(LTRIM(RTRIM(ExceptionType))) AS ExceptionType,

    DateReported,

    -- Prevent invalid resolution chronology
    CASE
        WHEN ResolvedDate IS NULL THEN NULL
        WHEN ResolvedDate < DateReported THEN DateReported
        ELSE ResolvedDate
    END AS ResolvedDate,

    -- Remove negative resolution durations
    CASE
        WHEN ResolutionTimeHours < 0 THEN NULL
        ELSE ResolutionTimeHours
    END AS ResolutionTimeHours,

    CASE
        WHEN UPPER(CAST(PriorityFlag AS VARCHAR(10)))
             IN ('TRUE','1','YES','Y')
        THEN 1
        ELSE 0
    END AS PriorityFlag,

    UPPER(LTRIM(RTRIM(Region))) AS Region,

    GETDATE() AS CleanLoadDate

FROM staging.staging_exceptions

WHERE ExceptionID IS NOT NULL
  AND DeliveryID IS NOT NULL
  AND DateReported IS NOT NULL;
GO


/*==============================================================
  View: clean.vw_routes
  Description:
      Validates route performance metrics.
==============================================================*/

CREATE OR ALTER VIEW clean.vw_routes
AS
SELECT
    RouteID,
    DriverID,

    TRY_CONVERT(INT, PlannedStops) AS PlannedStops,
    TRY_CONVERT(INT, ActualStops) AS ActualStops,

    TRY_CONVERT(DECIMAL(10,2), PlannedHours) AS PlannedHours,
    TRY_CONVERT(DECIMAL(10,2), ActualHours) AS ActualHours,

    UPPER(LTRIM(RTRIM(Region))) AS Region,

    GETDATE() AS CleanLoadDate

FROM staging.staging_routes

-- Data quality validation rules
WHERE PlannedStops >= 0
  AND ActualStops >= 0
  AND PlannedHours > 0
  AND ActualHours > 0
  AND RouteID IS NOT NULL
  AND DriverID IS NOT NULL;
GO
*/
