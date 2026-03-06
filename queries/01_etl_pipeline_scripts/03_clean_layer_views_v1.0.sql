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