/*==============================================================
  CLEAN LAYER VIEWS
  Schema: clean
  Version: 2.0

  PURPOSE
  -------
  The Clean layer transforms raw staging data into standardized,
  validated datasets that are safe for loading into the Data
  Warehouse (dw schema).

  DATA PIPELINE FLOW
  ------------------
  staging  ->  clean  ->  dw  ->  reporting  ->  Power BI

  CHANGE LOG
  ----------
  v2.0 - Fixed DateKey range check to use proper DATE literals
         instead of integer comparison.
       - Removed redundant TRY_CONVERT on already-typed staging
         columns; replaced with direct column references.
       - Removed GETDATE() from views (not a stable load
         timestamp; write audit timestamps during the physical
         load step instead).
       - Simplified PriorityFlag normalization: BIT columns only
         ever cast to '0' or '1', so TRUE/YES/Y branches were
         dead code.
       - Added IsDateCorrected flag to vw_exceptions so consumers
         know when a ResolvedDate was silently adjusted.
       - Derived ResolutionTimeHours from dates when the stored
         value is NULL or negative but valid dates exist.
       - Made vw_routes stop filter consistent: both stops and
         hours now require > 0 (no zero-stop routes).
       - Added EfficiencyRatio and StopVariance derived columns
         to vw_routes to avoid duplicating logic in the DW layer.
       - Added documentation note explaining that IsBadDateKey
         rows are intentionally retained in vw_sales for auditing.
==============================================================*/


/*==============================================================
  VIEW: clean.vw_sales
  DESCRIPTION: Clean and standardize sales transaction data.

  TRANSFORMATIONS
  ---------------
  - Validates DateKey is within a sensible date range
  - Flags bad DateKey values for audit (IsBadDateKey)
  - Records with a bad DateKey are RETAINED with DateKey = NULL
    so they appear in audit queries; they will be excluded from
    DW fact loads by filtering on IsBadDateKey = 0.
  - Normalizes ProductType and Region text to UPPER CASE
  - Removes invalid transactions (null keys, non-positive amounts)
==============================================================*/

CREATE OR ALTER VIEW clean.vw_sales
AS

SELECT
    SalesID,
    DeliveryID,

    ---------------------------------------------------------
    -- DATE KEY VALIDATION
    -- Range check uses DATE literals, not integers, to avoid
    -- implicit conversion issues with the DATE column type.
    ---------------------------------------------------------
    CASE
        WHEN DateKey BETWEEN '1900-01-01' AND '2100-12-31'
            THEN DateKey
        ELSE NULL
    END AS DateKey,

    ---------------------------------------------------------
    -- DATA QUALITY MONITORING FLAG
    -- 1 = bad or out-of-range date; 0 = valid date.
    -- Rows with IsBadDateKey = 1 are kept in this view for
    -- auditing. The DW load step must filter on IsBadDateKey = 0.
    ---------------------------------------------------------
    CASE
        WHEN DateKey BETWEEN '1900-01-01' AND '2100-12-31'
            THEN 0
        ELSE 1
    END AS IsBadDateKey,

    ---------------------------------------------------------
    -- NORMALIZE TEXT VALUES
    -- Prevents duplicate dimension records caused by case
    -- or whitespace inconsistencies in source data.
    ---------------------------------------------------------
    UPPER(LTRIM(RTRIM(ProductType))) AS ProductType,
    UPPER(LTRIM(RTRIM(Region)))      AS Region,

    ---------------------------------------------------------
    -- NUMERIC COLUMNS
    -- Staging columns are already typed INT / DECIMAL(10,2);
    -- pass them through directly. TRY_CONVERT on a correctly
    -- typed column is a no-op and masks silent NULL returns.
    ---------------------------------------------------------
    UnitsSold,
    SalesAmount

FROM staging.staging_sales

---------------------------------------------------------
-- DATA QUALITY FILTERS
-- Removes incomplete or clearly invalid transactions.
-- Zero/negative values are dropped here; if returns or
-- corrections need to be tracked, add a separate
-- vw_sales_adjustments view sourced from a corrections table.
---------------------------------------------------------
WHERE SalesID    IS NOT NULL
  AND DeliveryID IS NOT NULL
  AND DateKey    IS NOT NULL
  AND UnitsSold  > 0
  AND SalesAmount > 0;

GO


/*==============================================================
  VIEW: clean.vw_deliveries
  DESCRIPTION: Standardize delivery operational data and apply
  business rules related to shipment tracking.

  TRANSFORMATIONS
  ---------------
  - Normalizes Region and ShipmentType text to UPPER CASE
  - Overrides DeliveryStatus to 'LATE' when DeliveryDate
    exceeds ExpectedDeliveryDate (business rule)
  - Normalizes PriorityFlag to a strict 0/1 INT
  - Removes records missing any required identifier or date
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
    UPPER(LTRIM(RTRIM(Region)))       AS Region,
    UPPER(LTRIM(RTRIM(ShipmentType))) AS ShipmentType,

    DeliveryDate,
    ExpectedDeliveryDate,

    ---------------------------------------------------------
    -- BUSINESS RULE: LATE DELIVERY FLAG
    -- When the actual delivery date is after the expected
    -- date, status is forced to 'LATE' regardless of the
    -- source value. Otherwise the source status is uppercased
    -- and trimmed for consistency.
    -- NOTE: All downstream checks must compare against 'LATE'
    -- (all caps) to match this output.
    ---------------------------------------------------------
    CASE
        WHEN DeliveryDate > ExpectedDeliveryDate
            THEN 'LATE'
        ELSE UPPER(LTRIM(RTRIM(DeliveryStatus)))
    END AS DeliveryStatus,

    ---------------------------------------------------------
    -- NORMALIZE BOOLEAN PRIORITY FLAG
    -- Staging column is typed BIT; casting a BIT to VARCHAR
    -- produces only '0' or '1'. A direct comparison is clearer
    -- and avoids dead branches for 'TRUE', 'YES', 'Y'.
    ---------------------------------------------------------
    CASE
        WHEN PriorityFlag = 1 THEN 1
        ELSE 0
    END AS PriorityFlag

FROM staging.staging_deliveries

---------------------------------------------------------
-- DATA QUALITY RULES
-- Both date columns are required because the late-delivery
-- business rule cannot be evaluated without them.
---------------------------------------------------------
WHERE DeliveryID           IS NOT NULL
  AND RouteID              IS NOT NULL
  AND DriverID             IS NOT NULL
  AND DeliveryDate         IS NOT NULL
  AND ExpectedDeliveryDate IS NOT NULL;

GO


/*==============================================================
  VIEW: clean.vw_exceptions
  DESCRIPTION: Clean operational exception data related to
  deliveries.

  TRANSFORMATIONS
  ---------------
  - Normalizes ExceptionType and Region text to UPPER CASE
  - Corrects chronological errors where ResolvedDate precedes
    DateReported; flags these rows with IsDateCorrected = 1
  - Derives ResolutionTimeHours from dates when the stored
    value is missing or negative but both dates are available
  - Normalizes PriorityFlag to strict 0/1 INT
  - Removes records missing required identifiers or DateReported
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
    -- When ResolvedDate precedes DateReported the row is
    -- corrected to DateReported as the floor value.
    -- IsDateCorrected = 1 signals that the original value
    -- was invalid; consumers can filter or audit as needed.
    ---------------------------------------------------------
    CASE
        WHEN ResolvedDate IS NULL              THEN NULL
        WHEN ResolvedDate < DateReported       THEN DateReported
        ELSE ResolvedDate
    END AS ResolvedDate,

    ---------------------------------------------------------
    -- DATE CORRECTION AUDIT FLAG
    -- 1 = ResolvedDate was out-of-order and was corrected;
    -- 0 = date is as-sourced or null (open exception).
    ---------------------------------------------------------
    CASE
        WHEN ResolvedDate IS NOT NULL
         AND ResolvedDate < DateReported THEN 1
        ELSE 0
    END AS IsDateCorrected,

    ---------------------------------------------------------
    -- RESOLUTION TIME (HOURS)
    -- Prefer the stored value when it is valid (>= 0).
    -- When it is negative but both dates are present, derive
    -- the duration from the corrected dates instead of
    -- returning NULL, which loses otherwise-recoverable data.
    ---------------------------------------------------------
    CASE
        WHEN ResolutionTimeHours >= 0
            THEN ResolutionTimeHours
        WHEN ResolvedDate IS NOT NULL
            THEN DATEDIFF(
                    HOUR,
                    DateReported,
                    CASE
                        WHEN ResolvedDate < DateReported
                            THEN DateReported
                        ELSE ResolvedDate
                    END)
        ELSE NULL
    END AS ResolutionTimeHours,

    ---------------------------------------------------------
    -- NORMALIZE PRIORITY FLAG
    -- Same pattern as vw_deliveries: BIT column, direct
    -- comparison is clearer than casting to VARCHAR.
    ---------------------------------------------------------
    CASE
        WHEN PriorityFlag = 1 THEN 1
        ELSE 0
    END AS PriorityFlag,

    ---------------------------------------------------------
    -- STANDARDIZE REGION VALUES
    ---------------------------------------------------------
    UPPER(LTRIM(RTRIM(Region))) AS Region

FROM staging.staging_exceptions

---------------------------------------------------------
-- DATA QUALITY FILTERS
-- ResolvedDate is intentionally NOT required here;
-- NULL ResolvedDate means the exception is still open.
---------------------------------------------------------
WHERE ExceptionID  IS NOT NULL
  AND DeliveryID   IS NOT NULL
  AND DateReported IS NOT NULL;

GO


/*==============================================================
  VIEW: clean.vw_routes
  DESCRIPTION: Validate route operational metrics and ensure
  numeric fields are safe for warehouse loading.

  TRANSFORMATIONS
  ---------------
  - Normalizes Region text to UPPER CASE
  - Filters out routes with zero or negative stops or hours
    (a route with no stops or no recorded time is not a valid
    completed route)
  - Adds EfficiencyRatio (actual / planned hours) and
    StopVariance (actual - planned stops) to avoid duplicating
    this logic in the DW layer
==============================================================*/

CREATE OR ALTER VIEW clean.vw_routes
AS

SELECT
    RouteID,
    DriverID,

    ---------------------------------------------------------
    -- OPERATIONAL METRICS
    -- Staging columns are already typed INT / DECIMAL(5,2);
    -- pass through directly.
    ---------------------------------------------------------
    PlannedStops,
    ActualStops,
    PlannedHours,
    ActualHours,

    ---------------------------------------------------------
    -- NORMALIZE REGION VALUES
    ---------------------------------------------------------
    UPPER(LTRIM(RTRIM(Region))) AS Region,

    ---------------------------------------------------------
    -- DERIVED EFFICIENCY METRICS
    -- Calculated here in the clean layer so the DW and
    -- reporting layers do not need to duplicate the logic.
    --
    -- EfficiencyRatio: > 1.0 means the route took longer
    --                  than planned; < 1.0 means it was
    --                  completed faster than planned.
    -- StopVariance:    positive = more stops than planned;
    --                  negative = fewer stops than planned.
    ---------------------------------------------------------
    ROUND(
        ActualHours / NULLIF(PlannedHours, 0),
        4
    ) AS EfficiencyRatio,

    ActualStops - PlannedStops AS StopVariance

FROM staging.staging_routes

---------------------------------------------------------
-- DATA QUALITY RULES
-- Both stops filters use > 0 (not >= 0) for consistency:
-- a route with zero planned OR actual stops is not a
-- valid completed route record.
-- PlannedHours > 0 and ActualHours > 0 are required for
-- EfficiencyRatio to be meaningful.
---------------------------------------------------------
WHERE RouteID      IS NOT NULL
  AND DriverID     IS NOT NULL
  AND PlannedStops > 0
  AND ActualStops  > 0
  AND PlannedHours > 0
  AND ActualHours  > 0;

GO
