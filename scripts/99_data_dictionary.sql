/*=============================================================
  DATA DICTIONARY
  Database: Fedex_Ops_Database
  Version:  1.0

  Purpose:
      Documents every table, view, and column across all four
      pipeline layers using SQL Server extended properties
      (sp_addextendedproperty). Once applied, descriptions are
      visible in SSMS Object Explorer tooltips, queryable via
      sys.extended_properties, and exportable to Excel or PDF.

  Pipeline Layers Documented:
      staging   -- raw source tables and load metadata
      clean     -- standardized transformation views
      dw        -- star schema dimensions and facts
      reporting -- aggregated BI views

  Run Order:
      Run after the full pipeline has been built:
      1. etl_staging_setup_v5.sql
      2. load_staging.py
      3. clean_layer_v1.sql
      4. dw_layer_v1.sql
      5. reporting_layer_v1.sql
      6. THIS SCRIPT

  Safe to Re-run:
      Each property is dropped before being re-added so this
      script can be run multiple times without errors. If a
      property does not exist on the first run the DROP is
      skipped safely via the IF EXISTS check.

  Querying the Dictionary:
      -- All table descriptions
      SELECT
          s.name          AS SchemaName,
          t.name          AS TableName,
          ep.value        AS Description
      FROM sys.extended_properties ep
      JOIN sys.tables  t ON ep.major_id = t.object_id
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE ep.name = 'MS_Description'
        AND ep.minor_id = 0
      ORDER BY s.name, t.name;

      -- All column descriptions
      SELECT
          s.name          AS SchemaName,
          t.name          AS TableName,
          c.name          AS ColumnName,
          ep.value        AS Description
      FROM sys.extended_properties ep
      JOIN sys.tables  t ON ep.major_id  = t.object_id
      JOIN sys.columns c ON ep.major_id  = c.object_id
                         AND ep.minor_id = c.column_id
      JOIN sys.schemas s ON t.schema_id  = s.schema_id
      WHERE ep.name = 'MS_Description'
      ORDER BY s.name, t.name, c.column_id;

=============================================================*/

USE Fedex_Ops_Database;
GO

/*=============================================================
  HELPER MACRO
  SQL Server requires sp_addextendedproperty to be called once
  per object. To allow re-runs, each property is dropped first
  if it already exists, then re-added with the current value.
=============================================================*/

PRINT '--- DATA DICTIONARY: APPLYING EXTENDED PROPERTIES ---';
GO

/*=============================================================
  LAYER 1: STAGING SCHEMA
  Raw source tables loaded directly from CSV files via
  load_staging.py. Data in this layer is unmodified from
  source -- truncated values and NULLs are preserved as-is
  and cleaned in the clean layer.
=============================================================*/

PRINT 'Documenting: staging layer...';

-- ===========================================================
-- TABLE: staging.staging_deliveries
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('staging.staging_deliveries')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'staging',
        @level1type = 'TABLE',  @level1name = 'staging_deliveries';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Raw delivery records loaded from deliveries.csv via load_staging.py. Contains route, driver, timing, and status for each delivery. Truncated ShipmentType and Region values are present -- these are corrected in clean.clean_deliveries.',
    @level0type = 'SCHEMA', @level0name = 'staging',
    @level1type = 'TABLE',  @level1name = 'staging_deliveries';

-- Columns
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Unique delivery identifier. Primary key.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='DeliveryID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Route assigned to this delivery e.g. R001 through R005.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='RouteID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Driver identifier e.g. Driver 1. NULL for approximately 1010 unassigned deliveries in the source file.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='DriverID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Delivery region code. May contain truncated values (M. N. S.) corrected to (MW N S) in clean layer.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='Region';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Shipment priority type. May contain truncated values (E. P. S.) corrected to (Express Priority Standard) in clean layer.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='ShipmentType';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Actual delivery timestamp including time component.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='DeliveryDate';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Planned delivery timestamp. NULL if no expected date was scheduled.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='ExpectedDeliveryDate';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Delivery outcome. Values: On-Time, Late, Exception.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='DeliveryStatus';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='1 = priority delivery, 0 = standard delivery.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_deliveries',@level2type='COLUMN',@level2name='PriorityFlag';
GO

-- ===========================================================
-- TABLE: staging.staging_exceptions
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('staging.staging_exceptions')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'staging',
        @level1type = 'TABLE',  @level1name = 'staging_exceptions';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Raw exception records loaded from exceptions.csv. Captures operational issues affecting deliveries. Approximately 30 open exceptions have NULL ResolvedDate by design.',
    @level0type = 'SCHEMA', @level0name = 'staging',
    @level1type = 'TABLE',  @level1name = 'staging_exceptions';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Unique exception identifier. Primary key.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_exceptions',@level2type='COLUMN',@level2name='ExceptionID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Delivery affected by this exception. Foreign key to staging_deliveries.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_exceptions',@level2type='COLUMN',@level2name='DeliveryID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Exception category. May contain truncated values (A. C. M. W.) corrected in clean layer.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_exceptions',@level2type='COLUMN',@level2name='ExceptionType';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Timestamp when the exception was reported.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_exceptions',@level2type='COLUMN',@level2name='DateReported';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Timestamp when the exception was resolved. NULL = exception is still open.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_exceptions',@level2type='COLUMN',@level2name='ResolvedDate';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Hours taken to resolve the exception. NULL if still open.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_exceptions',@level2type='COLUMN',@level2name='ResolutionTimeHours';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='1 = priority exception, 0 = standard.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_exceptions',@level2type='COLUMN',@level2name='PriorityFlag';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Region where exception occurred. May contain truncated values corrected in clean layer.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_exceptions',@level2type='COLUMN',@level2name='Region';
GO

-- ===========================================================
-- TABLE: staging.staging_routes
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('staging.staging_routes')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'staging',
        @level1type = 'TABLE',  @level1name = 'staging_routes';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Raw route performance records loaded from routes.csv. Each row represents one route run by one driver. Composite primary key is RouteID + DriverID. Contains 77 duplicate combinations representing genuine separate runs with different stops and hours.',
    @level0type = 'SCHEMA', @level0name = 'staging',
    @level1type = 'TABLE',  @level1name = 'staging_routes';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Route identifier e.g. R001. Part of composite primary key with DriverID.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_routes',@level2type='COLUMN',@level2name='RouteID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Driver identifier. Part of composite primary key with RouteID. NULL for approximately 59 unassigned route runs.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_routes',@level2type='COLUMN',@level2name='DriverID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Number of stops planned for this route run.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_routes',@level2type='COLUMN',@level2name='PlannedStops';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Number of stops actually completed on this route run.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_routes',@level2type='COLUMN',@level2name='ActualStops';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Hours planned for this route run.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_routes',@level2type='COLUMN',@level2name='PlannedHours';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Hours actually taken to complete this route run.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_routes',@level2type='COLUMN',@level2name='ActualHours';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Region of this route run. May contain truncated values corrected in clean layer.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_routes',@level2type='COLUMN',@level2name='Region';
GO

-- ===========================================================
-- TABLE: staging.staging_sales
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('staging.staging_sales')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'staging',
        @level1type = 'TABLE',  @level1name = 'staging_sales';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Raw sales transaction records loaded from sales.csv. Each row is one sales transaction tied to a delivery. Truncated ProductType and Region values are present and corrected in clean.clean_sales.',
    @level0type = 'SCHEMA', @level0name = 'staging',
    @level1type = 'TABLE',  @level1name = 'staging_sales';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Unique sales transaction identifier. Primary key.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_sales',@level2type='COLUMN',@level2name='SalesID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Delivery associated with this sale. Foreign key to staging_deliveries.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_sales',@level2type='COLUMN',@level2name='DeliveryID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Sale timestamp including time component. Stored as DATETIME2 to match source CSV format.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_sales',@level2type='COLUMN',@level2name='DateKey';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Type of product sold. May contain truncated values (F. L. M. S.) corrected in clean layer.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_sales',@level2type='COLUMN',@level2name='ProductType';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Region of sale. May contain truncated values corrected in clean layer.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_sales',@level2type='COLUMN',@level2name='Region';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Number of units sold in this transaction. Always greater than zero.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_sales',@level2type='COLUMN',@level2name='UnitsSold';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Total sale value in dollars. Always greater than zero.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='staging_sales',@level2type='COLUMN',@level2name='SalesAmount';
GO

-- ===========================================================
-- TABLE: staging.load_log
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('staging.load_log')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'staging',
        @level1type = 'TABLE',  @level1name = 'load_log';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Pipeline metadata table. Records row counts and timestamps from each load_staging.py run. Append-only -- rows are never deleted. Used by staging_layer_validation_v2.sql for dynamic row count checks instead of hardcoded expected values.',
    @level0type = 'SCHEMA', @level0name = 'staging',
    @level1type = 'TABLE',  @level1name = 'load_log';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Auto-incrementing load run identifier. Primary key.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='load_log',@level2type='COLUMN',@level2name='LoadID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Fully qualified table name that was loaded e.g. staging.staging_sales.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='load_log',@level2type='COLUMN',@level2name='TableName';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Number of rows loaded into the table during this run.',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='load_log',@level2type='COLUMN',@level2name='RowsLoaded';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Timestamp when this load completed. Defaults to GETDATE().',
    @level0type='SCHEMA',@level0name='staging',@level1type='TABLE',@level1name='load_log',@level2type='COLUMN',@level2name='LoadedAt';
GO

/*=============================================================
  LAYER 2: CLEAN SCHEMA
  View-based transformation layer. No data is stored here.
  All views read from staging tables and apply truncation
  fixes, NULL standardisation, and derived column logic.
=============================================================*/

PRINT 'Documenting: clean layer...';

-- ===========================================================
-- VIEW: clean.clean_deliveries
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('clean.clean_deliveries')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'clean',
        @level1type = 'VIEW',   @level1name = 'clean_deliveries';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Standardized delivery records. Expands truncated ShipmentType values (E.->Express P.->Priority S.->Standard) and Region values (M.->MW N.->N S.->S). Replaces NULL DriverID with Unknown. Adds date-only columns, time dimensions, IsLate flag, and DaysVariance.',
    @level0type = 'SCHEMA', @level0name = 'clean',
    @level1type = 'VIEW',   @level1name = 'clean_deliveries';
GO

-- ===========================================================
-- VIEW: clean.clean_sales
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('clean.clean_sales')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'clean',
        @level1type = 'VIEW',   @level1name = 'clean_sales';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Standardized sales records. Expands truncated ProductType values (F.->Freight L.->Large Package M.->Medium Package S.->Small Package) and Region values. Adds SaleDate, time dimensions, and RevenuePerUnit.',
    @level0type = 'SCHEMA', @level0name = 'clean',
    @level1type = 'VIEW',   @level1name = 'clean_sales';
GO

-- ===========================================================
-- VIEW: clean.clean_routes
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('clean.clean_routes')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'clean',
        @level1type = 'VIEW',   @level1name = 'clean_routes';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Standardized route performance records. Expands truncated Region values. Replaces NULL or empty DriverID with Unknown. Adds RouteRunID surrogate key via ROW_NUMBER to uniquely identify each route run since duplicate RouteID+DriverID combinations represent genuine separate runs. Adds StopVariance, HourVariance, StopEfficiencyPct, HourEfficiencyPct.',
    @level0type = 'SCHEMA', @level0name = 'clean',
    @level1type = 'VIEW',   @level1name = 'clean_routes';
GO

-- ===========================================================
-- VIEW: clean.clean_exceptions
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('clean.clean_exceptions')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'clean',
        @level1type = 'VIEW',   @level1name = 'clean_exceptions';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Standardized exception records. Expands truncated ExceptionType values (A.->Address Issue C.->Customer Not Available M.->Mechanical W.->Weather) and Region values. Adds date-only columns, time dimensions, IsResolved flag, and ResolutionDays.',
    @level0type = 'SCHEMA', @level0name = 'clean',
    @level1type = 'VIEW',   @level1name = 'clean_exceptions';
GO

/*=============================================================
  LAYER 3: DW SCHEMA
  Star schema dimension and fact tables. Surrogate integer
  keys (SK) used on all dimensions. Natural keys retained
  as alternate keys for traceability.
=============================================================*/

PRINT 'Documenting: dw layer...';

-- ===========================================================
-- TABLE: dw.dim_date
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.dim_date')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'dim_date';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Date dimension covering 2023-01-01 to 2025-12-31 (1096 rows). DateKey is INT in YYYYMMDD format for fast joins in Power BI. All time intelligence attributes are pre-computed.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'dim_date';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Primary key. Date in YYYYMMDD integer format e.g. 20230101. Used for all fact table date joins.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='DateKey';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Full calendar date.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='FullDate';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Day of week number. 1=Sunday, 7=Saturday.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='DayOfWeek';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Full day name e.g. Monday.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='DayName';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Day of month (1-31).',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='DayOfMonth';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Day of year (1-366).',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='DayOfYear';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='ISO week number of the year.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='WeekOfYear';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Month number (1-12).',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='MonthNumber';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Full month name e.g. January.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='MonthName';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='3-character month abbreviation e.g. Jan.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='MonthShort';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Quarter number (1-4).',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='Quarter';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Quarter label e.g. Q1.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='QuarterName';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Four-digit calendar year e.g. 2023.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='YearNumber';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Year and month in YYYY-MM format e.g. 2023-01. Used for monthly trending in Power BI.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='YearMonth';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Year and quarter in YYYY-Q# format e.g. 2023-Q1.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='YearQuarter';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='1 if Saturday or Sunday, 0 otherwise.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_date',@level2type='COLUMN',@level2name='IsWeekend';
GO

-- ===========================================================
-- TABLE: dw.dim_driver
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.dim_driver')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'dim_driver';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Driver dimension. 20 named drivers plus one Unknown row (IsUnknown=1) to handle deliveries and routes with no DriverID in the source data.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'dim_driver';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Surrogate key. Auto-incrementing integer. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_driver',@level2type='COLUMN',@level2name='DriverSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key. Driver identifier from source data e.g. Driver 1. Unknown for unassigned rows.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_driver',@level2type='COLUMN',@level2name='DriverCode';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Display label for reporting e.g. Driver 1. Unknown Driver for unassigned rows.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_driver',@level2type='COLUMN',@level2name='DriverLabel';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='1 = this is the Unknown placeholder row for unassigned deliveries. 0 = named driver.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_driver',@level2type='COLUMN',@level2name='IsUnknown';
GO

-- ===========================================================
-- TABLE: dw.dim_region
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.dim_region')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'dim_region';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Region dimension. Seven clean region codes after truncation mapping applied in the clean layer. Values: MW, N, NE, NW, S, SE, SW.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'dim_region';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Surrogate key. Auto-incrementing integer. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_region',@level2type='COLUMN',@level2name='RegionSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key. Region code e.g. NE, MW, SE.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_region',@level2type='COLUMN',@level2name='RegionCode';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Human-readable region name e.g. North East, Midwest.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_region',@level2type='COLUMN',@level2name='RegionName';
GO

-- ===========================================================
-- TABLE: dw.dim_product
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.dim_product')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'dim_product';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Product type dimension. Four members: Freight, Large Package, Medium Package, Small Package.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'dim_product';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Surrogate key. Auto-incrementing integer. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_product',@level2type='COLUMN',@level2name='ProductSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key and display name. Values: Freight, Large Package, Medium Package, Small Package.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_product',@level2type='COLUMN',@level2name='ProductType';
GO

-- ===========================================================
-- TABLE: dw.dim_shipment_type
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.dim_shipment_type')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'dim_shipment_type';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Shipment type dimension. Three members: Express, Priority, Standard.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'dim_shipment_type';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Surrogate key. Auto-incrementing integer. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_shipment_type',@level2type='COLUMN',@level2name='ShipmentTypeSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key and display name. Values: Express, Priority, Standard.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_shipment_type',@level2type='COLUMN',@level2name='ShipmentType';
GO

-- ===========================================================
-- TABLE: dw.dim_exception_type
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.dim_exception_type')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'dim_exception_type';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Exception type dimension. Four members: Address Issue, Customer Not Available, Mechanical, Weather.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'dim_exception_type';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Surrogate key. Auto-incrementing integer. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_exception_type',@level2type='COLUMN',@level2name='ExceptionTypeSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key and display name. Values: Address Issue, Customer Not Available, Mechanical, Weather.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_exception_type',@level2type='COLUMN',@level2name='ExceptionType';
GO

-- ===========================================================
-- TABLE: dw.dim_route
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.dim_route')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'dim_route';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Route dimension. Five members: R001 through R005.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'dim_route';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Surrogate key. Auto-incrementing integer. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_route',@level2type='COLUMN',@level2name='RouteSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key. Route identifier. Values: R001, R002, R003, R004, R005.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='dim_route',@level2type='COLUMN',@level2name='RouteID';
GO

-- ===========================================================
-- TABLE: dw.fact_deliveries
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.fact_deliveries')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'fact_deliveries';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Central delivery fact table. Grain: one row per delivery. Referenced by fact_sales and fact_exceptions via DeliveryID. Contains 5000 rows.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'fact_deliveries';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key. Unique delivery identifier. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='DeliveryID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_date. Delivery date in YYYYMMDD integer format.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='DateKey';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_driver.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='DriverSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_region.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='RegionSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_shipment_type.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='ShipmentTypeSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_route.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='RouteSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Denormalized delivery outcome. Values: On-Time, Late, Exception.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='DeliveryStatus';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='1 = Late or Exception delivery, 0 = On-Time. Derived from DeliveryStatus in clean layer.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='IsLate';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='1 = priority delivery, 0 = standard.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='PriorityFlag';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Days between actual and expected delivery date. Positive = delivered late, Negative = early, NULL = no expected date was set.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_deliveries',@level2type='COLUMN',@level2name='DaysVariance';
GO

-- ===========================================================
-- TABLE: dw.fact_sales
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.fact_sales')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'fact_sales';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Sales transaction fact table. Grain: one row per sales transaction. References fact_deliveries via DeliveryID for delivery context. Contains 4000 rows.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'fact_sales';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key. Unique sales transaction identifier. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_sales',@level2type='COLUMN',@level2name='SalesID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to fact_deliveries. Links this sale to its delivery.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_sales',@level2type='COLUMN',@level2name='DeliveryID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_date. Sale date in YYYYMMDD integer format.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_sales',@level2type='COLUMN',@level2name='DateKey';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_product.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_sales',@level2type='COLUMN',@level2name='ProductSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_region.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_sales',@level2type='COLUMN',@level2name='RegionSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Number of units sold. Always greater than zero.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_sales',@level2type='COLUMN',@level2name='UnitsSold';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Total sale value in dollars. Always greater than zero.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_sales',@level2type='COLUMN',@level2name='SalesAmount';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='SalesAmount divided by UnitsSold rounded to 2 decimal places. NULL if UnitsSold is zero.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_sales',@level2type='COLUMN',@level2name='RevenuePerUnit';
GO

-- ===========================================================
-- TABLE: dw.fact_exceptions
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('dw.fact_exceptions')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'dw',
        @level1type = 'TABLE',  @level1name = 'fact_exceptions';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Exception event fact table. Grain: one row per exception event. References fact_deliveries via DeliveryID. Uses two date foreign keys: DateReportedKey and ResolvedDateKey. Contains 1000 rows including approximately 30 open exceptions with NULL ResolvedDateKey.',
    @level0type = 'SCHEMA', @level0name = 'dw',
    @level1type = 'TABLE',  @level1name = 'fact_exceptions';

EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Natural key. Unique exception identifier. Primary key.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='ExceptionID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to fact_deliveries. Delivery affected by this exception.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='DeliveryID';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_date. Date exception was reported in YYYYMMDD format.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='DateReportedKey';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_date. Date exception was resolved in YYYYMMDD format. NULL = exception is still open.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='ResolvedDateKey';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_exception_type.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='ExceptionTypeSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Foreign key to dim_region.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='RegionSK';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='1 = priority exception, 0 = standard.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='PriorityFlag';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='1 = exception resolved, 0 = still open. Consistent with ResolvedDateKey: NULL key = 0, populated key = 1.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='IsResolved';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Hours taken to resolve exception. NULL if still open.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='ResolutionTimeHours';
EXEC sys.sp_addextendedproperty @name='MS_Description', @value='Whole days taken to resolve exception. NULL if still open.',
    @level0type='SCHEMA',@level0name='dw',@level1type='TABLE',@level1name='fact_exceptions',@level2type='COLUMN',@level2name='ResolutionDays';
GO

/*=============================================================
  LAYER 4: REPORTING SCHEMA
  Aggregated views for Power BI and BI consumption. All views
  read from dw tables. No data is stored in this schema.
=============================================================*/

PRINT 'Documenting: reporting layer...';

-- ===========================================================
-- VIEW: reporting.rpt_delivery_performance
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('reporting.rpt_delivery_performance')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'reporting',
        @level1type = 'VIEW',   @level1name = 'rpt_delivery_performance';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Delivery performance metrics grouped by year, month, region, shipment type, and route. Provides on-time rate, late rate, exception rate, and average days variance for trend analysis and operational dashboards.',
    @level0type = 'SCHEMA', @level0name = 'reporting',
    @level1type = 'VIEW',   @level1name = 'rpt_delivery_performance';
GO

-- ===========================================================
-- VIEW: reporting.rpt_sales_performance
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('reporting.rpt_sales_performance')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'reporting',
        @level1type = 'VIEW',   @level1name = 'rpt_sales_performance';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Sales revenue and volume metrics grouped by year, month, region, and product type. Supports revenue trending, product mix analysis, and regional performance comparison.',
    @level0type = 'SCHEMA', @level0name = 'reporting',
    @level1type = 'VIEW',   @level1name = 'rpt_sales_performance';
GO

-- ===========================================================
-- VIEW: reporting.rpt_exception_analysis
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('reporting.rpt_exception_analysis')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'reporting',
        @level1type = 'VIEW',   @level1name = 'rpt_exception_analysis';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Exception counts, resolution rates, and resolution time metrics grouped by year, month, exception type, and region. Supports operational monitoring of exception trends and resolution efficiency.',
    @level0type = 'SCHEMA', @level0name = 'reporting',
    @level1type = 'VIEW',   @level1name = 'rpt_exception_analysis';
GO

-- ===========================================================
-- VIEW: reporting.rpt_driver_performance
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('reporting.rpt_driver_performance')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'reporting',
        @level1type = 'VIEW',   @level1name = 'rpt_driver_performance';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Per-driver KPI summary. One row per driver covering delivery count, on-time rate, late rate, exception rate, exception per delivery ratio, priority rate, average days variance, and regions served. Used for driver ranking and performance management.',
    @level0type = 'SCHEMA', @level0name = 'reporting',
    @level1type = 'VIEW',   @level1name = 'rpt_driver_performance';
GO

-- ===========================================================
-- VIEW: reporting.rpt_route_efficiency
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('reporting.rpt_route_efficiency')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'reporting',
        @level1type = 'VIEW',   @level1name = 'rpt_route_efficiency';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Route-level efficiency metrics aggregated across all runs and drivers per route. Compares planned vs actual stops and hours. Provides stop and hour efficiency percentages. One row per RouteID (R001-R005).',
    @level0type = 'SCHEMA', @level0name = 'reporting',
    @level1type = 'VIEW',   @level1name = 'rpt_route_efficiency';
GO

-- ===========================================================
-- VIEW: reporting.rpt_executive_summary
-- ===========================================================
IF EXISTS (SELECT 1 FROM sys.extended_properties
           WHERE major_id = OBJECT_ID('reporting.rpt_executive_summary')
             AND minor_id = 0 AND name = 'MS_Description')
    EXEC sys.sp_dropextendedproperty
        @name = 'MS_Description',
        @level0type = 'SCHEMA', @level0name = 'reporting',
        @level1type = 'VIEW',   @level1name = 'rpt_executive_summary';

EXEC sys.sp_addextendedproperty
    @name = 'MS_Description',
    @value = 'Single-row KPI snapshot covering all operational areas. Returns one row with delivery rates, total revenue, exception resolution rate, and route efficiency metrics. Designed for Power BI dashboard header cards.',
    @level0type = 'SCHEMA', @level0name = 'reporting',
    @level1type = 'VIEW',   @level1name = 'rpt_executive_summary';
GO

/*=============================================================
  DATA DICTIONARY QUERY
  Run this after applying extended properties to view the
  full data dictionary as a readable result set.
=============================================================*/

PRINT '--- DATA DICTIONARY: FULL OUTPUT ---';

-- Table and view descriptions
SELECT
    s.name                                              AS SchemaName,
    o.type_desc                                         AS ObjectType,
    o.name                                              AS ObjectName,
    CAST(ep.value AS NVARCHAR(MAX))                     AS Description
FROM sys.extended_properties    ep
JOIN sys.objects                o  ON ep.major_id  = o.object_id
JOIN sys.schemas                s  ON o.schema_id  = s.schema_id
WHERE ep.name     = 'MS_Description'
  AND ep.minor_id = 0
  AND s.name IN ('staging', 'clean', 'dw', 'reporting')
ORDER BY
    CASE s.name
        WHEN 'staging'   THEN 1
        WHEN 'clean'     THEN 2
        WHEN 'dw'        THEN 3
        WHEN 'reporting' THEN 4
    END,
    CASE o.type_desc
        WHEN 'USER_TABLE' THEN 1
        WHEN 'VIEW'       THEN 2
    END,
    o.name;

-- Column descriptions
SELECT
    s.name                                              AS SchemaName,
    o.name                                              AS TableName,
    c.name                                              AS ColumnName,
    t.name                                              AS DataType,
    c.max_length                                        AS MaxLength,
    c.is_nullable                                       AS IsNullable,
    CAST(ep.value AS NVARCHAR(MAX))                     AS Description
FROM sys.extended_properties    ep
JOIN sys.objects                o  ON ep.major_id  = o.object_id
JOIN sys.columns                c  ON ep.major_id  = c.object_id
                                   AND ep.minor_id = c.column_id
JOIN sys.types                  t  ON c.user_type_id = t.user_type_id
JOIN sys.schemas                s  ON o.schema_id  = s.schema_id
WHERE ep.name = 'MS_Description'
  AND s.name IN ('staging', 'clean', 'dw', 'reporting')
ORDER BY
    CASE s.name
        WHEN 'staging'   THEN 1
        WHEN 'clean'     THEN 2
        WHEN 'dw'        THEN 3
        WHEN 'reporting' THEN 4
    END,
    o.name,
    c.column_id;

PRINT '--- DATA DICTIONARY COMPLETE ---';
GO
