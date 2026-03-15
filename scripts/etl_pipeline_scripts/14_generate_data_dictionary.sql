/*==============================================================
  DATA DICTIONARY GENERATOR
  Database: Fedex_Ops_Database
  Version:  2.0

  Purpose:
      Generate a complete metadata catalog for all tables and
      views across the four pipeline schemas. Useful for
      documentation, stakeholder handoff, and BI governance.
      Export results to Excel via SSMS "Save Results As" or
      link directly to a Power BI metadata report.

  Schemas Included:
      staging   - Raw imported source tables
      clean     - Cleansed and standardized views
      dw        - Star schema fact and dimension tables
      reporting - Business-friendly reporting views

  Steps:
      1.  Column catalog         - All columns with type metadata
      2.  Primary key catalog    - PK columns per table (base tables only)
      3.  Foreign key catalog    - FK relationships with referenced columns
      4.  Extended properties    - Column descriptions (if populated)

  Change Log:
      v2.0 - Removed duplicate commented block.
           - Fixed Step 2 PK detection: the original joined
             KEY_COLUMN_USAGE without filtering to PRIMARY KEY
             constraints, causing FK and unique constraint
             columns to be incorrectly flagged as primary keys.
             Fixed by joining INFORMATION_SCHEMA.TABLE_CONSTRAINTS
             and filtering on CONSTRAINT_TYPE = 'PRIMARY KEY'.
           - Fixed Step 2 join: the original omitted the
             TABLE_SCHEMA match condition between TABLES and
             COLUMNS, which would cross-join same-named tables
             in different schemas. Schema match is now enforced
             on all joins.
           - Added Step 3: foreign key catalog sourced from
             sys.foreign_keys and sys.foreign_key_columns,
             showing each FK column alongside its referenced
             table and column. Fills the data lineage gap left
             by the original two-step script.
           - Added Step 4: extended properties query to surface
             any column-level descriptions stored via
             sp_addextendedproperty. Returns a note if none
             have been populated yet, prompting documentation.
==============================================================*/

PRINT '===== DATA DICTIONARY GENERATOR START =====';


/*==============================================================
  STEP 1: COLUMN CATALOG
  Every column across all tables and views in the four schemas.
  Sorted by schema, table, then original column definition order.
==============================================================*/
PRINT '--- STEP 1: COLUMN CATALOG ---';

SELECT
    c.TABLE_SCHEMA                          AS SchemaName,
    t.TABLE_TYPE                            AS ObjectType,      -- BASE TABLE or VIEW
    c.TABLE_NAME                            AS TableName,
    c.ORDINAL_POSITION                      AS ColumnOrder,
    c.COLUMN_NAME                           AS ColumnName,
    c.DATA_TYPE                             AS DataType,
    c.CHARACTER_MAXIMUM_LENGTH              AS MaxLength,       -- For NVARCHAR / VARCHAR
    c.NUMERIC_PRECISION                     AS NumericPrecision,
    c.NUMERIC_SCALE                         AS NumericScale,
    c.IS_NULLABLE                           AS IsNullable,
    c.COLUMN_DEFAULT                        AS DefaultValue
FROM INFORMATION_SCHEMA.COLUMNS     c
JOIN INFORMATION_SCHEMA.TABLES      t
    ON  c.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND c.TABLE_NAME   = t.TABLE_NAME
WHERE c.TABLE_SCHEMA IN ('staging', 'clean', 'dw', 'reporting')
ORDER BY
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.ORDINAL_POSITION;


/*==============================================================
  STEP 2: PRIMARY KEY CATALOG
  Lists PK columns for all base tables.
  Views are excluded (they have no PKs).
  Filtered strictly to PRIMARY KEY constraint type so that FK
  and unique constraint columns are not incorrectly flagged.
==============================================================*/
PRINT '--- STEP 2: PRIMARY KEY CATALOG ---';

SELECT
    c.TABLE_SCHEMA                          AS SchemaName,
    c.TABLE_NAME                            AS TableName,
    c.COLUMN_NAME                           AS ColumnName,
    c.DATA_TYPE                             AS DataType,
    c.ORDINAL_POSITION                      AS ColumnOrder,
    tc.CONSTRAINT_NAME                      AS PKConstraintName
FROM INFORMATION_SCHEMA.COLUMNS             c
JOIN INFORMATION_SCHEMA.TABLES              t
    ON  c.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND c.TABLE_NAME   = t.TABLE_NAME
    AND t.TABLE_TYPE   = 'BASE TABLE'       -- Exclude views
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE    k
    ON  c.TABLE_SCHEMA  = k.TABLE_SCHEMA
    AND c.TABLE_NAME    = k.TABLE_NAME
    AND c.COLUMN_NAME   = k.COLUMN_NAME
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS   tc
    ON  k.TABLE_SCHEMA      = tc.TABLE_SCHEMA
    AND k.TABLE_NAME        = tc.TABLE_NAME
    AND k.CONSTRAINT_NAME   = tc.CONSTRAINT_NAME
    AND tc.CONSTRAINT_TYPE  = 'PRIMARY KEY' -- Only true PKs; excludes FK and UNIQUE
WHERE c.TABLE_SCHEMA IN ('staging', 'clean', 'dw', 'reporting')
ORDER BY
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    k.ORDINAL_POSITION;


/*==============================================================
  STEP 3: FOREIGN KEY CATALOG
  Lists every FK relationship in the warehouse with the source
  column, the referenced table, and the referenced column.
  Provides the data lineage map missing from the original script.
==============================================================*/
PRINT '--- STEP 3: FOREIGN KEY CATALOG ---';

SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id)         AS SchemaName,
    OBJECT_NAME(fk.parent_object_id)                AS FactTable,
    COL_NAME(fkc.parent_object_id,
             fkc.parent_column_id)                  AS FKColumn,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id)     AS ReferencedSchema,
    OBJECT_NAME(fk.referenced_object_id)            AS ReferencedTable,
    COL_NAME(fkc.referenced_object_id,
             fkc.referenced_column_id)              AS ReferencedColumn,
    fk.name                                         AS ConstraintName,
    CASE fk.delete_referential_action_desc
        WHEN 'NO_ACTION'  THEN 'NO ACTION'
        WHEN 'CASCADE'    THEN 'CASCADE'
        ELSE fk.delete_referential_action_desc
    END                                             AS OnDelete
FROM sys.foreign_keys           fk
JOIN sys.foreign_key_columns    fkc
    ON fk.object_id = fkc.constraint_object_id
WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) IN ('staging', 'dw')
ORDER BY
    OBJECT_SCHEMA_NAME(fk.parent_object_id),
    OBJECT_NAME(fk.parent_object_id),
    fkc.constraint_column_id;


/*==============================================================
  STEP 4: COLUMN DESCRIPTIONS (EXTENDED PROPERTIES)
  Surfaces any column-level documentation stored via
  sp_addextendedproperty with property name 'MS_Description'.

  To add a description to a column:
      EXEC sp_addextendedproperty
          @name       = N'MS_Description',
          @value      = N'Total revenue for this sales transaction.',
          @level0type = N'SCHEMA',  @level0name = N'dw',
          @level1type = N'TABLE',   @level1name = N'fact_sales',
          @level2type = N'COLUMN',  @level2name = N'SalesAmount';
==============================================================*/
PRINT '--- STEP 4: COLUMN DESCRIPTIONS (EXTENDED PROPERTIES) ---';

SELECT
    s.name                  AS SchemaName,
    t.name                  AS TableName,
    c.name                  AS ColumnName,
    CAST(ep.value AS NVARCHAR(MAX)) AS Description
FROM sys.extended_properties    ep
JOIN sys.columns                c
    ON  ep.major_id     = c.object_id
    AND ep.minor_id     = c.column_id
JOIN sys.tables                 t
    ON  c.object_id     = t.object_id
JOIN sys.schemas                s
    ON  t.schema_id     = s.schema_id
WHERE ep.name           = 'MS_Description'
  AND ep.class          = 1               -- OBJECT_OR_COLUMN
  AND s.name IN ('staging', 'clean', 'dw', 'reporting')
ORDER BY
    s.name,
    t.name,
    c.column_id;

-- If no rows are returned, no extended property descriptions have been added yet.
-- Use sp_addextendedproperty (example above) to document key columns.

PRINT '===== DATA DICTIONARY GENERATOR COMPLETE =====';
