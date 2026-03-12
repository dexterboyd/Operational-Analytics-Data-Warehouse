/*
============================================================
DATA DICTIONARY GENERATOR - PRODUCTION READY
Purpose:
    Automatically generate a catalog of all tables and columns
    in the warehouse for documentation, auditing, or BI purposes.

Schemas included:
    - staging   : Raw imported data
    - clean     : Cleansed and transformed views
    - dw        : Data warehouse fact & dimension tables
    - reporting : Reporting layer views for dashboards

Notes:
    - This query retrieves metadata from INFORMATION_SCHEMA
    - Useful for creating a data dictionary report or Excel export
============================================================
*/

------------------------------------------------------------
-- STEP 1: LIST ALL COLUMNS PER TABLE
-- Purpose: Get column-level metadata for all relevant schemas
-- Details captured:
--    - Table schema & name
--    - Column name & data type
--    - Max length for character columns
--    - Numeric precision for numeric columns
--    - Nullable flag
------------------------------------------------------------
SELECT
    TABLE_SCHEMA,                     -- Schema of the table
    TABLE_NAME,                       -- Table or view name
    COLUMN_NAME,                      -- Column name
    DATA_TYPE,                        -- SQL data type
    CHARACTER_MAXIMUM_LENGTH AS MaxLength, -- Max length for text fields
    NUMERIC_PRECISION,                -- Precision for numeric fields
    IS_NULLABLE                       -- Yes/No if column allows NULLs
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('staging','clean','dw','reporting')
ORDER BY
    TABLE_SCHEMA,
    TABLE_NAME,
    ORDINAL_POSITION;                 -- Maintain column order as defined
------------------------------------------------------------

------------------------------------------------------------
-- STEP 2: IDENTIFY PRIMARY KEYS
-- Purpose: Flag columns that are primary keys for reference
-- Notes:
--    - Joins COLUMNS with KEY_COLUMN_USAGE
--    - Useful for documentation and data lineage diagrams
------------------------------------------------------------
SELECT
    t.TABLE_SCHEMA,                   -- Table schema
    t.TABLE_NAME,                     -- Table name
    c.COLUMN_NAME,                    -- Column name
    c.DATA_TYPE,                      -- Column type
    c.IS_NULLABLE,                    -- Nullable?
    CASE
        WHEN k.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY' -- Flag PKs
        ELSE ''
    END AS KeyType
FROM INFORMATION_SCHEMA.TABLES t
JOIN INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_NAME = c.TABLE_NAME
    AND t.TABLE_SCHEMA = c.TABLE_SCHEMA    -- Ensure schema match
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
    ON c.COLUMN_NAME = k.COLUMN_NAME
    AND c.TABLE_NAME = k.TABLE_NAME
    AND c.TABLE_SCHEMA = k.TABLE_SCHEMA    -- Ensure schema match for PK detection
WHERE t.TABLE_SCHEMA IN ('staging','clean','dw','reporting')
ORDER BY
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.ORDINAL_POSITION;                 -- Maintain original column order

/*
/*
============================================================
DATA DICTIONARY GENERATOR
Purpose:
    Automatically list every table and column in the warehouse

Schemas included:
    staging
    clean
    dw
    reporting
============================================================
*/

SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH AS MaxLength,
    NUMERIC_PRECISION,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('staging','clean','dw','reporting')
ORDER BY
    TABLE_SCHEMA,
    TABLE_NAME,
    ORDINAL_POSITION;
--------------------------------------------------------

SELECT
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.IS_NULLABLE,
    CASE
        WHEN k.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY'
        ELSE ''
    END AS KeyType
FROM INFORMATION_SCHEMA.TABLES t
JOIN INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_NAME = c.TABLE_NAME
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
    ON c.COLUMN_NAME = k.COLUMN_NAME
    AND c.TABLE_NAME = k.TABLE_NAME
WHERE t.TABLE_SCHEMA IN ('staging','clean','dw','reporting')
ORDER BY
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.ORDINAL_POSITION;
	*/
