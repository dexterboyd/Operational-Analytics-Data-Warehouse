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