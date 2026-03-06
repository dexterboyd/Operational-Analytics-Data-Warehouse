-----------------------------------------------------
-- STEP 3: TRANSFER DW DIMENSION TABLES
-- (moving pre-existing DW tables into the dw schema)
-----------------------------------------------------

DECLARE @dimTables TABLE (Name NVARCHAR(128));
INSERT INTO @dimTables VALUES
('dim_shipment_type'), ('dim_route'), ('dim_date'), ('dim_region'), 
('dim_product'), ('dim_priority_flag'), ('dim_exception_type'), ('dim_driver');

DECLARE @t NVARCHAR(128);
DECLARE dim_cursor CURSOR FOR SELECT Name FROM @dimTables;
OPEN dim_cursor;
FETCH NEXT FROM dim_cursor INTO @t;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @t, 'U') IS NOT NULL
    BEGIN
        EXEC('ALTER SCHEMA dw TRANSFER dbo.' + @t);
        PRINT 'Transferred DW dimension table: ' + @t;
    END
    FETCH NEXT FROM dim_cursor INTO @t;
END
CLOSE dim_cursor;
DEALLOCATE dim_cursor;


-----------------------------------------------------
-- STEP 4: TRANSFER DW FACT TABLES
-- (moving pre-existing DW tables into the dw schema)
-----------------------------------------------------

DECLARE @factTables TABLE (Name NVARCHAR(128));
INSERT INTO @factTables VALUES
('fact_sales'), ('fact_routes'), ('fact_exceptions'), ('fact_deliveries');

DECLARE fact_cursor CURSOR FOR SELECT Name FROM @factTables;
OPEN fact_cursor;
FETCH NEXT FROM fact_cursor INTO @t;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @t, 'U') IS NOT NULL
    BEGIN
        EXEC('ALTER SCHEMA dw TRANSFER dbo.' + @t);
        PRINT 'Transferred DW fact table: ' + @t;
    END
    FETCH NEXT FROM fact_cursor INTO @t;
END
CLOSE fact_cursor;
DEALLOCATE fact_cursor;
