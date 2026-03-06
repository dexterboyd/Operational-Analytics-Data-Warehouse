-----------------------------------------------------
-- STEP 5: TRANSFER REPORTING OBJECTS
-----------------------------------------------------

DECLARE @reportViews TABLE (Name NVARCHAR(128));
INSERT INTO @reportViews VALUES
('vw_sales_summary'), ('vw_delivery_metrics');  -- Replace with your reporting views

DECLARE report_cursor CURSOR FOR SELECT Name FROM @reportViews;
OPEN report_cursor;
FETCH NEXT FROM report_cursor INTO @t;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @t, 'V') IS NOT NULL
    BEGIN
        EXEC('ALTER SCHEMA reporting TRANSFER dbo.' + @t);
        PRINT 'Transferred reporting view: ' + @t;
    END
    FETCH NEXT FROM report_cursor INTO @t;
END
CLOSE report_cursor;
DEALLOCATE report_cursor;

