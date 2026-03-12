/*==============================================================
  CLEAN LAYER VALIDATION GATE
  Purpose:
      Production data quality validation before DW load.
      Stops pipeline if critical checks fail.

  Behavior:
      ✔ Executes row & business rule checks
      ✔ Logs PASS / FAIL for each check
      ✔ Increments failure counter for DW load decision

  Pipeline Stage:
      staging → clean → VALIDATION → dw
==============================================================*/

PRINT '===== CLEAN VALIDATION START =====';

/*==============================================================
  VALIDATION CONTROL VARIABLES
  Purpose:
      Track number of failed checks and dynamic check names
==============================================================*/
DECLARE @FailureCount INT = 0;       -- Counts failed checks
DECLARE @CheckName NVARCHAR(200);    -- Stores check description
DECLARE @BadRows INT;                -- Stores number of invalid rows per check

/*==============================================================
  CHECK 1: SALES REQUIRED FIELDS
  Purpose:
      Ensure critical sales fields are not null
      (DateKey, SalesAmount, UnitsSold)
==============================================================*/
SET @CheckName = 'vw_sales required fields';

SELECT @BadRows = COUNT(*)
FROM clean.vw_sales
WHERE DateKey IS NULL
   OR SalesAmount IS NULL
   OR UnitsSold IS NULL;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*==============================================================
  CHECK 2: DELIVERY BUSINESS RULE
  Purpose:
      Ensure deliveries past ExpectedDeliveryDate are flagged 'Late'
==============================================================*/
SET @CheckName = 'Late delivery flag validation';

SELECT @BadRows = COUNT(*)
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'Late';

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*==============================================================
  CHECK 3: PRIORITY FLAG NORMALIZATION
  Purpose:
      Ensure PriorityFlag values are correctly normalized (0/1)
==============================================================*/
SET @CheckName = 'PriorityFlag normalization';

SELECT @BadRows = COUNT(*)
FROM clean.vw_deliveries
WHERE PriorityFlag NOT IN (0,1);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*==============================================================
  CHECK 4: ROUTE DATA VALIDITY
  Purpose:
      Ensure planned & actual hours are positive for all routes
==============================================================*/
SET @CheckName = 'Route hours validation';

SELECT @BadRows = COUNT(*)
FROM clean.vw_routes
WHERE PlannedHours <= 0
   OR ActualHours <= 0;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

/*==============================================================
  FINAL PIPELINE DECISION
  Purpose:
      Stop DW load if any validation checks failed
==============================================================*/
IF @FailureCount > 0
BEGIN
    PRINT '===== CLEAN VALIDATION FAILED =====';

    THROW 51000, -- Custom error number
          'Clean Layer Validation Failed. DW Load Cancelled.',
          1;
END
ELSE
BEGIN
    PRINT '===== CLEAN VALIDATION PASSED =====';
END;

/*
/*==============================================================
 CLEAN LAYER VALIDATION GATE
 Purpose:
     Production data quality validation before DW load.

 Behavior:
     ✔ Runs validation checks
     ✔ Logs PASS / FAIL results
     ✔ Stops pipeline if validation fails

 Pipeline Stage:
     staging → clean → VALIDATION → dw
==============================================================*/

PRINT '===== CLEAN VALIDATION START =====';

-----------------------------------------------------
-- VALIDATION CONTROL VARIABLES
-----------------------------------------------------
DECLARE @FailureCount INT = 0;
DECLARE @CheckName NVARCHAR(200);
DECLARE @BadRows INT;

-----------------------------------------------------
-- CHECK 1: SALES REQUIRED FIELDS
-----------------------------------------------------
SET @CheckName = 'vw_sales required fields';

SELECT @BadRows = COUNT(*)
FROM clean.vw_sales
WHERE DateKey IS NULL
   OR SalesAmount IS NULL
   OR UnitsSold IS NULL;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName + ' | Bad Rows = ' + CAST(@BadRows AS VARCHAR);
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

-----------------------------------------------------
-- CHECK 2: DELIVERY BUSINESS RULE
-----------------------------------------------------
SET @CheckName = 'Late delivery flag validation';

SELECT @BadRows = COUNT(*)
FROM clean.vw_deliveries
WHERE DeliveryDate > ExpectedDeliveryDate
  AND DeliveryStatus <> 'Late';

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName;
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

-----------------------------------------------------
-- CHECK 3: PRIORITY NORMALIZATION
-----------------------------------------------------
SET @CheckName = 'PriorityFlag normalization';

SELECT @BadRows = COUNT(*)
FROM clean.vw_deliveries
WHERE PriorityFlag NOT IN (0,1);

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName;
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

-----------------------------------------------------
-- CHECK 4: ROUTE DATA VALIDITY
-----------------------------------------------------
SET @CheckName = 'Route hours validation';

SELECT @BadRows = COUNT(*)
FROM clean.vw_routes
WHERE PlannedHours <= 0
   OR ActualHours <= 0;

IF @BadRows > 0
BEGIN
    PRINT 'FAIL: ' + @CheckName;
    SET @FailureCount += 1;
END
ELSE
    PRINT 'PASS: ' + @CheckName;

-----------------------------------------------------
-- FINAL PIPELINE DECISION
-----------------------------------------------------
IF @FailureCount > 0
BEGIN
    PRINT '===== CLEAN VALIDATION FAILED =====';

    THROW 51000,
    'Clean Layer Validation Failed. DW Load Cancelled.',
    1;
END
ELSE
BEGIN
    PRINT '===== CLEAN VALIDATION PASSED =====';
END
*/