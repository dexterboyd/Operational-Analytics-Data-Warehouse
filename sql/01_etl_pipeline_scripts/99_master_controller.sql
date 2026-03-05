/*==============================================================
 MASTER ETL CONTROLLER
 Purpose:
     Orchestrates the full ETL pipeline from staging to reporting.

 Pipeline Flow:
     1. Schema Setup
     2. Staging Validation
     3. Clean Layer Views
     4. Clean Layer Validation
     5. Data Warehouse Load
     6. Reporting Layer

 Behavior:
     ✔ Logs each stage
     ✔ Stops pipeline if errors occur
     ✔ Ensures proper ETL order
==============================================================*/

PRINT '====================================';
PRINT 'STARTING MASTER ETL PIPELINE';
PRINT '====================================';

BEGIN TRY

-----------------------------------------------------
-- STEP 1: SCHEMA SETUP
-----------------------------------------------------

PRINT 'STEP 1: Schema Setup';

:r .\01_schema_setup.sql

PRINT 'Schema Setup Complete';


-----------------------------------------------------
-- STEP 2: STAGING VALIDATION
-----------------------------------------------------

PRINT 'STEP 2: Staging Validation';

:r .\02_staging_validation.sql

PRINT 'Staging Validation Complete';


-----------------------------------------------------
-- STEP 3: CLEAN LAYER VIEWS
-----------------------------------------------------

PRINT 'STEP 3: Clean Layer Views';

:r .\03_clean_layer_views.sql

PRINT 'Clean Views Created';


-----------------------------------------------------
-- STEP 4: CLEAN VALIDATION GATE
-----------------------------------------------------

PRINT 'STEP 4: Clean Layer Validation';

:r .\04_clean_validation_gate.sql

PRINT 'Clean Validation Passed';


-----------------------------------------------------
-- STEP 5: DATA WAREHOUSE LOAD
-----------------------------------------------------

PRINT 'STEP 5: Data Warehouse Load';

:r .\05_dw_load.sql

PRINT 'DW Load Complete';


-----------------------------------------------------
-- STEP 6: REPORTING LAYER
-----------------------------------------------------

PRINT 'STEP 6: Reporting Layer';

:r .\06_reporting_views.sql

PRINT 'Reporting Views Ready';


-----------------------------------------------------
-- PIPELINE SUCCESS
-----------------------------------------------------

PRINT '====================================';
PRINT 'ETL PIPELINE COMPLETED SUCCESSFULLY';
PRINT '====================================';

END TRY

BEGIN CATCH

PRINT '====================================';
PRINT 'ETL PIPELINE FAILED';
PRINT '====================================';

PRINT ERROR_MESSAGE();

THROW;

END CATCH;