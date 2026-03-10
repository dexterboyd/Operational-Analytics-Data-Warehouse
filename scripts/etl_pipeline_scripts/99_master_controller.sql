/*
==============================================================
MASTER ETL CONTROLLER
File: master_etl_controller.sql

Purpose:
    Orchestrates the entire ETL pipeline from staging to reporting.
    Central control script that ensures proper execution order and error handling.

Pipeline Flow:
    1. Schema Setup
    2. Staging Validation
    3. Clean Layer Views
    4. Clean Layer Validation
    5. Data Warehouse Load
    6. Reporting Layer

Behavior:
    ✔ Logs each stage using PRINT statements
    ✔ Stops pipeline if any errors occur
    ✔ Ensures correct ETL sequence
    ✔ Facilitates modular development using :r to include scripts
==============================================================
*/

PRINT '====================================';
PRINT 'STARTING MASTER ETL PIPELINE';
PRINT '====================================';

-- TRY/CATCH block to handle errors gracefully
BEGIN TRY

-----------------------------------------------------
-- STEP 1: SCHEMA SETUP
-- Purpose: Ensure required schemas (staging, clean, dw, reporting) exist
-- Script: 01_schema_setup.sql
-----------------------------------------------------
PRINT 'STEP 1: Schema Setup';

:r .\01_schema_setup.sql  -- Modular inclusion of schema creation script

PRINT 'Schema Setup Complete';

-----------------------------------------------------
-- STEP 2: STAGING VALIDATION
-- Purpose: Validate raw data before loading into clean layer
-- Checks: Row counts, required fields, basic business rules
-- Script: 02_staging_validation.sql
-----------------------------------------------------
PRINT 'STEP 2: Staging Validation';

:r .\02_staging_validation.sql

PRINT 'Staging Validation Complete';

-----------------------------------------------------
-- STEP 3: CLEAN LAYER VIEWS
-- Purpose: Create standard clean views with transformations applied
-- Script: 03_clean_layer_views.sql
-----------------------------------------------------
PRINT 'STEP 3: Clean Layer Views';

:r .\03_clean_layer_views.sql

PRINT 'Clean Views Created';

-----------------------------------------------------
-- STEP 4: CLEAN VALIDATION GATE
-- Purpose: Run automated quality checks on clean views
-- Behavior: Stops ETL if validation fails
-- Script: 04_clean_validation_gate.sql
-----------------------------------------------------
PRINT 'STEP 4: Clean Layer Validation';

:r .\04_clean_validation_gate.sql

PRINT 'Clean Validation Passed';

-----------------------------------------------------
-- STEP 5: DATA WAREHOUSE LOAD
-- Purpose: Load dimensions and facts with surrogate keys
-- Ensures referential integrity and prepares warehouse for reporting
-- Script: 05_dw_load.sql
-----------------------------------------------------
PRINT 'STEP 5: Data Warehouse Load';

:r .\05_dw_load.sql

PRINT 'DW Load Complete';

-----------------------------------------------------
-- STEP 6: REPORTING LAYER
-- Purpose: Create production-ready views for dashboards
-- Script: 06_reporting_views.sql
-----------------------------------------------------
PRINT 'STEP 6: Reporting Layer';

:r .\06_reporting_views.sql

PRINT 'Reporting Views Ready';

-----------------------------------------------------
-- PIPELINE SUCCESS MESSAGE
-- Indicates that all steps completed without errors
-----------------------------------------------------
PRINT '====================================';
PRINT 'ETL PIPELINE COMPLETED SUCCESSFULLY';
PRINT '====================================';

END TRY

-----------------------------------------------------
-- ERROR HANDLING
-- Catches any error in the pipeline and halts execution
-- Prints the error message for debugging
-----------------------------------------------------
BEGIN CATCH

PRINT '====================================';
PRINT 'ETL PIPELINE FAILED';
PRINT '====================================';

PRINT ERROR_MESSAGE();  -- Show the exact SQL error

-- Rethrow the error to stop further execution
THROW;

END CATCH;

/*
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
*/