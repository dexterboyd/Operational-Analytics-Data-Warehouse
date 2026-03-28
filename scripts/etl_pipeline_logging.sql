/*=============================================================
  ETL PIPELINE LOGGING
  Database: Fedex_Ops_Database
  Version:  1.0

  Purpose:
      Creates the pipeline_log table and three stored
      procedures that together provide a full audit trail
      of every pipeline run -- which steps ran, when they
      started and finished, how long they took, whether they
      passed or failed, and what error occurred if they failed.

  Run Order:
      Run this script once before any pipeline runs.
      It is safe to re-run -- all objects use IF NOT EXISTS
      guards so existing log data is never lost.

  Objects Created:
      staging.pipeline_log         -- audit trail table
      staging.usp_log_step_start   -- call at the start of each step
      staging.usp_log_step_end     -- call at the end of each step
      staging.usp_log_step_error   -- call in CATCH blocks on failure

  How to Use:
      At the start of each pipeline step:
          EXEC staging.usp_log_step_start
              @RunID    = @RunID,
              @StepName = 'staging_load';

      At the end of each step if it succeeded:
          EXEC staging.usp_log_step_end
              @RunID    = @RunID,
              @StepName = 'staging_load',
              @RowCount = 10300;   -- pass 0 if not applicable

      In the CATCH block if the step failed:
          EXEC staging.usp_log_step_error
              @RunID    = @RunID,
              @StepName = 'staging_load',
              @ErrorMsg = ERROR_MESSAGE();

  Pipeline Steps Logged:
      Step 1: staging_setup        etl_staging_setup_v5.sql
      Step 2: staging_load         load_staging.py
      Step 3: staging_validation   staging_layer_validation_v2.sql
      Step 4: clean_build          clean_layer_v1.sql
      Step 5: clean_gate           07_clean_validation_gate_v3_0.sql
      Step 6: dw_build             dw_layer_v1.sql
      Step 7: dw_gate              09_dw_validation_v3_0.sql
      Step 8: reporting_build      reporting_layer_v1.sql
      Step 9: reporting_gate       reporting_validation_gate_v1.sql

  Querying the Log:
      -- Latest pipeline run summary
      SELECT * FROM staging.pipeline_log
      WHERE RunID = (SELECT MAX(RunID) FROM staging.pipeline_log)
      ORDER BY StepStartTime;

      -- All failed steps across all runs
      SELECT * FROM staging.pipeline_log
      WHERE StepStatus = 'FAILED'
      ORDER BY StepStartTime DESC;

      -- Average duration per step across all runs
      SELECT StepName,
             AVG(DurationSeconds) AS AvgSeconds,
             MAX(DurationSeconds) AS MaxSeconds,
             COUNT(*)             AS RunCount
      FROM staging.pipeline_log
      WHERE StepStatus = 'PASSED'
      GROUP BY StepName
      ORDER BY StepName;

=============================================================*/

USE Fedex_Ops_Database;
GO

/*=============================================================
  STEP 1: CREATE pipeline_log TABLE
  Append-only audit trail. Rows are never deleted -- full
  history is retained across all pipeline runs.

  RunID groups all steps from the same pipeline execution.
  It is generated once per full pipeline run by the caller
  and passed to every step logging call.
=============================================================*/

IF OBJECT_ID('staging.pipeline_log', 'U') IS NULL
BEGIN
    CREATE TABLE staging.pipeline_log (
        LogID           INT IDENTITY(1,1) NOT NULL,  -- Auto-incrementing row identifier
        RunID           INT               NOT NULL,  -- Groups all steps from one pipeline run
        StepOrder       INT               NOT NULL,  -- Execution sequence (1-9)
        StepName        NVARCHAR(100)     NOT NULL,  -- Short step identifier e.g. staging_load
        StepDescription NVARCHAR(500)     NULL,      -- Human-readable step description
        StepStatus      NVARCHAR(10)      NOT NULL,  -- RUNNING, PASSED, FAILED
        StepStartTime   DATETIME2         NOT NULL,  -- When the step started
        StepEndTime     DATETIME2         NULL,      -- When the step finished (NULL if still running)
        DurationSeconds INT               NULL,      -- Elapsed seconds (NULL if still running)
        RowsAffected    INT               NULL,      -- Rows loaded or processed (0 if not applicable)
        ErrorMessage    NVARCHAR(MAX)     NULL,      -- Error message if StepStatus = FAILED
        CONSTRAINT PK_pipeline_log PRIMARY KEY (LogID)
    );
    PRINT 'Table created: staging.pipeline_log';
END
ELSE
    PRINT 'Table already exists: staging.pipeline_log';
GO

/*=============================================================
  STEP 2: CREATE usp_log_step_start
  Call at the beginning of each pipeline step. Inserts a
  RUNNING row and returns the LogID of the new row so
  usp_log_step_end and usp_log_step_error can update it.

  Parameters:
      @RunID       INT           -- Pipeline run identifier
      @StepOrder   INT           -- Step sequence number (1-9)
      @StepName    NVARCHAR(100) -- Short step name
      @StepDesc    NVARCHAR(500) -- Optional description
=============================================================*/

IF OBJECT_ID('staging.usp_log_step_start', 'P') IS NOT NULL
    DROP PROCEDURE staging.usp_log_step_start;
GO

CREATE PROCEDURE staging.usp_log_step_start
    @RunID      INT,
    @StepOrder  INT,
    @StepName   NVARCHAR(100),
    @StepDesc   NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO staging.pipeline_log (
        RunID,
        StepOrder,
        StepName,
        StepDescription,
        StepStatus,
        StepStartTime,
        StepEndTime,
        DurationSeconds,
        RowsAffected,
        ErrorMessage
    )
    VALUES (
        @RunID,
        @StepOrder,
        @StepName,
        @StepDesc,
        'RUNNING',
        GETDATE(),
        NULL,
        NULL,
        NULL,
        NULL
    );

    PRINT 'STARTED [' + @StepName + '] RunID=' + CAST(@RunID AS NVARCHAR)
        + ' at ' + CONVERT(NVARCHAR, GETDATE(), 120);
END;
GO

PRINT 'Procedure created: staging.usp_log_step_start';
GO

/*=============================================================
  STEP 3: CREATE usp_log_step_end
  Call at the end of each pipeline step when it succeeds.
  Updates the RUNNING row to PASSED, records the end time,
  calculates duration in seconds, and records rows affected.

  Parameters:
      @RunID        INT -- Pipeline run identifier
      @StepName     NVARCHAR(100) -- Must match the name used in usp_log_step_start
      @RowsAffected INT -- Rows loaded or processed. Pass 0 if not applicable.
=============================================================*/

IF OBJECT_ID('staging.usp_log_step_end', 'P') IS NOT NULL
    DROP PROCEDURE staging.usp_log_step_end;
GO

CREATE PROCEDURE staging.usp_log_step_end
    @RunID        INT,
    @StepName     NVARCHAR(100),
    @RowsAffected INT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EndTime   DATETIME2 = GETDATE();
    DECLARE @StartTime DATETIME2;

    -- Retrieve start time from the RUNNING row for this step
    SELECT @StartTime = StepStartTime
    FROM staging.pipeline_log
    WHERE RunID     = @RunID
      AND StepName  = @StepName
      AND StepStatus = 'RUNNING';

    UPDATE staging.pipeline_log
    SET
        StepStatus      = 'PASSED',
        StepEndTime     = @EndTime,
        DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime),
        RowsAffected    = @RowsAffected
    WHERE RunID      = @RunID
      AND StepName   = @StepName
      AND StepStatus = 'RUNNING';

    PRINT 'PASSED  [' + @StepName + '] RunID=' + CAST(@RunID AS NVARCHAR)
        + ' Duration=' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS NVARCHAR) + 's'
        + ' Rows=' + CAST(@RowsAffected AS NVARCHAR);
END;
GO

PRINT 'Procedure created: staging.usp_log_step_end';
GO

/*=============================================================
  STEP 4: CREATE usp_log_step_error
  Call inside CATCH blocks when a pipeline step fails.
  Updates the RUNNING row to FAILED and records the error
  message. The pipeline should THROW after calling this
  to propagate the failure to the caller.

  Parameters:
      @RunID    INT           -- Pipeline run identifier
      @StepName NVARCHAR(100) -- Must match the name used in usp_log_step_start
      @ErrorMsg NVARCHAR(MAX) -- Error message. Pass ERROR_MESSAGE() from CATCH block.
=============================================================*/

IF OBJECT_ID('staging.usp_log_step_error', 'P') IS NOT NULL
    DROP PROCEDURE staging.usp_log_step_error;
GO

CREATE PROCEDURE staging.usp_log_step_error
    @RunID    INT,
    @StepName NVARCHAR(100),
    @ErrorMsg NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EndTime   DATETIME2 = GETDATE();
    DECLARE @StartTime DATETIME2;

    SELECT @StartTime = StepStartTime
    FROM staging.pipeline_log
    WHERE RunID      = @RunID
      AND StepName   = @StepName
      AND StepStatus = 'RUNNING';

    UPDATE staging.pipeline_log
    SET
        StepStatus      = 'FAILED',
        StepEndTime     = @EndTime,
        DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime),
        ErrorMessage    = @ErrorMsg
    WHERE RunID      = @RunID
      AND StepName   = @StepName
      AND StepStatus = 'RUNNING';

    PRINT 'FAILED  [' + @StepName + '] RunID=' + CAST(@RunID AS NVARCHAR)
        + ' Error: ' + LEFT(@ErrorMsg, 200);
END;
GO

PRINT 'Procedure created: staging.usp_log_step_error';
GO

/*=============================================================
  STEP 5: VERIFY SETUP
  Confirms the table and all three procedures were created.
=============================================================*/

PRINT '--- PIPELINE LOGGING SETUP COMPLETE ---';

SELECT
    o.type_desc                                         AS ObjectType,
    s.name + '.' + o.name                               AS ObjectName,
    o.create_date                                       AS CreatedAt
FROM sys.objects  o
JOIN sys.schemas  s ON o.schema_id = s.schema_id
WHERE s.name  = 'staging'
  AND o.name IN ('pipeline_log', 'usp_log_step_start',
                 'usp_log_step_end', 'usp_log_step_error')
ORDER BY o.type_desc DESC, o.name;

PRINT '--- END OF SETUP ---';
GO
