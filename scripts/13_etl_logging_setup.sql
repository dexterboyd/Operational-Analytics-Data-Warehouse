/*==============================================================
  ETL LOGGING FRAMEWORK SETUP
  Database: Fedex_Ops_Database
  Schema:   dw
  Version:  2.0

  Purpose:
      Create audit and logging tables for ETL pipeline runs.
      Provides visibility into execution times, row counts,
      statuses, and errors at both the run level and the
      individual step level.

  Components:
      1. dw.etl_run_log        — Overall ETL run metadata
      2. dw.etl_step_log       — Step-level detail within a run
      3. dw.usp_start_etl_run  — Opens a new run record; returns RunID
      4. dw.usp_end_etl_run    — Closes a run with final status
      5. dw.usp_log_etl_step   — Inserts a completed step record

  Usage Pattern:
      DECLARE @RunID INT;
      EXEC dw.usp_start_etl_run @PipelineName = 'DW Full Load', @RunID = @RunID OUTPUT;

      DECLARE @StepStart DATETIME2 = SYSUTCDATETIME();
      -- ... do ETL work ...
      EXEC dw.usp_log_etl_step
          @RunID         = @RunID,
          @StepName      = 'Load fact_sales',
          @StartTime     = @StepStart,
          @RowsProcessed = @@ROWCOUNT,
          @Status        = 'SUCCESS';

      EXEC dw.usp_end_etl_run @RunID = @RunID, @Status = 'SUCCESS';

  Change Log:
      v2.0 - Removed duplicate commented block.
           - Added IF OBJECT_ID ... IS NULL existence checks on
             both CREATE TABLE statements so the script is safe
             to re-run without "object already exists" errors.
           - Enabled the FK constraint on etl_step_log.RunID
             (was commented out); orphaned step records with
             invalid RunIDs can no longer accumulate silently.
           - Added CHECK constraints on all Status columns to
             enforce valid values: RUNNING, SUCCESS, FAILED,
             SKIPPED.
           - Fixed usp_log_etl_step: both StartTime and EndTime
             were set to GETDATE() at insert time, making every
             step duration zero. The procedure now accepts
             @StartTime as a required parameter (captured by
             the caller before the step begins) and sets
             EndTime to SYSUTCDATETIME() at insert time.
           - Switched all timestamps from GETDATE() to
             SYSUTCDATETIME() (UTC, higher precision) for
             consistency across time zones and SQL Agent jobs.
           - Added usp_start_etl_run and usp_end_etl_run to
             manage the run-level log records. Without these,
             callers had to manually INSERT and UPDATE
             etl_run_log, which was error-prone.
==============================================================*/

USE Fedex_Ops_Database;
GO

/*==============================================================
  TABLE 1: dw.etl_run_log
  One row per ETL pipeline execution.
  Opened by usp_start_etl_run; closed by usp_end_etl_run.
==============================================================*/
IF OBJECT_ID('dw.etl_run_log', 'U') IS NULL
BEGIN
    CREATE TABLE dw.etl_run_log
    (
        RunID         INT           NOT NULL IDENTITY(1,1)
                          CONSTRAINT PK_etl_run_log PRIMARY KEY,
        PipelineName  NVARCHAR(200) NOT NULL,              -- e.g. 'DW Full Load'
        StartTime     DATETIME2     NOT NULL,               -- UTC timestamp; set at run open
        EndTime       DATETIME2     NULL,                   -- NULL until run closes
        Status        VARCHAR(20)   NOT NULL
                          CONSTRAINT CHK_etl_run_status
                          CHECK (Status IN ('RUNNING','SUCCESS','FAILED','SKIPPED')),
        ErrorMessage  NVARCHAR(MAX) NULL                    -- Populated on FAILED runs
    );
    PRINT 'Table created: dw.etl_run_log';
END
ELSE
    PRINT 'Table already exists: dw.etl_run_log';
GO

/*==============================================================
  TABLE 2: dw.etl_step_log
  One row per ETL step within a run.
  Inserted by usp_log_etl_step after the step completes.
  FK to etl_run_log ensures orphaned step records cannot exist.
==============================================================*/
IF OBJECT_ID('dw.etl_step_log', 'U') IS NULL
BEGIN
    CREATE TABLE dw.etl_step_log
    (
        StepLogID     INT           NOT NULL IDENTITY(1,1)
                          CONSTRAINT PK_etl_step_log PRIMARY KEY,
        RunID         INT           NOT NULL
                          CONSTRAINT FK_etl_step_run
                          REFERENCES dw.etl_run_log(RunID),
        StepName      VARCHAR(200)  NOT NULL,               -- e.g. 'Load fact_sales'
        StartTime     DATETIME2     NOT NULL,               -- Captured by caller before step
        EndTime       DATETIME2     NOT NULL,               -- Set by procedure at insert time
        DurationSec   AS DATEDIFF(SECOND, StartTime, EndTime),  -- Computed duration column
        RowsProcessed INT           NULL,                   -- @@ROWCOUNT from the step
        Status        VARCHAR(20)   NOT NULL
                          CONSTRAINT CHK_etl_step_status
                          CHECK (Status IN ('RUNNING','SUCCESS','FAILED','SKIPPED')),
        ErrorMessage  NVARCHAR(MAX) NULL                    -- Populated on FAILED steps
    );
    PRINT 'Table created: dw.etl_step_log';
END
ELSE
    PRINT 'Table already exists: dw.etl_step_log';
GO

/*==============================================================
  PROCEDURE 1: dw.usp_start_etl_run
  Opens a new run record with Status = 'RUNNING' and returns
  the new RunID to the caller via OUTPUT parameter.

  The caller stores @RunID and passes it to all subsequent
  usp_log_etl_step calls and to usp_end_etl_run.
==============================================================*/
CREATE OR ALTER PROCEDURE dw.usp_start_etl_run
(
    @PipelineName NVARCHAR(200),
    @RunID        INT OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dw.etl_run_log (PipelineName, StartTime, Status)
    VALUES (@PipelineName, SYSUTCDATETIME(), 'RUNNING');

    SET @RunID = SCOPE_IDENTITY();
END;
GO

/*==============================================================
  PROCEDURE 2: dw.usp_end_etl_run
  Closes a run record by setting EndTime and final Status.
  Call this at the end of the pipeline, in both the success
  path and the CATCH block.
==============================================================*/
CREATE OR ALTER PROCEDURE dw.usp_end_etl_run
(
    @RunID        INT,
    @Status       VARCHAR(20),           -- 'SUCCESS' or 'FAILED'
    @ErrorMessage NVARCHAR(MAX) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dw.etl_run_log
    SET
        EndTime      = SYSUTCDATETIME(),
        Status       = @Status,
        ErrorMessage = @ErrorMessage
    WHERE RunID = @RunID;
END;
GO

/*==============================================================
  PROCEDURE 3: dw.usp_log_etl_step
  Inserts a completed step record into etl_step_log.

  The caller must capture @StartTime = SYSUTCDATETIME()
  BEFORE the step begins and pass it in here. EndTime is
  set to SYSUTCDATETIME() at insert time so the computed
  DurationSec column reflects the actual elapsed time.

  Example:
      DECLARE @StepStart DATETIME2 = SYSUTCDATETIME();
      INSERT INTO dw.fact_sales ...           -- do the work
      DECLARE @Rows INT = @@ROWCOUNT;
      EXEC dw.usp_log_etl_step
          @RunID         = @RunID,
          @StepName      = 'Load fact_sales',
          @StartTime     = @StepStart,
          @RowsProcessed = @Rows,
          @Status        = 'SUCCESS';
==============================================================*/
CREATE OR ALTER PROCEDURE dw.usp_log_etl_step
(
    @RunID         INT,
    @StepName      VARCHAR(200),
    @StartTime     DATETIME2,            -- Captured by caller BEFORE step begins
    @RowsProcessed INT           = NULL,
    @Status        VARCHAR(20),
    @ErrorMessage  NVARCHAR(MAX) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dw.etl_step_log
    (
        RunID,
        StepName,
        StartTime,
        EndTime,
        RowsProcessed,
        Status,
        ErrorMessage
    )
    VALUES
    (
        @RunID,
        @StepName,
        @StartTime,
        SYSUTCDATETIME(),   -- EndTime set at insert; DurationSec is computed from these two
        @RowsProcessed,
        @Status,
        @ErrorMessage
    );
END;
GO

PRINT 'ETL logging framework setup complete.';
PRINT 'Tables  : dw.etl_run_log, dw.etl_step_log';
PRINT 'Procs   : dw.usp_start_etl_run, dw.usp_end_etl_run, dw.usp_log_etl_step';
GO
