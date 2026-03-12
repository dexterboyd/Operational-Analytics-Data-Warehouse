/*
=========================================================
ETL LOGGING FRAMEWORK SETUP
File: dw_etl_logging_setup.sql

Purpose:
    Establish audit and logging tables for ETL runs.
    Provides visibility into ETL execution times, statuses, and errors.
    Enables step-level tracking for debugging and performance monitoring.

Components:
    1. dw.etl_run_log      → Logs overall ETL run metadata
    2. dw.etl_step_log     → Logs individual ETL steps within a run
    3. dw.usp_log_etl_step → Stored procedure to simplify step logging
=========================================================
*/

---------------------------------------------------------
-- 1. ETL RUN LOG TABLE
-- Purpose: Capture high-level ETL run info
---------------------------------------------------------
CREATE TABLE dw.etl_run_log
(
    RunID INT IDENTITY(1,1) PRIMARY KEY, -- Unique ETL run identifier
    StartTime DATETIME2,                  -- Timestamp when the ETL run started
    EndTime DATETIME2,                    -- Timestamp when the ETL run ended
    Status VARCHAR(20),                   -- 'RUNNING', 'SUCCESS', 'FAILED', etc.
    ErrorMessage NVARCHAR(MAX)            -- Stores error messages if run fails
);

---------------------------------------------------------
-- 2. ETL STEP LOG TABLE
-- Purpose: Capture detailed info for each ETL step
---------------------------------------------------------
CREATE TABLE dw.etl_step_log
(
    StepLogID INT IDENTITY(1,1) PRIMARY KEY, -- Unique step log identifier
    RunID INT,                               -- Foreign key to dw.etl_run_log.RunID
    StepName VARCHAR(100),                    -- Descriptive name of the ETL step
    StartTime DATETIME2,                      -- Step start timestamp
    EndTime DATETIME2,                        -- Step end timestamp
    RowsProcessed INT,                        -- Number of rows processed in this step
    Status VARCHAR(20),                        -- 'RUNNING', 'SUCCESS', 'FAILED', etc.
    ErrorMessage NVARCHAR(MAX)                -- Step-level error message, if any
    -- Optionally: Add foreign key constraint to etl_run_log
    -- CONSTRAINT FK_etl_step_run FOREIGN KEY (RunID) REFERENCES dw.etl_run_log(RunID)
);

---------------------------------------------------------
-- 3. STORED PROCEDURE TO LOG ETL STEPS
-- Purpose: Simplify insertion of step-level logs
---------------------------------------------------------
CREATE OR ALTER PROCEDURE dw.usp_log_etl_step
(
    @RunID INT,                 -- ETL run identifier
    @StepName VARCHAR(100),     -- Name of the ETL step
    @RowsProcessed INT,         -- Number of rows affected
    @Status VARCHAR(20),        -- 'RUNNING', 'SUCCESS', 'FAILED'
    @ErrorMessage NVARCHAR(MAX) = NULL  -- Optional error message
)
AS
BEGIN
    /*
    Inserts a record into dw.etl_step_log.
    StartTime and EndTime are set to current time (GETDATE()).
    Could be enhanced to accept start/end separately for longer-running steps.
    */

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
        GETDATE(),       -- Step start timestamp
        GETDATE(),       -- Step end timestamp
        @RowsProcessed,
        @Status,
        @ErrorMessage
    );
END;

/*
/*
=========================================================
ETL LOGGING FRAMEWORK SETUP
=========================================================
*/

CREATE TABLE dw.etl_run_log
(
    RunID INT IDENTITY(1,1) PRIMARY KEY,
    StartTime DATETIME2,
    EndTime DATETIME2,
    Status VARCHAR(20),
    ErrorMessage NVARCHAR(MAX)
);

CREATE TABLE dw.etl_step_log
(
    StepLogID INT IDENTITY(1,1) PRIMARY KEY,
    RunID INT,
    StepName VARCHAR(100),
    StartTime DATETIME2,
    EndTime DATETIME2,
    RowsProcessed INT,
    Status VARCHAR(20),
    ErrorMessage NVARCHAR(MAX)
);
--------------------------------------------------


CREATE OR ALTER PROCEDURE dw.usp_log_etl_step
(
    @RunID INT,
    @StepName VARCHAR(100),
    @RowsProcessed INT,
    @Status VARCHAR(20),
    @ErrorMessage NVARCHAR(MAX) = NULL
)
AS
BEGIN

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
    GETDATE(),
    GETDATE(),
    @RowsProcessed,
    @Status,
    @ErrorMessage
);

END;
*/