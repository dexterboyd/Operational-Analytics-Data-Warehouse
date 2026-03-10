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