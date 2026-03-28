"""
load_staging.py
===============
Loads CSV source files into staging tables in Fedex_Ops_Database.
Records row counts into staging.load_log and writes pipeline
audit entries to staging.pipeline_log via the logging procedures.

Run Order:
    1. etl_staging_setup_v5.sql        -- build schemas and tables
    2. etl_pipeline_logging_v1.sql     -- build logging objects
    3. THIS SCRIPT                     -- load CSV data (Step 2)
    4. staging_layer_validation_v2.sql -- validate staging data

Usage:
    python load_staging.py [--run-id <int>]

    --run-id  Optional. Pass a RunID to group this load with other
              pipeline steps in the same run. If omitted, a new
              RunID is generated automatically from the current
              MAX(RunID) + 1 in pipeline_log.

Requirements:
    pip install pandas pyodbc sqlalchemy
"""

import sys
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# ============================================================
# CONFIGURATION
# Update SERVER and FILE_PATHS if your environment changes.
# ============================================================

SERVER   = r'DESKTOP-CC8DKBU\SQLEXPRESS'
DATABASE = 'Fedex_Ops_Database'
DRIVER   = 'ODBC+Driver+17+for+SQL+Server'

FILE_PATHS = {
    'staging_sales':       r'C:\ETL_DATA\sales.csv',
    'staging_deliveries':  r'C:\ETL_DATA\deliveries.csv',
    'staging_routes':      r'C:\ETL_DATA\routes.csv',
    'staging_exceptions':  r'C:\ETL_DATA\exceptions.csv',
}

# Pipeline step metadata
STEP_ORDER = 2
STEP_NAME  = 'staging_load'
STEP_DESC  = 'Load CSV source files into staging tables via pandas. Records row counts in staging.load_log.'

# ============================================================
# CONNECTION
# ============================================================

connection_string = (
    f'mssql+pyodbc://{SERVER}/{DATABASE}'
    f'?driver={DRIVER}&trusted_connection=yes'
)

engine = create_engine(connection_string)

# ============================================================
# RESOLVE RUN ID
# Accept --run-id from command line or generate a new one.
# ============================================================

def get_run_id(conn):
    """Return the next RunID by incrementing the current MAX."""
    result = conn.execute(text(
        "SELECT ISNULL(MAX(RunID), 0) + 1 FROM staging.pipeline_log"
    ))
    return result.scalar()

run_id = None
if '--run-id' in sys.argv:
    idx = sys.argv.index('--run-id')
    run_id = int(sys.argv[idx + 1])

# ============================================================
# LOAD
# All four CSV loads and their load_log entries run inside
# a single transaction. If any table fails, all four loads
# and all log entries roll back so staging is never left
# in a partial state.
# The pipeline_log RUNNING row is written before the
# transaction opens so it is always visible even if the
# transaction rolls back.
# ============================================================

print(f'Starting load -- {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print(f'Server:   {SERVER}')
print(f'Database: {DATABASE}')
print('-' * 50)

with engine.begin() as conn:

    # Resolve RunID inside the connection
    if run_id is None:
        run_id = get_run_id(conn)

    print(f'RunID:    {run_id}')
    print('-' * 50)

    # Log step start
    conn.execute(text("""
        EXEC staging.usp_log_step_start
            @RunID      = :run_id,
            @StepOrder  = :step_order,
            @StepName   = :step_name,
            @StepDesc   = :step_desc
    """), {
        'run_id':     run_id,
        'step_order': STEP_ORDER,
        'step_name':  STEP_NAME,
        'step_desc':  STEP_DESC
    })

    try:
        total_rows = 0

        for table, path in FILE_PATHS.items():

            # Read CSV
            df = pd.read_csv(path)
            row_count = len(df)
            total_rows += row_count

            # Load into staging table
            # if_exists='replace' drops and recreates the table
            # each run so it is always a clean load
            df.to_sql(
                table,
                conn,
                schema='staging',
                if_exists='replace',
                index=False
            )

            # Record in staging.load_log for validation script
            conn.execute(text("""
                INSERT INTO staging.load_log (TableName, RowsLoaded, LoadedAt)
                VALUES (:table, :rows, GETDATE())
            """), {'table': f'staging.{table}', 'rows': row_count})

            print(f'Loaded {row_count:,} rows into staging.{table}')

        # Log step success with total rows across all four tables
        conn.execute(text("""
            EXEC staging.usp_log_step_end
                @RunID        = :run_id,
                @StepName     = :step_name,
                @RowsAffected = :rows
        """), {
            'run_id':    run_id,
            'step_name': STEP_NAME,
            'rows':      total_rows
        })

    except Exception as e:
        # Log step failure before re-raising so the audit trail
        # captures the error even though the transaction will roll back
        error_msg = str(e)[:2000]
        try:
            conn.execute(text("""
                EXEC staging.usp_log_step_error
                    @RunID    = :run_id,
                    @StepName = :step_name,
                    @ErrorMsg = :error_msg
            """), {
                'run_id':    run_id,
                'step_name': STEP_NAME,
                'error_msg': error_msg
            })
        except Exception:
            pass  # Do not mask the original error
        raise

print('-' * 50)
print(f'Done -- {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print(f'RunID {run_id} -- staging_load PASSED')
print('Run staging_layer_validation_v2.sql to validate the load.')
print(f'Pass --run-id {run_id} to subsequent pipeline steps to group this run.')
