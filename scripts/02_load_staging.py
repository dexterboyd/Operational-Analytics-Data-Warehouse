"""
load_staging.py
===============
Loads CSV source files into staging tables in Fedex_Ops_Database.
Records row counts and timestamps into staging.load_log after
each successful load for use by the validation script.

Run Order:
    1. etl_staging_setup.sql   -- build schemas and tables
    2. THIS SCRIPT             -- load CSV data
    3. etl_staging_validation.sql -- validate staging data

Usage:
    python load_staging.py

Requirements:
    pip install pandas pyodbc sqlalchemy
"""

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

# ============================================================
# CONNECTION
# ============================================================

connection_string = (
    f'mssql+pyodbc://{SERVER}/{DATABASE}'
    f'?driver={DRIVER}&trusted_connection=yes'
)

engine = create_engine(connection_string)

# ============================================================
# LOAD
# All four tables and their load_log entries are written
# inside a single transaction. If any table fails, all
# four loads and all log entries are rolled back so staging
# is never left in a partial state.
# ============================================================

print(f'Starting load -- {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print(f'Server:   {SERVER}')
print(f'Database: {DATABASE}')
print('-' * 50)

with engine.begin() as conn:
    for table, path in FILE_PATHS.items():

        # Read CSV
        df = pd.read_csv(path)
        row_count = len(df)

        # Load into staging table (replace = drop and recreate each run)
        df.to_sql(
            table,
            conn,
            schema='staging',
            if_exists='replace',
            index=False
        )

        # Record load in staging.load_log
        conn.execute(text("""
            INSERT INTO staging.load_log (TableName, RowsLoaded, LoadedAt)
            VALUES (:table, :rows, GETDATE())
        """), {'table': f'staging.{table}', 'rows': row_count})

        print(f'Loaded {row_count:,} rows into staging.{table}')

print('-' * 50)
print(f'Done -- {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print('Run 03_etl_staging_validation to validate the load.')
