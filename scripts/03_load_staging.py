"""
load_staging.py
===============
Loads CSV source files into staging tables in Fedex_Ops_Database.

Enhancements:
- Clears staging.load_log each run to prevent uncontrolled growth
- Truncates staging tables before load to avoid duplicates
- Validates CSV file existence
- Provides better error handling
- Uses a single database transaction for full pipeline integrity

Run Order:
    1. etl_staging_setup_v5.sql
    2. load_staging.py
    3. staging_layer_validation_v2.sql
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys

# ============================================================
# CONFIGURATION
# ============================================================

SERVER   = r'DESKTOP-CC8DKBU\SQLEXPRESS'
DATABASE = 'Fedex_Ops_Database'
DRIVER   = 'ODBC+Driver+17+for+SQL+Server'

FILE_PATHS = {
    'staging_sales':       r'C:\ETL_DATA\sales.csv',
    'staging_deliveries':  r'C:\ETL_DATA\deliveries.csv',
    'staging_routes':      r'C:\ETL_DATA\routes.csv',
    'staging_exceptions':  r'C:\ETL_DATA\exceptions.csv'
}

# ============================================================
# DATABASE CONNECTION
# ============================================================

connection_string = (
    f'mssql+pyodbc://{SERVER}/{DATABASE}'
    f'?driver={DRIVER}&trusted_connection=yes'
)

engine = create_engine(connection_string, fast_executemany=True)

# ============================================================
# ETL LOAD PROCESS
# ============================================================

print(f"\nStarting staging load: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Server:   {SERVER}")
print(f"Database: {DATABASE}")
print("-" * 60)

try:

    with engine.begin() as conn:

        # ----------------------------------------------------
        # Clear Load Log (prevents unlimited growth)
        # ----------------------------------------------------
        print("Clearing staging.load_log...")
        conn.execute(text("TRUNCATE TABLE staging.load_log"))

        # ----------------------------------------------------
        # Loop through files and load
        # ----------------------------------------------------
        for table, path in FILE_PATHS.items():

            print(f"\nProcessing {table}...")

            # Validate file exists
            if not os.path.exists(path):
                raise FileNotFoundError(f"CSV file not found: {path}")

            # Read CSV
            df = pd.read_csv(path)

            # Clean column names (remove spaces)
            df.columns = df.columns.str.strip()

            row_count = len(df)

            if row_count == 0:
                raise ValueError(f"{table} CSV file contains zero rows.")

            # -----------------------------------------------
            # Clear staging table before load
            # -----------------------------------------------
            conn.execute(text(f"TRUNCATE TABLE staging.{table}"))

            # -----------------------------------------------
            # Load data
            # -----------------------------------------------
            df.to_sql(
                table,
                conn,
                schema="staging",
                if_exists="append",
                index=False
            )

            # -----------------------------------------------
            # Record load log
            # -----------------------------------------------
            conn.execute(text("""
                INSERT INTO staging.load_log (TableName, RowsLoaded, LoadedAt)
                VALUES (:table, :rows, GETDATE())
            """), {
                "table": f"staging.{table}",
                "rows": row_count
            })

            print(f"Loaded {row_count:,} rows into staging.{table}")

    print("\n" + "-" * 60)
    print("STAGING LOAD COMPLETE")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Next Step: Run staging_layer_validation_v2.sql")

except Exception as e:

    print("\n" + "-" * 60)
    print("ETL LOAD FAILED")
    print(f"Error: {e}")
    print("No tables were partially loaded (transaction rolled back).")
    print("-" * 60)

    sys.exit(1)