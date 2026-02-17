import duckdb
import os
import glob

# Connect to DuckDB database
conn = duckdb.connect('taxi_rides.duckdb')

# Create schema if it doesn't exist
conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

# Load GREEN taxi data (2019-2020)
print("Loading green taxi data (2019-2020)...")
green_files = sorted(glob.glob(os.path.join('data/green', '*.parquet')))
green_files_2019_2020 = [f for f in green_files if '2019' in f or '2020' in f]

if green_files_2019_2020:
    pattern = 'data/green/green_tripdata_*.parquet'
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.green_tripdata AS
        SELECT * FROM read_parquet('{pattern}')
    """)
    result = conn.execute("SELECT COUNT(*) FROM raw.green_tripdata").fetchall()
    print(f"✓ Green taxi data loaded: {result[0][0]} rows")

# Load YELLOW taxi data (2019-2020)
print("Loading yellow taxi data (2019-2020)...")
yellow_files = sorted(glob.glob(os.path.join('data/yellow', '*.parquet')))
yellow_files_2019_2020 = [f for f in yellow_files if '2019' in f or '2020' in f]

if yellow_files_2019_2020:
    pattern = 'data/yellow/yellow_tripdata_*.parquet'
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.yellow_tripdata AS
        SELECT * FROM read_parquet('{pattern}')
    """)
    result = conn.execute("SELECT COUNT(*) FROM raw.yellow_tripdata").fetchall()
    print(f"✓ Yellow taxi data loaded: {result[0][0]} rows")

# Load FHV taxi data (2019-2020)
print("Loading FHV taxi data (2019-2020)...")
fhv_files = sorted(glob.glob(os.path.join('data/fhv_parquet', '*.parquet')))
fhv_files_2019_2020 = [f for f in fhv_files if '2019' in f or '2020' in f]

if fhv_files_2019_2020:
    print(f"Found {len(fhv_files_2019_2020)} FHV files")
    
    # Load all FHV files
    pattern = 'data/fhv_parquet/fhv_tripdata_*.parquet'
    
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.fhv_tripdata AS
        SELECT * FROM read_parquet('{pattern}')
    """)
    result = conn.execute("SELECT COUNT(*) FROM raw.fhv_tripdata").fetchall()
    print(f"✓ FHV data loaded: {result[0][0]} rows")
else:
    print("✗ No FHV parquet files found in data/fhv_parquet")

conn.close()
print("\n✓ All data loaded successfully!")

# import duckdb
# import os
# import glob

# conn = duckdb.connect('taxi_rides.duckdb')
# conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

# # Load FHV taxi data (2019 ONLY)
# print("Loading FHV taxi data (2019)...")
# fhv_files = sorted(glob.glob(os.path.join('data/fhv_parquet', '*.parquet')))
# fhv_files_2019 = [f for f in fhv_files if '2019' in f]

# if fhv_files_2019:
#     print(f"Found {len(fhv_files_2019)} FHV files for 2019")
    
#     pattern = 'data/fhv_parquet/fhv_tripdata_2019-*.parquet'
    
#     conn.execute(f"""
#         CREATE OR REPLACE TABLE raw.fhv_tripdata AS
#         SELECT * FROM read_parquet('{pattern}')
#     """)
#     result = conn.execute("SELECT COUNT(*) FROM raw.fhv_tripdata").fetchall()
#     print(f"✓ FHV data loaded: {result[0][0]} rows")
# else:
#     print("✗ No FHV parquet files found for 2019")

# conn.close()
# print("✓ Done!")