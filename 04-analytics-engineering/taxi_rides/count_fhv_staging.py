import duckdb

conn = duckdb.connect('taxi_rides.duckdb')

result = conn.execute("""
    SELECT COUNT(*) as record_count
    FROM prod.stg_fhv_tripdata
""").fetchall()

print(f"Records in stg_fhv_tripdata (2019, non-NULL dispatching_base_num): {result[0][0]}")

conn.close()
