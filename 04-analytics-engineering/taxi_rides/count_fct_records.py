import duckdb

conn = duckdb.connect('taxi_rides.duckdb')

result = conn.execute("""
    SELECT COUNT(*) as record_count
    FROM prod.fct_monthly_zone_revenue
""").fetchall()

print(f"Total records in fct_monthly_zone_revenue: {result[0][0]}")

conn.close()