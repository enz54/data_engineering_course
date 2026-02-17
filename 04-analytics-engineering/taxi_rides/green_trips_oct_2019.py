import duckdb

conn = duckdb.connect('taxi_rides.duckdb')

# Check October 2019 details
result = conn.execute("""
    SELECT 
        pickup_date,
        zone,
        trip_count,
        total_revenue
    FROM prod.fct_monthly_zone_revenue
    WHERE service_type = 'green'
        AND pickup_date >= '2019-10-01'
        AND pickup_date < '2019-11-01'
    ORDER BY pickup_date, zone
    LIMIT 20
""").fetchall()

print("October 2019 Green taxi data sample:")
for row in result:
    print(row)

conn.close()