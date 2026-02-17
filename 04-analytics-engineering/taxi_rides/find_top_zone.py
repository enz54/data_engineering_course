import duckdb

conn = duckdb.connect('taxi_rides.duckdb')

result = conn.execute("""
    SELECT 
        zone,
        SUM(total_revenue) as total_zone_revenue
    FROM prod.fct_monthly_zone_revenue
    WHERE service_type = 'green'
        AND YEAR(pickup_date) = 2020
    GROUP BY zone
    ORDER BY total_zone_revenue DESC
    LIMIT 1
""").fetchall()

if result:
    print(f"Zone with highest revenue for Green taxis in 2020:")
    print(f"  PULocationID {result[0][0]}: ${result[0][1]:,.2f}")
else:
    print("No results found")

conn.close()