# Analytics Module

## Local Setup Guide
1- create `taxi_rides_ny` folder where all dbt commands will run from.

2- Install DuckDB: ` pip install duckdb`.

3- Install dbt: `pip install dbt-duckdb`.

4- Update `~/.dbt/profiles.yml` with this code:
taxi_rides:
  target: dev
  outputs:

    # DuckDB Development profile
    dev:
      type: duckdb
      path: taxi_rides.duckdb
      schema: dev
      threads: 1
      extensions:
        - parquet
      settings:
        memory_limit: '2GB'
        preserve_insertion_order: false

    # DuckDB Production profile
    prod:
      type: duckdb
      path: taxi_rides.duckdb
      schema: prod
      threads: 1
      extensions:
        - parquet
      settings:
        memory_limit: '2GB'
        preserve_insertion_order: false

## Adding data to green and yellow taxi
To load the data you will need to download it then load the data, running the following scripts:

Run [`python ingestion.py`](https://github.com/enz54/data_engineering_course/blob/main/04-analytics-engineering/taxi_rides/ingestion.py) then
[`python load_data.py `](https://github.com/enz54/data_engineering_course/blob/main/04-analytics-engineering/taxi_rides/load_data.py)

## Question 1. dbt run --select int_trips_unioned builds which models?
int_trips_unioned - dbt runs only that specific model, not its dependencies or downstream models.

## Question 2. New value 6 appears in payment_type. What happens on dbt test?
dbt fails the test with non-zero exit code
## Question 3. Count of records in fct_monthly_zone_revenue?
run [`python count_fct_records.py`](https://github.com/enz54/data_engineering_course/blob/main/04-analytics-engineering/taxi_rides/count_fct_records.py)
## Question 4. Zone with highest revenue for Green taxis in 2020?
Run [`python find_top_zone.py`](https://github.com/enz54/data_engineering_course/blob/main/04-analytics-engineering/taxi_rides/find_top_zone.py)
PULocationID:74 corresponds to: East Harlem North
The answer is East Harlem North
## Question 5. Total trips for Green taxis in October 2019?
run the following script [`python green_trips_oct_2019.py`](https://github.com/enz54/data_engineering_course/blob/main/04-analytics-engineering/taxi_rides/green_trips_oct_2019.py)
## Question 6. Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)?
run [`python count_fhv_staging.py`](https://github.com/enz54/data_engineering_course/blob/main/04-analytics-engineering/taxi_rides/count_fhv_staging.py)
 