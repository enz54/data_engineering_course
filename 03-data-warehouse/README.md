# Data Warehouse Module
### BigQuery Setup

#### Create an external table using the Yellow Taxi Trip Records.

DROP TABLE IF EXISTS `mysite-257001`.`zoomcamp_db`.`yellow_tripdata`;
CREATE OR REPLACE EXTERNAL TABLE `mysite-257001`.`zoomcamp_db`.`yellow_tripdata`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://zoomcamp-db/yellow_tripdata_2024-*.parquet']);


#### Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). 
CREATE OR REPLACE TABLE `mysite-257001`.`zoomcamp_db`.`yellow_tripdata_internal`
AS
SELECT * FROM `mysite-257001.zoomcamp_db.yellow_tripdata`;


### Q1

select count(*) FROM `zoomcamp_db.yellow_tripdata`




### Q4

SELECT COUNT(*) as zero_fare_count
FROM `mysite-257001.zoomcamp_db.yellow_tripdata`
WHERE fare_amount = 0;

### Q6
  
SELECT DISTINCT VendorID
  FROM `mysite-257001.zoomcamp_db.yellow_tripdata`  -- Materialized
  WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';