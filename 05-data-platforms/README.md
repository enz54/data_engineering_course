## Data Platforms

### Setup
1- Install Bruin CLI: curl -LsSf https://getbruin.com/install/cli | sh

2- Initialize the zoomcamp template: bruin init zoomcamp my-pipeline

3- Configure your .bruin.yml with a DuckDB connection

### Question 1. Bruin Pipeline Structure

pipeline/ with pipeline.yml and assets

###  Question 2. Materialization Strategies.bruin.yml and 
answer:
`time_interval - incremental based on a time column`

Since the data is organized by month using `pickup_datetime`, the time_interval strategy is the right fit for the staging layer

### Question 3. Pipeline Variables

### Question 4. Running with Dependencies

### Question 5. Quality Checks

This directly checks that the column contains no NULL values.

### Question 6. Lineage and Dependencies

answer: bruin lineage

### Question 7. First-Time Run

answer: --full-refresh

This forces the pipeline to build tables from scratch rather than attempting incremental updates