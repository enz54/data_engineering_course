## Question 1: Install Spark and PySpark

- Install Spark
    Spark 4.x requires Java 17. Download and unpack.
    Check Java version: `java --version`
    then run `pip install pyspark`
- Run PySpark

        `import pyspark
    
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
                .master("local[*]") \
                .appName('test') \
                .getOrCreate()

        print(f"Spark version: {spark.version}")

        df = spark.range(10)
        df.show()

        spark.stop()`
- Create a local spark session
    To create a local spark session you will need to import SparkSession: `from pyspark.sql import SparkSession`
    Then:

`spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()`

- Execute spark.version.
To get spark version you will need to run `{spark.version}`, once your spark session is built.

What's the output?
`Spark version: 4.0.1`


## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

```
import pandas as pd
import numpy as np
import os
import urllib.request

# Download the file
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-11.parquet"
local_filename = "yellow_tripdata_2024-11.parquet"

if not os.path.exists(local_filename):
    print(f"Downloading {url}...")
    urllib.request.urlretrieve(url, local_filename)
    print("Download complete!")

# Read with pandas
print("Reading parquet file...")
df = pd.read_parquet(local_filename)
print(f"DataFrame shape: {df.shape}")
print(f"Total rows: {len(df):,}")

# Calculate total size for reference
original_size_mb = os.path.getsize(local_filename) / (1024 * 1024)
print(f"Original file size: {original_size_mb:.2f} MB")

# Split into 4 parts - using numpy array_split but converting back to DataFrame
print("\nSplitting into 4 parts...")
# Get the indices to split
split_indices = np.array_split(range(len(df)), 4)

# Create DataFrames from the indices
splits = []
for indices in split_indices:
    splits.append(df.iloc[indices].copy())

# Create output directory
output_dir = "yellow_output_pandas"
os.makedirs(output_dir, exist_ok=True)

# Save each part
print("Saving parts...")
for i, split_df in enumerate(splits):
    output_file = f"{output_dir}/part-{i:04d}.parquet"
    split_df.to_parquet(output_file, compression='snappy')
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Part {i}: {file_size:.2f} MB, {len(split_df):,} rows")

# Calculate average
parquet_files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in parquet_files)
avg_size_mb = total_size / len(parquet_files) / (1024 * 1024)

print(f"\n{'='*50}")
print(f"Total size of all parts: {total_size / (1024 * 1024):.2f} MB")
print(f"Number of files: {len(parquet_files)}")
print(f"Average file size: {avg_size_mb:.2f} MB")
print(f"{'='*50}")

# Compare with options
options = [6, 25, 75, 100]
closest_option = min(options, key=lambda x: abs(x - avg_size_mb))
print(f"\nClosest answer option: {closest_option}MB")

```

### this is the output:

Reading parquet file...
DataFrame shape: (3646369, 19)
Total rows: 3,646,369
Original file size: 57.85 MB

Splitting into 4 parts...
Saving parts...
Part 0: 17.62 MB, 911,593 rows
Part 1: 17.62 MB, 911,592 rows
Part 2: 17.75 MB, 911,592 rows
Part 3: 17.62 MB, 911,592 rows

Total size of all parts: 70.60 MB
Number of files: 4
Average file size: 17.65 MB


Closest answer option: 25MB



## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

```
import pandas as pd
import pyarrow.parquet as pq

# Load the Parquet file directly from the URL
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"

# Read the parquet file
print("Loading data... This may take a moment.")
df = pd.read_parquet(url)

# Display basic info about the dataset
print(f"Total trips in November 2025: {len(df):,}")
print(f"Date range: {df['tpep_pickup_datetime'].min()} to {df['tpep_pickup_datetime'].max()}")

# Convert pickup datetime to date for easier filtering
df['pickup_date'] = df['tpep_pickup_datetime'].dt.date

# Filter for November 15th, 2025
target_date = pd.to_datetime('2025-11-15').date()
nov_15_trips = df[df['pickup_date'] == target_date]

# Count the trips
trip_count = len(nov_15_trips)

print(f"\n📊 Taxi trips on November 15th, 2025: {trip_count:,}")

```
Output is

```
Loading data... This may take a moment.
Total trips in November 2025: 4,181,444
Date range: 2008-12-31 23:04:21 to 2025-11-30 23:59:59

📊 Taxi trips on November 15th, 2025: 162,604

```

The answer is: 

Taxi trips on November 15th, 2025: **162,604**

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

Longest trip duration: **90.6 hours**
```
import pandas as pd

# Load the data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
df = pd.read_parquet(url)

# Calculate trip duration in hours
df['trip_duration_hours'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 3600

# Find the maximum duration
max_duration = df['trip_duration_hours'].max()

# Also check for potential data anomalies (very long trips might be errors)
print(f"Maximum trip duration: {max_duration:.1f} hours")

# Let's look at the top 5 longest trips to verify
print("\nTop 5 longest trips:")
top_5_longest = df.nlargest(5, 'trip_duration_hours')[
    ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_duration_hours']
]
print(top_5_longest)
```
Output is:
```


Top 5 longest trips:
        tpep_pickup_datetime tpep_dropoff_datetime  trip_duration_hours
2844155  2025-11-26 20:22:12   2025-11-30 15:01:00            90.646667
2862191  2025-11-27 04:22:41   2025-11-30 09:19:35            76.948333
242536   2025-11-03 10:42:55   2025-11-06 14:55:45            76.213889
701241   2025-11-07 11:23:22   2025-11-10 08:40:41            69.288611
1958490  2025-11-18 17:12:47   2025-11-21 12:17:37            67.080556

```
Answer is:  **90.6 hours**


## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

answer : **4040**



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, asc
import urllib.request
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Taxi Zone Analysis") \
    .getOrCreate()

# Function to download files using urllib (built-in)
def download_file(url, filename):
    if not os.path.exists(filename):
        print(f"Downloading {filename}...")
        urllib.request.urlretrieve(url, filename)
        print(f"Downloaded {filename}")
    else:
        print(f"{filename} already exists")

# 1. DOWNLOAD the files
print("Starting downloads...")
download_file("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet", 
              "yellow_tripdata_2025-11.parquet")
download_file("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", 
              "taxi_zone_lookup.csv")

print("\nLoading files into Spark...")

# 2. Load the downloaded files
trip_df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
zone_df = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

# 3. Create temporary views
trip_df.createOrReplaceTempView("trips")
zone_df.createOrReplaceTempView("zones")

# 4. Find least frequent pickup locations
print("\n" + "="*60)
print("LEAST FREQUENT PICKUP LOCATIONS")
print("="*60)

# Check specifically for the four options with their LocationIDs
print("\n📊 PICKUP COUNTS FOR THE FOUR OPTIONS:")
print("-"*40)

options = [
    ('Rikers Island', 199),
    ("Governor's Island/Ellis Island/Liberty Island", 103),
    ('Arden Heights', 5),
    ('Jamaica Bay', 2)
]

for zone_name, location_id in options:
    # Count pickups for this specific LocationID
    result = spark.sql(f"""
        SELECT COUNT(*) as pickup_count
        FROM trips
        WHERE PULocationID = {location_id}
    """).collect()[0]
    
    count_value = result[0]
    print(f"{zone_name:50} : {count_value:8,} pickups")
```

### output:
```Starting downloads...
yellow_tripdata_2025-11.parquet already exists
taxi_zone_lookup.csv already exists

Loading files into Spark...

============================================================
LEAST FREQUENT PICKUP LOCATIONS
============================================================

📊 PICKUP COUNTS FOR THE FOUR OPTIONS:
----------------------------------------
Rikers Island                                       :        4 pickups
Governor's Island/Ellis Island/Liberty Island      b:        0 pickups ✔
Arden Heights                                       :        1 pickups
Jamaica Bay                                         :        5 pickups
```

**Answer**: Governor's Island/Ellis Island/Liberty Island with **0 pickups**
