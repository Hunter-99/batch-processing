import os

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import max, unix_timestamp, to_date


def run():
    # Initialize spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

    tripdata_schema = types.StructType([
        types.StructField('dispatching_base_num', types.StringType(), True), 
        types.StructField('pickup_datetime', types.TimestampType(), True), 
        types.StructField('dropoff_datetime', types.TimestampType(), True), 
        types.StructField('PULocationID', types.IntegerType(), True), 
        types.StructField('DOLocationID', types.IntegerType(), True), 
        types.StructField('SR_Flag', types.StringType(), True), 
        types.StructField('Affiliated_base_number', types.StringType(), True)
    ])

    # Read csv file
    tripdata_df = spark.read \
        .option("header", "true") \
        .schema(tripdata_schema) \
        .csv('fhvhv_tripdata/fhvhv_tripdata_2021-06.csv.gz')
        
    # Repartition csv file into 12 partitions
    partitions_dir = 'fhvhv_tripdata/2021/06/'

    tripdata_df = tripdata_df.repartition(12)
    tripdata_df.write.parquet(
        path=partitions_dir,
        mode='ignore'
    )

    # Get files size in MD
    for file in os.listdir(partitions_dir):
        if file.startswith('part'):
            file_stats = os.stat(partitions_dir + file)
            print(f'Csv partition file size in MegaBytes is {file_stats.st_size / (1024 * 1024)}')

    # Read parquet files

    tripdata_df = spark.read \
        .schema(tripdata_schema) \
        .parquet(partitions_dir)
    
    # Trips count in fhvhv_tripdata_2021-06 dataset
    print(tripdata_df.filter(to_date(tripdata_df.pickup_datetime) == '2021-06-15').count())
    
    # Select the longest trip in Hours
    tripdata_df \
        .select(max((unix_timestamp(tripdata_df.dropoff_datetime) - unix_timestamp(tripdata_df.pickup_datetime))/3600)) \
        .show(truncate=False)

    # Select the name of the most frequent pickup location zone

    taxi_zones_df = spark.read \
        .option("header", "true") \
        .csv('fhvhv_tripdata/taxi_zone_lookup.csv')


    final_df = tripdata_df.join(
        taxi_zones_df,
        tripdata_df.PULocationID == taxi_zones_df.LocationID,
        'inner'
    )

    final_df \
        .select(final_df.PULocationID, final_df.Zone) \
        .groupby(final_df.PULocationID, final_df.Zone) \
        .count() \
        .orderBy(['count'], ascending = [False]) \
        .show(truncate=False)


if __name__ == '__main__':
    run()
