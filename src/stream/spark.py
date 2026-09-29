import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import h3

# Initialize spark session
# add Kafka connector package so Spark knows how to talk to Redpanda
spark = SparkSession.builder \
    .appName("RideHailingDemandStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# suppress overly verbose logging
spark.sparkContext.setLogLevel("WARN")

# The schema
json_schema = StructType([
    StructField("vendor_id", IntegerType()),
    StructField("pickup_datetime", TimestampType()),
    StructField("pickup_lat", DoubleType()),
    StructField("pickup_lon", DoubleType()),
    StructField("dropoff_lat", DoubleType()),
    StructField("dropoff_lon", DoubleType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("total_amount", DoubleType())
])

# Define the H3 geospatial UDF
# convert latitude/longitude into a Hexagon ID
def get_h3_index(lat, lon):
    if lat is None or lon is None:
        return None
    try:
        return h3.latlng_to_cell(lat, lon, 8)
    except AttributeError:
        return h3.geo_to_h3(lat, lon, 8)

h3_udf = udf(get_h3_index, StringType())

# read from Redpanda
print("Connecting to Redpanda.")
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "ride_requests") \
    .option("startingOffsets", "latest") \
    .load()

# transform the data
# kafka stores messages as binary bytes. cast value to string, then parse the JSON.
parsed_stream = raw_stream \
    .select(from_json(col("value").cast("string"), json_schema).alias("data")) \
    .select("data.*")

# add the H3 Hexagon ID column
enriched_stream = parsed_stream \
    .withColumn("pickup_h3", h3_udf(col("pickup_lat"), col("pickup_lon")))

# Windowing and Watermarking
# Watermark: Allow data to arrive up to 10 minutes late. Anything later is ignored.
# Window: Group data into 5-minute buckets based on event time (pickup_datetime)
demand_stream = enriched_stream \
    .withWatermark("pickup_datetime", "10 minutes") \
    .groupBy(
        window(col("pickup_datetime"), "5 minutes"),
        col("pickup_h3")
    ) \
    .count() \
    .withColumnRenamed("count", "demand")

# Output to Redis (Serving Layer)
print("Starting streaming query to Redis")

def write_to_redis(df, epoch_id):
    """
    This function processes each micro-batch of data. Using foreachPartition to ensure only open one Redis connection per Spark worker node, rather than one per row .
    """
    def process_partition(partition):
        import redis
        # Connect to the Redis container running on my machine
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # pipeline to send commands in bulk
        pipe = r.pipeline()
        
        for row in partition:
            h3_cell = row['pickup_h3']
            demand = row['demand']
            
            if h3_cell is not None:
                pipe.setex(f"demand:{h3_cell}", 600, demand)
                
        # Execute the bulk insert
        pipe.execute()

    # Apply the partition function to the dataframe
    df.foreachPartition(process_partition)

# Start the stream using the custom foreachBatch function
query = demand_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_redis) \
    .start()

# output to data lake
print("starting streaming query to S3")
datalake_query = enriched_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://datalake/raw_rides/") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/datalake") \
    .start()

# wait for both streams to finish
spark.streams.awaitAnyTermination()