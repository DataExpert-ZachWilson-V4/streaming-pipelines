import ast
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, udf, when, concat_ws, lit, min, max, count, first
from pyspark.sql.types import StringType, TimestampType, StructType, StructField
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.streaming import StreamingQueryException

# Initialize Spark and Glue contexts
spark = SparkSession.builder \
    .appName("DataExpertSessionJob") \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Parse job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", "output_table", "kafka_credentials", "checkpoint_location"])
output_table = args['output_table']
checkpoint_location = args['checkpoint_location']
kafka_credentials = ast.literal_eval(args['kafka_credentials'])

# Extract Kafka credentials
kafka_key = kafka_credentials['KAFKA_WEB_TRAFFIC_KEY']
kafka_secret = kafka_credentials['KAFKA_WEB_TRAFFIC_SECRET']
kafka_bootstrap_servers = kafka_credentials['KAFKA_WEB_BOOTSTRAP_SERVER']
kafka_topic = kafka_credentials['KAFKA_TOPIC']

# Define schema for Kafka messages
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("operating_system", StringType(), True),
    StructField("browser", StringType(), True)
])

# Function to geocode IP address
def geocode_ip(ip):
    try:
        # Replace with actual geocoding API call
        response = requests.get(f"https://api.ipgeolocation.io/ipgeo?apiKey=YOUR_API_KEY&ip={ip}")
        data = response.json()
        return (data['city'], data['state_prov'], data['country_name'])
    except:
        return ("Unknown", "Unknown", "Unknown")

# UDF for geocoding IP addresses
geocode_udf = udf(geocode_ip, StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True)
]))

# Read Kafka stream
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
    .load()

# Parse the value column as JSON
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Enrich data with geocoded location information
enriched_df = parsed_df.withColumn("location", geocode_udf(col("ip")))

# Flatten the location struct into individual columns
final_df = enriched_df.select(
    col("user_id"),
    col("ip"),
    col("event_time"),
    col("operating_system"),
    col("browser"),
    col("location.city").alias("city"),
    col("location.state").alias("state"),
    col("location.country").alias("country")
)

# Add a session window and group by user_id and ip
session_df = final_df \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "5 minutes").alias("session_window"), col("user_id"), col("ip")) \
    .agg(
        col("user_id"),
        col("ip"),
        min("event_time").alias("session_start"),
        max("event_time").alias("session_end"),
        count("event_time").alias("event_count"),
        first("city").alias("city"),
        first("state").alias("state"),
        first("country").alias("country"),
        first("operating_system").alias("operating_system"),
        first("browser").alias("browser")
    ) \
    .withColumn("session_id", concat_ws("_", col("user_id"), col("ip"), col("session_start"))) \
    .withColumn("session_date", col("session_start").cast("date")) \
    .withColumn("is_logged_in", when(col("user_id").isNotNull(), lit(True)).otherwise(lit(False)))

# Write to Iceberg table
query = session_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .start(output_table)

# Initialize Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

query.awaitTermination(timeout=60*5)  # Set timeout to 1 hour

