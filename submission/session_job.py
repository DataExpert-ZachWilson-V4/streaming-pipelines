import sys
import ast
import hashlib
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, session_window, from_json, udf, when, min, max, count, first
from pyspark.sql.types import StringType, StructType, StructField, MapType, TimestampType, BooleanType, IntegerType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# API key for geocoding - replace with your actual API key
GEOCODING_API_KEY = 'BB699B7D0F67636D8D437563B0E1CA6C'

def geocode_ip_address(ip_address):
    url = "https://api.ip2location.io"
    try:
        response = requests.get(url, params={'ip': ip_address, 'key': GEOCODING_API_KEY})
        response.raise_for_status()
        data = response.json()
        return {
            'country': data.get('country_code', ''),
            'state': data.get('region_name', ''),
            'city': data.get('city_name', '')
        }
    except requests.exceptions.RequestException as e:
        return {'country': '', 'state': '', 'city': ''}

# Initialize Spark session and Glue context
spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table', 'kafka_credentials', 'checkpoint_location'])
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Kafka configuration extracted from environment variables
kafka_credentials = ast.literal_eval(args['kafka_credentials'])
kafka_key = kafka_credentials.get('KAFKA_WEB_TRAFFIC_KEY')
kafka_secret = kafka_credentials.get('KAFKA_WEB_TRAFFIC_SECRET')
kafka_bootstrap_servers = kafka_credentials.get('KAFKA_WEB_BOOTSTRAP_SERVER')
kafka_topic = kafka_credentials.get('KAFKA_TOPIC')

if not kafka_key or not kafka_secret:
    raise ValueError("Kafka key and secret must be provided.")

# Define the schema of the Kafka message
schema = StructType([
    StructField("url", StringType()),
    StructField("referrer", StringType()),
    StructField("user_agent", StructType([
        StructField("family", StringType()),
        StructField("major", StringType()),
        StructField("minor", StringType()),
        StructField("patch", StringType()),
        StructField("device", StructType([
            StructField("family", StringType()),
            StructField("major", StringType()),
            StructField("minor", StringType()),
            StructField("patch", StringType()),
        ])),
        StructField("os", StructType([
            StructField("family", StringType()),
            StructField("major", StringType()),
            StructField("minor", StringType()),
            StructField("patch", StringType()),
        ]))
    ])),
    StructField("headers", MapType(StringType(), StringType())),
    StructField("host", StringType()),
    StructField("ip", StringType()),
    StructField("user_id", StringType()),
    StructField("event_time", TimestampType())
])

# Reading from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
    .load()

def session_id_from_hash(user_id, ip, window_start):
    """Generate a unique session ID using SHA-256 hashing."""
    unique_string = f"{user_id}-{ip}-{window_start}"
    return hashlib.sha256(unique_string.encode()).hexdigest()

# Register UDFs for session ID generation and geocoding
session_id_udf = udf(session_id_from_hash, StringType())
geocode_udf = udf(geocode_ip_address, MapType(StringType(), StringType()))

# Define the processing logic
session_window_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("event_time", "1 hour") \
    .groupBy(
        session_window("event_time", "5 minutes").alias("session_window"),
        "user_id",
        "ip"
    ) \
    .agg(
        min("event_time").alias("session_start"),
        max("event_time").alias("session_end"),
        count("*").alias("event_count"),
        first("user_agent.device.family").alias("device_family"),
        first("user_agent.family").alias("browser_family"),
        first("ip").alias("ip")
    ) \
    .withColumn("session_id", session_id_udf(col("user_id"), col("ip"), col("session_start"))) \
    .withColumn("is_logged_in", when(col("user_id").isNotNull(), True).otherwise(False)) \
    .withColumn("geocode", geocode_udf(col("ip"))) \
    .withColumn("session_date", col("session_start").cast("date"))

# Rename columns and select required fields
result_df = session_window_df.select(
    col("session_id"),
    col("session_start"),
    col("session_end"),
    col("event_count"),
    col("session_date"),
    col("geocode.city").alias("city"),
    col("geocode.state").alias("state"),
    col("geocode.country").alias("country"),
    col("device_family").alias("operating_system"),
    col("browser_family").alias("browser"),
    col("is_logged_in")
)

# Output the stream to an Iceberg table
query = result_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", args['checkpoint_location']) \
    .toTable(args['output_table'])

# Set timeout for job termination to handle long-running stream processing
query.awaitTermination(timeout=3600)  # Timeout after 1 hour to prevent indefinite running

# End the job
job.commit()
