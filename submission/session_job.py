import ast
import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, session_window, from_json, udf, when
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, MapType, DateType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import hashlib

# API key for geocoding - replace with your actual API key
GEOCODING_API_KEY = 'BB699B7D0F67636D8D437563B0E1CA6C'

def geocode_ip_address(ip_address):
    """Perform geolocation lookup for the given IP address using an external API."""
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': GEOCODING_API_KEY
    })

    if response.status_code != 200:
        return {'country': '', 'state': '', 'city': ''}

    data = json.loads(response.text)
    return {
        'country': data.get('country_code', ''),
        'state': data.get('region_name', ''),
        'city': data.get('city_name', '')
    }

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

session_id_udf = udf(session_id_from_hash, StringType())

# Define the processing logic
session_window_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        "data.*",
        session_window(col("event_time"), "5 minutes").alias("session_window")
    ) \
    .groupBy("session_window", "user_id", "ip") \
    .count()

result_df = session_window_df \
    .withColumn("session_id", session_id_udf(col("user_id"), col("ip"), col("session_window.start"))) \
    .select(
        col("session_id"),
        col("session_window.start").alias("window_start"),
        col("session_window.end").alias("window_end"),
        col("user_id"),
        col("ip"),
        col("count").alias("event_count")
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
query.awaitTermination(timeout=60 * 60)  # Timeout after 1 hour to prevent infinite running

# End the job
job.commit()

