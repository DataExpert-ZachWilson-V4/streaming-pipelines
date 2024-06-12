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

# API key for geocoding
GEOCODING_API_KEY = '68621EF53E61DA8A34AC5EB931E85872'

# Function to get geolocation details from an IP address
def fetch_geolocation(ip):
    api_url = "https://api.ip2location.io"
    response = requests.get(api_url, params={
        'ip': ip,
        'key': GEOCODING_API_KEY
    })

    if response.status_code != 200:
        return {}

    data = json.loads(response.text)
    return {
        'country_code': data.get('country_code', ''),
        'state_name': data.get('region_name', ''),
        'city_name': data.get('city_name', '')
    }

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table', 'kafka_credentials', 'checkpoint_location'])
run_date = args['ds']
output_table = args['output_table']
checkpoint_location = args['checkpoint_location']
kafka_credentials = ast.literal_eval(args['kafka_credentials'])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Kafka credentials and configurations
kafka_key = kafka_credentials['KAFKA_WEB_TRAFFIC_KEY']
kafka_secret = kafka_credentials['KAFKA_WEB_TRAFFIC_SECRET']
kafka_bootstrap_servers = kafka_credentials['KAFKA_WEB_BOOTSTRAP_SERVER']
kafka_topic = kafka_credentials['KAFKA_TOPIC']

if kafka_key is None or kafka_secret is None:
    raise ValueError("Kafka credentials must be set.")

# Define schema for Kafka message values
schema = StructType([
    StructField("url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("user_agent", StructType([
        StructField("family", StringType(), True),
        StructField("major", StringType(), True),
        StructField("minor", StringType(), True),
        StructField("patch", StringType(), True),
        StructField("device", StructType([
            StructField("family", StringType(), True),
            StructField("major", StringType(), True),
            StructField("minor", StringType(), True),
            StructField("patch", StringType(), True),
        ]), True),
        StructField("os", StructType([
            StructField("family", StringType(), True),
            StructField("major", StringType(), True),
            StructField("minor", StringType(), True),
            StructField("patch", StringType(), True),
        ]), True)
    ]), True),
    StructField("headers", MapType(StringType(), StringType()), True),
    StructField("host", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

# Read data from Kafka in streaming mode
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";')
    .load()
)

# Function to decode Kafka message values
decode_message = udf(lambda col: col.decode('utf-8'), StringType())
geocode_udf = udf(fetch_geolocation, StructType([
    StructField("country_code", StringType(), True),
    StructField("state_name", StringType(), True),
    StructField("city_name", StringType(), True),
]))

# Function to create a unique session ID
def generate_session_id(user_id, ip, window_start):
    return hash(f"{user_id}{ip}{window_start}")

session_id_udf = udf(generate_session_id, IntegerType())

# Process streaming data with a session window
session_data_df = kafka_df.withColumn("decoded_value", decode_message(col("value"))) \
    .withColumn("value", from_json(col("decoded_value"), schema)) \
    .withColumn("geo_info", geocode_udf(col("value.ip"))) \
    .withWatermark("timestamp", "30 seconds")

session_grouped_df = session_data_df.groupBy(
    session_window(col("timestamp"), "5 minutes"),
    col("value.user_id"),
    col("value.ip"),
    col("geo_info.country_code"),
    col("geo_info.city_name"),
    col("geo_info.state_name"),
    col("value.user_agent")
).count().select(
    session_id_udf(col("user_id"), col("ip"), col("session_window.start")).alias("session_id"),
    col("session_window.start").alias("session_start"),
    col("session_window.end").alias("session_end"),
    col("count").alias("event_count"),
    col("session_window.start").cast(DateType()).alias("start_date"),
    col("country_code"),
    col("state_name"),
    col("city_name"),
    col("user_agent.os.family").alias("operating_system"),
    col("user_agent.family").alias("browser_type"),
    when(col("user_id").isNotNull(), "Logged In").otherwise("Logged Out").alias("login_status")
)

# Write stream to the output table
query = session_grouped_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(output_table)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Set a timeout to stop the job after one hour
query.awaitTermination(timeout=60 * 60)
