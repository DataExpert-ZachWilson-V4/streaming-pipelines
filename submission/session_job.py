'''
Write a Spark Streaming Job (session_job.py) that reads from Kafka

It groups the Kafka events with a session_window with a 5-minute gap 
(sessions end after 5 minutes of inactivity)
It is also grouped by user_id and ip to track each user behavior
Make sure to increase the timeout of your job to 1 hour so you can capture real sessions
Create a unique identifier for each session called session_id 
(you should think about using hash and the necessary columns to uniquely identify the session)
If user_id is not null, that means the event is from a logged in user
If a user logs in, that creates a new session 
(if you group by ip and user_id that solves the problem pretty elegantly)
'''

import ast
import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, lit, window, concat_ws, min, max, count, first, when
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

GEOCODING_API_KEY = '0E9084485B96B207E7A7915614381EE2'

def geocode_ip_address(ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': GEOCODING_API_KEY
    })

    # Return empty dict if request failed
    if response.status_code != 200:
        return {}

    data = json.loads(response.text)

    # Extract the country, state, and city from the response
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')

    return {'country': country, 'state': state, 'city': city}

spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME",
                                     "ds",
                                     'output_table',
                                     'kafka_credentials',
                                     'checkpoint_location'
                                     ])
run_date = args['ds']
output_table = args['output_table']
checkpoint_location = args['checkpoint_location']
kafka_credentials = ast.literal_eval(args['kafka_credentials'])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Retrieve Kafka credentials from environment variables
kafka_key = kafka_credentials['KAFKA_WEB_TRAFFIC_KEY']
kafka_secret = kafka_credentials['KAFKA_WEB_TRAFFIC_SECRET']
kafka_bootstrap_servers = kafka_credentials['KAFKA_WEB_BOOTSTRAP_SERVER']
kafka_topic = kafka_credentials['KAFKA_TOPIC']

if kafka_key is None or kafka_secret is None:
    raise ValueError("KAFKA_WEB_TRAFFIC_KEY and KAFKA_WEB_TRAFFIC_SECRET must be set as environment variables.")

# Kafka configuration
start_timestamp = f"{run_date}T00:00:00.000Z"

# Define the schema of the Kafka message value
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
        response = requests.get("https://api.ip2location.io", params={'ip': ip, 'key': GEOCODING_API_KEY})
        if response.status_code != 200:
            return {'country': 'Unknown', 'state': 'Unknown', 'city': 'Unknown'}
        data = json.loads(response.text)
        return {'country': data.get('country_code', ''), 'state': data.get('region_name', ''), 'city': data.get('city_name', '')}
    except:
        return {'country': 'Unknown', 'state': 'Unknown', 'city': 'Unknown'}

# UDF for geocoding IP addresses
geocode_udf = udf(geocode_ip, StructType([
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True)
]))

# Read Kafka stream
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

# Parse the value column as JSON
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Enhance data with geocoded location information
enhanced_df = parsed_df.withColumn("location", geocode_udf(col("ip")))

# Flatten the location struct into individual columns
cleaned_df = enhanced_df.select(
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
session_df = cleaned_df \
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
    .withColumn("status_logged_in", when(col("user_id").isNotNull(), lit(True)).otherwise(lit(False)))

# Create the table if it does not exist
spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} (
    session_id VARCHAR PRIMARY KEY,
    user_id VARCHAR, 
    session_start TIMESTAMP, 
    session_end TIMESTAMP, 
    event_count INTEGER,
    session_date DATE,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    operating_system VARCHAR,
    browser VARCHAR,
    status_logged_in BOOLEAN
)
USING ICEBERG
""")

# Write to Iceberg table
query = session_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(output_table)

# Initialize Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 1 hour timeout
query.awaitTermination(timeout=60*60)