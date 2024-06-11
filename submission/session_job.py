import ast
import sys
import requests
import json
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, window, from_json, udf, count
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, MapType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# TODO PUT YOUR API KEY HERE
GEOCODING_API_KEY = 'C88166B4ADF1FDE7D40F9628FBAB5D91'

def generate_session_id(user_id, ip, start_time):
    # Create a unique session ID based on user_id, ip and start_time
    data = f"{user_id}_{ip}_{start_time}"
    return hashlib.md5(data.encode()).hexdigest()

generate_session_id_udf = udf(generate_session_id, StringType())

def geocode_ip_address(ip_address):
    url = "https://api.ip2location.io"
    try:
        response = requests.get(url, params={
            'ip': ip_address,
            'key': GEOCODING_API_KEY
        })
        response.raise_for_status()
    except requests.RequestException as e:
        # Log the error and return empty dict
        print(f"Error geocoding IP address {ip_address}: {e}")
        return {}

    data = response.json()
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')

    return {'country': country, 'state': state, 'city': city}

spark = (SparkSession.builder
         .getOrCreate())
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
    StructField("operating_system", StructType([
        StructField("family", StringType(), True),
        StructField("major", StringType(), True),
        StructField("minor", StringType(), True),
        StructField("patch", StringType(), True),
    ]), True),
    StructField("browser", StructType([
        StructField("family", StringType(), True),
        StructField("major", StringType(), True),
        StructField("minor", StringType(), True),
        StructField("patch", StringType(), True),
    ]), True)
])


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {output_table} (
    session_id STRING,
    user_id STRING,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    event_count INTEGER,
    session_date DATE,
    city STRING,
    state STRING,
    country STRING,
    operating_system STRING,
    browser STRING,
    is_logged_in BOOLEAN
)
USING ICEBERG
""")

# Read from Kafka in batch mode
kafka_df = (spark \
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
)

def decode_col(column):
    return column.decode('utf-8')

decode_udf = udf(decode_col, StringType())

geocode_schema = StructType([
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
])

geocode_udf = udf(geocode_ip_address, geocode_schema)

tumbling_window_df = kafka_df \
    .withColumn("decoded_value", decode_udf(col("value"))) \
    .withColumn("value", from_json(col("decoded_value"), schema)) \
    .withColumn("geodata", geocode_udf(col("value.ip"))) \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "5 minute"), col("value.user_id"), col("value.ip")) \
    .agg(
        col("value.user_id").alias("user_id"),
        col("value.ip").alias("ip"),
        col("geodata.city").alias("city"),
        col("geodata.state").alias("state"),
        col("geodata.country").alias("country"),
        col("value.operating_system.family").alias("operating_system"),
        col("value.browser.family").alias("browser"),
        lit(True).alias("is_logged_in"),
        count("*").alias("event_count"),
        col(window(col("event_time"), "5 minute").start, "yyyy-MM-dd").alias("session_date"),
        col("window.start").alias("session_start"),
        col("window.end").alias("session_end")
    )

tumbling_window_df = tumbling_window_df.withColumn("session_id", generate_session_id_udf(col("user_id"), col("ip"), col("session_start")))

query = tumbling_window_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(output_table)


job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# stop the job after 5 minutes
# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60*60)
# hw prompt mentioned to change it for an hour


