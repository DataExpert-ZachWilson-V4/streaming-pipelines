import hashlib
import ast
import sys
import requests
import json
from typing import Dict
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    session_window,
    from_json,
    udf,
    when
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    TimestampType,
    StructType,
    StructField,
    MapType,
    DateType
)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# TODO PUT YOUR API KEY HERE
GEOCODING_API_KEY = 'EC8EB4709BC34444B4D81CA011C70741'

# Function to geocode IP address using external API
def geocode_ip_address(ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={'ip': ip_address, 'key': GEOCODING_API_KEY})

    if response.status_code == 200:
        data = json.loads(response.text)
        country = data.get('country_code', '')
        state = data.get('region_name', '')
        city = data.get('city_name', '')

        return {'country': country, 'state': state, 'city': city}
    else:
        # Return empty dict if request failed
        return {}


# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "ds",
    "output_table",
    "kafka_credentials",
    "checkpoint_location"
])
run_date = args['ds']
output_table = args['output_table']
checkpoint_location = args['checkpoint_location']
kafka_credentials = ast.literal_eval(args['kafka_credentials'])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Retrieve Kafka credentials
kafka_key = kafka_credentials['KAFKA_WEB_TRAFFIC_KEY']
kafka_secret = kafka_credentials['KAFKA_WEB_TRAFFIC_SECRET']
kafka_bootstrap_servers = kafka_credentials['KAFKA_WEB_BOOTSTRAP_SERVER']
kafka_topic = kafka_credentials['KAFKA_TOPIC']

if kafka_key is None or kafka_secret is None:
    raise ValueError("KAFKA_WEB_TRAFFIC_KEY and KAFKA_WEB_TRAFFIC_SECRET must be set as environment variables.")

# Kafka configuration
start_timestamp = f"{run_date}T00:00:00.000Z"

# Define the schema for Kafka message values
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

# Create table if not exists
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {output_table} (
        session_id VARCHAR,
        user_id VARCHAR,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP NOT NULL,
        event_count INTEGER NOT NULL,
        start_date DATE NOT NULL,
        city VARCHAR,
        state VARCHAR,
        country VARCHAR,
        operating_system VARCHAR,
        browser VARCHAR,
        is_logged_in BOOLEAN NOT NULL
    )
    USING ICEBERG
    PARTITIONED BY (start_date)
    """
)

# Read from Kafka in streaming mode
kafka_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'
    )
    .load()
)


# Auxiliary Functions

# Decode UTF-8
def decode_col(column):
    return column.decode('utf-8')

decode_udf = udf(decode_col, StringType())

# Generate and hash unique session string
def generate_session_string(ip: str, user_agent: Dict[str, Dict[str, str]], session_start: datetime) -> str:
    session_params = {
        "ip": ip,
        "user_agent": (
            user_agent['family'] + user_agent['major'] + user_agent['minor'] + user_agent['patch'] +
            user_agent['device']['family'] + user_agent['device']['major'] + user_agent['device']['minor'] + user_agent['device']['patch'] +
            user_agent['os']['family'] + user_agent['os']['major'] + user_agent['os']['minor'] + user_agent['os']['patch']
        ),
        "session_start": str(session_start)
    }
    return f"{ip}{user_agent}{session_start}".format(**session_params)

# Hash the session string
def hash_session_string(session_string: str) -> str:
    return hashlib.sha256(session_string.encode()).hexdigest()

# Generate unique session ID
def generate_session_id(ip: str, user_agent: Dict[str, Dict[str, str]], session_start: datetime) -> str:
    session_string = generate_session_string(ip, user_agent, session_start)
    session_id = hash_session_string(session_string)
    return session_id

generate_session_id_udf = udf(generate_session_id, StringType())

# Define UDFs and schema for geocoding
geocode_schema = StructType([
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
])

geocode_udf = udf(geocode_ip_address, geocode_schema)

# Process Kafka messages and aggregate into sessions
tumbling_window_df = kafka_df \
    .withColumn("decoded_value", decode_udf(col("value"))) \
    .withColumn("value", from_json(col("decoded_value"), schema)) \
    .withColumn("geodata", geocode_udf(col("value.ip"))) \
    .withWatermark("timestamp", "30 seconds")

session_aggregate_df = tumbling_window_df \
    .groupBy(
        session_window(col("timestamp"), "5 minutes"),
        "value.user_id",
        "value.user_agent",
        "value.ip",
        "geodata.country",
        "geodata.city",
        "geodata.state"
    ) \
    .count() \
    .select(
        generate_session_id_udf(col("ip"), col("user_agent"), col("session_window.start")).alias("session_id"),
        col("value.user_id").alias("user_id"),
        col("session_window.start").alias("start_time"),
        col("session_window.end").alias("end_time"),
        col("count").alias("event_count"),
        col("session_window.start").cast(DateType()).alias("start_date"),
        col("geodata.country").alias("country"),
        col("geodata.city").alias("city"),
        col("geodata.state").alias("state"),
        col("value.user_agent.os.family").alias("operating_system"),
        col("value.user_agent.family").alias("browser"),
        when(col("value.user_id").isNull(), lit(False)).otherwise(lit(True)).alias("is_logged_in")
    )

# Write the aggregated sessions to the Iceberg table
write_query = session_aggregate_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(output_table)

# Initialize and start the job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Stop the job after 1 hour
write_query.awaitTermination(timeout=60*60)
