from typing import Dict
from datetime import datetime
import hashlib
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

# TODO PUT YOUR API KEY HERE
GEOCODING_API_KEY = '63A35031401E9E38B5A24BDD1A494552'

def geocode_ip_address(ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': GEOCODING_API_KEY
    })

    if response.status_code != 200:
        # Return empty dict if request failed
        return {}

    data = json.loads(response.text)

    # Extract the country and state from the response
    # This might change depending on the actual response structure
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
    StructField("headers", MapType(keyType=StringType(), valueType=StringType()), True),
    StructField("host", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

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

# %% Auxiliary Functions
# Decode UTF-8
def decode_col(column):
    return column.decode('utf-8')

decode_udf = udf(decode_col, StringType())

# Generate and hash unique session string
def generate_session_string(
        ip: str,
        user_agent: Dict[str, Dict[str, str]],
        session_start: datetime
    ) -> str:
    """Generates a unique session string based on the IP address, user agent, and session start time.
    """
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
    """Return the SHA-256 hash of the session string.
    """
    return hashlib.sha256(session_string.encode()).hexdigest()

# Generate session ID
def generate_session_id(
        ip: str,
        user_agent: Dict[str, Dict[str, str]],
        session_start: datetime
    ) -> str:
    """Generates a unique session id based on a SHA-256 hashed combination of the
    IP address, user agent, and session start time.
    Args:
        ip (str): IP address at session start (assuming it is static for the duration of the session)
        user_agent (Dict[str, Dict[str, str]]): User agent at session start
        session_start (datetime): Session start timestamp
    Returns:
        str: Unique session id
    """
    session_string = generate_session_string(ip, user_agent, session_start)
    session_id = hash_session_string(session_string)
    return session_id

generate_session_id_udf = udf(generate_session_id, StringType())

# %% Define UDFs and schema for geocoding
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
    .withWatermark("timestamp", "30 seconds")

session_aggregate_df = tumbling_window_df\
    .groupBy(
        session_window(col("timestamp"), "5 minutes"),
        "value.user_id",
        "value.user_agent",
        "value.ip",
        "geodata.country",
        "geodata.city",
        "geodata.state"
    )\
    .count()\
    .select(
        generate_session_id_udf(col("ip"), col("user_agent"), col("session_window.start")).alias("session_id"),
        when(col("user_id").isNull(), 0).otherwise(1).alias("is_logged_user"),
        col("session_window.start").cast(DateType()).alias("session_start_date"),
        col("session_window.start").alias("session_start_ts"),
        col("session_window.end").alias("session_end_ts"),
        col("count").alias("event_count"),
        col("country"),
        col("city"),
        col("state"),
        col("user_agent.os.family").alias("os"),
        col("user_agent.family").alias("browser")
    )

# Write query
write_query = session_aggregate_df\
    .writeStream\
    .format("iceberg")\
    .outputMode("append")\
    .trigger(processingTime="5 seconds")\
    .option("fanout-enabled", "true")\
    .option("checkpointLocation", checkpoint_location)\
    .toTable(output_table)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# stop the job after 5 minutes
# PLEASE DO NOT REMOVE TIMEOUT
write_query.awaitTermination(timeout=60*60)