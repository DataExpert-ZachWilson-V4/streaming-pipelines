import ast
import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, session_window, from_json, udf
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, MapType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Removed the API key for security reasons.
GEOCODING_API_KEY = ''

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
    StructField("event_time", TimestampType(), True)
])

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {output_table} (
    id STRING,
    country STRING,
    state STRING,
    city STRING,
    operating_system STRING,
    browser STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    n_events INTEGER,
    logged_in BOOLEAN
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

decoded_df = kafka_df \
    .withColumn("decoded_value", decode_udf(col("value"))) \
    .withColumn("value", from_json(col("decoded_value"), schema)) \
    .withColumn("geodata", geocode_udf(col("value.ip"))) \
    .withWatermark("timestamp", "30 seconds")


selected_columns = decoded_df.select(
        col("value.ip").alias("id"), # Didn't concat user_id because it wasn't in the data. I assumed it should arrive in the headers field, but all records had the headers field empty.
        col("geodata.country").alias("country"),
        col("geodata.state").alias("state"),
        col("geodata.city").alias("city"),
        col("value.user_agent.os.family").alias("operating_system"),
        col("value.user_agent.family").alias("browser"),
        col("timestamp")
    )


session_window_df = selected_columns.groupBy(
                                        col("id"),
                                        col("country"),
                                        col("state"),
                                        col("city"),
                                        col("operating_system"),
                                        col("browser"),
                                        session_window(col("timestamp"), "5 minute")
                                        ) \
                                        .count() \
                                        .select(
                                            col("id"),
                                            col("country"),
                                            col("state"),
                                            col("city"),
                                            col("operating_system"),
                                            col("browser"),
                                            col("session_window.start").alias("start_time"),
                                            col("session_window.end").alias("end_time"),
                                            col("count").cast(IntegerType()).alias("n_events")
                                        )
# Hardcoded "logged_in" value because couldn't find user_id in data.
query = session_window_df.withColumn("logged_in", lit(False)) \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(output_table)

# Change overwrite to append
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# stop the job after 60 minutes
# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60*60)