import ast
import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, window, from_json, udf, count,aggregate, coalesce,first,unix_timestamp,hash
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# TODO PUT YOUR API KEY HERE
GEOCODING_API_KEY = '355BC8A7EA8C83CF545058859366B94F'

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
    StructField("user_id", StringType(), True),
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
            session_id STRING,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            event_count BIGINT,
            session_begin_date DATE,
            city STRING,
            country STRING,
            state STRING,
            os STRING,
            browser STRING,
            user_id BIGINT,
            is_logged BOOLEAN
)
USING ICEBERG
PARTITIONED BY (session_begin_date)
TBLPROPERTIES ('write.format.default'='parquet')
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
    .withWatermark("timestamp", "2 minutes")


session_df = (tumbling_window_df.groupBy(window(col("timestamp"), "5 minutes"),
                       col("value.user_id"),
                       col("value.ip"))) \
                .agg(
                        first("geodata.city").alias("city"),
                        first("geodata.country").alias("country"),
                        first("geodata.state").alias("state"),
                        first("value.user_agent.os.family").alias("os"),
                        first("value.user_agent.family").alias("browser"),
                        count("*").alias("event_count")) \
                .select(
                        #hash((coalesce(col("user_id"), lit('no_user_id')), unix_timestamp("window.start"), col("ip"))).cast(StringType()).alias("session_id"),
                        #The above line is erroring out complaining multiple inputs to hash but the below worked with same set of columns but different ordering
                        hash(col("ip"), unix_timestamp("window.start"), coalesce(col("user_id"), lit('-'))).cast(StringType()).alias("session_id"),
                        col("window.start").alias("session_start"),
                        col("window.end").alias("session_end"),
                        col("event_count"),
                        col("window.start").cast("date").alias("session_begin_date"),
                        col("city"),
                        col("country"),
                        col("state"),
                        col("os"),
                        col("browser"),
                        hash(coalesce(col("user_id"), lit('no_user_id'))).alias("user_id"),
                        col("user_id").isNotNull().alias("is_logged")
                        )


query = session_df \
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
