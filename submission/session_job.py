import ast
import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, length, col, lit, window, from_json, udf, count, md5, concat_ws, to_date, to_timestamp, max as sp_max, min as sp_min
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, MapType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# TODO PUT YOUR API KEY HERE
GEOCODING_API_KEY = 'C68DD23F5456E1D8123B336CFCE9479A'

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
         .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
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
    StructField("user_id", StringType(), True),
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
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  event_count INTEGER,
  session_start_date DATE,
  user_city STRING,
  user_state STRING,
  user_country STRING,
  user_os STRING,
  user_browser STRING,
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

# define what logged in means to be logged in
df = kafka_df \
    .withColumn("decoded_value", decode_udf(col("value"))) \
    .withColumn("value", from_json(col("decoded_value"), schema)) \
    .withColumn("geodata", geocode_udf(col("value.ip"))) \
    .withColumn("value.event_time", to_timestamp(col("value.event_time"))) \
    .withWatermark("timestamp", "5 minutes")

session = df.groupBy(window(col("timestamp"), "5 minutes"),
    col("value.url"),
    col("value.ip"),
    col("value.user_id"),
    col("value.host"),
    # attributes
    col("geodata.city").alias("user_city"),
    col("geodata.state").alias("user_state"),
    col("geodata.country").alias("user_country"),
    col("value.user_agent.os.family").alias("user_os"),
    col("value.user_agent.family").alias("user_browser"),
    col("value.referrer").contains("/login").alias("is_logged_in")
    ) \
    .agg(
        to_timestamp(sp_min(col("value.event_time"))).alias("start_time"),
        to_timestamp(sp_max(col("value.event_time"))).alias("end_time"),
        count("*").alias("event_count")
    ) \
    .filter(length(concat_ws('', col("ip"), col("user_id"), col("url"), col("host"))) > lit(0)) \
    .select(
        md5(
            concat_ws('-', col("ip"), col("user_id"), col("url"), col("host"), col("start_time"), col("end_time"))
            ).alias("session_id"),
        col("start_time"),
        col("end_time"),
        coalesce(col("event_count"), lit(0)).alias("event_count"),
        to_date(col("start_time")).alias("session_start_date"),
        col("user_city"),
        col("user_state"),
        col("user_country"),
        col("user_os"),
        col("user_browser"),
        col("is_logged_in")
    ) \
    .withColumn("event_count", col("event_count").cast("integer")) \
    #.withWatermark("start_time", "5 minutes") \
    

query = session \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("fanout-enabled", "false") \
    .option("truncate", "true") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(output_table)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# stop the job after 5 minutes
# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60*5)

