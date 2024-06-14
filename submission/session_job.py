import sys
import json
import hashlib
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# API key for geocoding - replace with your actual API key
GEOCODING_API_KEY = '2816D73E11DB8BDF3D45A4B0CC9D0544'

def geocode_ip_address(ip_address):
    """Perform geolocation lookup for the given IP address using an external API with error handling."""
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
kafka_credentials = json.loads(args['kafka_credentials'])
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

geocode_schema = StructType([
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
])

# User-defined functions
def session_id_from_hash(user_id, ip, window_start):
    """Generate a unique session ID using SHA-256 hashing."""
    unique_string = f"{user_id}-{ip}-{window_start}"
    return hashlib.sha256(unique_string.encode()).hexdigest()

session_id_udf = udf(session_id_from_hash, StringType())
geocode_udf = udf(geocode_ip_address, geocode_schema)

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

# Processing logic
event_df = kafka_df.selectExpr("CAST(value AS STRING)")
event_df = event_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
event_df = (event_df
    .withColumn("os", col("user_agent.os.family"))
    .withColumn("browser", col("user_agent.family"))
    .withColumn("geodata", geocode_udf(col("ip"))))

# Extract fields from geodata struct
event_df = event_df.withColumn("city", col("geodata.city")) \
                   .withColumn("state", col("geodata.state")) \
                   .withColumn("country", col("geodata.country"))

# Define session window and group by user_id and ip
session_window = window(col("event_time"), "5 minutes")

session_df = (
    event_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(session_window, col("user_id"), col("ip"))
    .agg(
        first("city").alias("city"),
        first("state").alias("state"),
        first("country").alias("country"),
        first("os").alias("os"),
        first("browser").alias("browser"),
        count("*").alias("event_count"),
    )
    .select(
        session_id_udf(col("user_id"), col("ip"), col("session_window.start")).alias("session_id"),
        col("user_id"),
        col("ip"),
        col("city"),
        col("state"),
        col("country"),
        col("os"),
        col("browser"),
        col("session_window.start").alias("session_start"),
        col("session_window.end").alias("session_end"),
        col("session_window.start").cast("date").alias("session_date"),
        col("event_count"),
        when(col("user_id").isNotNull(), True).otherwise(False).alias("is_logged")
    )
)

# Output the stream to an Iceberg table
query = session_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", args['checkpoint_location']) \
    .toTable(args['output_table'])

# Set timeout for job termination to handle long-running stream processing
query.awaitTermination(timeout=3600)  # Timeout after 1 hour to prevent indefinite running

# End the job
job.commit()