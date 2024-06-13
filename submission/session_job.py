import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

GEOCODING_API_KEY = '967505975B539579DF26FD558C07C740'

def geocode_ip_address(ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': GEOCODING_API_KEY
    })

    if response.status_code != 200:
        return {}

    data = response.json()

    country = data.get("country_code", "")
    state = data.get("region_name", "")
    city = data.get("city_name", "")

    return {
        "country": country,
        "state": state,
        "city": city
    }
    

spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(sys.argv, ["JOB_NAME",
                                     "ds",
                                     "output_table",
                                     "kafka_credentials",
                                     "checkpoint_location"
                                    ])
run_date = args.get("ds")
output_table = args.get("output_table")
checkpoint_location = args.get("checkpoint_location")
kafka_credentials = json.loads(args.get("kafka_credentials"))
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session


kafka_key = kafka_credentials.get('KAFKA_WEB_TRAFFIC_KEY')
kafka_secret = kafka_credentials.get('KAFKA_WEB_TRAFFIC_SECRET')
kafka_bootstrap_servers = kafka_credentials.get('KAFKA_WEB_BOOTSTRAP_SERVER')
kafka_topic = kafka_credentials.get('KAFKA_TOPIC')

if kafka_key is None or kafka_secret is None:
    raise ValueError("KAFKA_WEB_TRAFFIC_KEY and KAFKA_WEB_TRAFFIC_SECRET must be set as environment variables.")



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

geocode_schema = StructType([
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
])


# Read from Kafka in batch mode
kafka_df = (spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 5000) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
    .load()
)


geocode_udf = udf(geocode_ip_address, geocode_schema)

event_df = kafka_df.select(col("value").cast("string"))
event_df = event_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
event_df = (event_df
    .withColumn("os", col("user_agent.os.family"))
    .withColumn("browser", col("user_agent.family"))
    .withColumn("geodata", geocode_udf(col("ip")))
    
)

event_df = event_df.withColumn("city", col("geodata.city")) \
                   .withColumn("state", col("geodata.state")) \
                   .withColumn("country", col("geodata.country"))


session_df = (
    event_df
    .withWatermark("event_time", "10 minutes")
    .groupBy( window(col("event_time"), "5 minutes"),
             col("user_id"),
             col("ip"),
             col('country'),
             col('city'),
             col('state'),
             col('browser'),
             col('os'))
        .count()
        .select(
            hash(col("user_id"), col("ip"), col("window.start"), col("window.end")).cast("string").alias("session_id"),
            col("user_id"),
            col("ip"),
            col("city"),
            col("state"),
            col("country"),
            col("os"),
            col("browser"),
            col("window.start").alias("session_start"),
            col("window.end").alias("session_end"),
            col("window.start").cast("date").alias("session_date"),
            col("count").alias('event_count'),
            (when(col("user_id").isNotNull(), True).otherwise(False)).alias("logged_in")

        )

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

# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60*5)