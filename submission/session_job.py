import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

GEOCODING_API_KEY = 'removed for privacy'

def geocode_ip_address(ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': GEOCODING_API_KEY
    })

    if response.status_code != 200:
        return {}
    
    data = response.json()

    # Extract the country and state from the response
    # This might change depending on the actual response structure
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

# Retrieve Kafka credentials from environment variables
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

# with open('submission/session_ddl.sql', 'r') as f:
#     query = f.read()
# query = query.format(output_table=output_table)

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS {output_table} (
    session_id STRING NOT NULL,
    user_id STRING,
    ip STRING,
    city STRING,
    state STRING,
    country STRING,
    os STRING,
    browser STRING,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_date DATE,
    event_count BIGINT,
    is_logged BOOLEAN
)
USING ICEBERG
PARTITIONED BY (session_date)
    """.format(output_table=output_table)
)

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


geocode_udf = udf(geocode_ip_address, geocode_schema)

event_df = kafka_df.selectExpr("CAST(value AS STRING)")
event_df = event_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
event_df = (event_df
    .withColumn("os", col("user_agent.os.family"))
    .withColumn("browser", col("user_agent.family"))
    .withColumn("geodata", geocode_udf(col("ip")))
    # .withColumn("user_id", when(col("headers").isNotNull() & (size(col("headers")) > 0), col("headers")["user-id"]).otherwise(None))
)

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
            col("event_count"),
            (when(col("user_id").isNotNull(), True).otherwise(False)).alias("is_logged")

        )

)

# session_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

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