import ast
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, hash, session_window, to_date, udf
from pyspark.sql.types import (
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import requests
import sys


cache = {}

GEOCODING_API_KEY = ""


def geocode_ip_address(ip_address):
    # Using short intervals so not worrying about becoming out of date
    if ip_address in cache:
        return cache[ip_address]
    url = "https://api.ip2location.io"
    response = requests.get(
        url,
        params={"ip": ip_address, "key": GEOCODING_API_KEY},
    )
    if response.status_code != 200:
        # Return empty dict if request failed
        return {}

    data = json.loads(response.text)
    country = data.get("country_code", "")
    state = data.get("region_name", "")
    city = data.get("city_name", "")
    ip_address_result = {"country": country, "state": state, "city": city}
    cache[ip_address] = ip_address_result
    return ip_address_result


spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "output_table", "kafka_credentials", "checkpoint_location"],
)
output_table = args["output_table"]
checkpoint_location = args["checkpoint_location"]
kafka_credentials = ast.literal_eval(args["kafka_credentials"])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Retrieve Kafka credentials from environment variables
kafka_key = kafka_credentials["KAFKA_WEB_TRAFFIC_KEY"]
kafka_secret = kafka_credentials["KAFKA_WEB_TRAFFIC_SECRET"]
kafka_bootstrap_servers = kafka_credentials["KAFKA_WEB_BOOTSTRAP_SERVER"]
kafka_topic = kafka_credentials["KAFKA_TOPIC"]

if kafka_key is None or kafka_secret is None:
    raise ValueError(
        "KAFKA_WEB_TRAFFIC_KEY and KAFKA_WEB_TRAFFIC_SECRET must be set as environment variables."
    )

# Kafka configuration
# Define the schema of the Kafka message value
schema = StructType(
    [
        StructField("url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField(
            "user_agent",
            StructType(
                [
                    StructField("family", StringType(), True),
                    StructField("major", StringType(), True),
                    StructField("minor", StringType(), True),
                    StructField("patch", StringType(), True),
                    StructField(
                        "device",
                        StructType(
                            [
                                StructField("family", StringType(), True),
                                StructField("major", StringType(), True),
                                StructField("minor", StringType(), True),
                                StructField("patch", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "os",
                        StructType(
                            [
                                StructField("family", StringType(), True),
                                StructField("major", StringType(), True),
                                StructField("minor", StringType(), True),
                                StructField("patch", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField(
            "headers", MapType(keyType=StringType(), valueType=StringType()), True
        ),
        StructField("host", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)
# For now just have ddl in this file.
# Otherwise would need to upload file to s3 & then pass the path so we know where to read it in from here
ddl = f"""
CREATE TABLE IF NOT EXISTS {output_table} (
    session_id BIGINT,
    user_id BIGINT,
    ip STRING,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    event_count BIGINT,
    date DATE,
    city STRING,
    state STRING,
    country STRING,
    os STRING,
    browser STRING,
    is_logged_in BOOLEAN
) USING ICEBERG
PARTITION BY date
"""
spark.sql(ddl)

# Read from Kafka in batch mode
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
    )
    .load()
)


def decode_col(column):
    return column.decode("utf-8")


decode_udf = udf(decode_col, StringType())

geocode_schema = StructType(
    [
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
    ]
)

geocode_udf = udf(geocode_ip_address, geocode_schema)

new_df = (
    kafka_df.withColumn("decoded_value", decode_udf(col("value")))
    .withColumn("value", from_json(col("decoded_value"), schema))
    .withColumn("geodata", geocode_udf(col("value.ip")))
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("is_logged_in", col("value.user_id").isNotNull())
    .withWatermark("timestamp", "30 seconds")
)

by_session = (
    new_df.groupBy(
        session_window(col("timestamp"), "5 minutes"),
        col("value.user_id"),
        col("value.ip"),
        col("date"),
        col("value.user_agent.os.family").alias("os"),
        col("value.user_agent.family").alias("browser"),
        col("geodata.country"),
        col("geodata.city"),
        col("geodata.state"),
        col("is_logged_in"),
    )
    .count()
    .select(
        hash("user_id", "ip", "session_window.start").alias("session_id"),
        col("user_id"),
        col("ip"),
        col("session_window.start").alias("session_start"),
        col("session_window.end").alias("session_end"),
        col("count").alias("event_count"),
        col("date"),
        col("city"),
        col("state"),
        col("country"),
        col("os"),
        col("browser"),
        col("is_logged_in"),
    )
)

query = (
    by_session.writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .option("fanout-enabled", "true")
    .option("checkpointLocation", checkpoint_location)
    .toTable(output_table)
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Stop the job after 60 minutes for this assignment
# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60 * 60)
