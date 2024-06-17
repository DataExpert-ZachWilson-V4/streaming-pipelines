import ast
import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    window,
    from_json,
    udf,
    count,
    coalesce,
    first,
    unix_timestamp,
    hash,
)
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    MapType,
    TimestampType,
)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


GEOCODING_API_URL = "https://api.ip2location.io"
GEOCODING_API_KEY = "A8C3133C7840E661A0D9320482A7002C"


# Function to geocode an IP address using IP2Location API
def geocode_ip_address(ip_address: str) -> dict:
    # Send GET request to the geocoding API with the IP address and API key as parameters
    response = requests.get(
        GEOCODING_API_URL, params={"ip": ip_address, "key": GEOCODING_API_KEY}
    )

    # Check if the response status code is not 200 (indicating an error)
    if response.status_code != 200:
        return {}  # Return an empty dictionary if there was an error

    # Parse the response JSON data
    data = json.loads(response.text)

    # Extract the country, state, and city information from the response data
    country = data.get("country_code", "")
    state = data.get("region_name", "")
    city = data.get("city_name", "")

    # Return a dictionary containing the geolocation information
    return {"country": country, "state": state, "city": city}


spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "ds", "output_table", "kafka_credentials", "checkpoint_location"],
)
run_date = args["ds"]
output_table = args["output_table"]
checkpoint_location = args["checkpoint_location"]

# Parse the Kafka credentials JSON string into a dictionary
kafka_credentials = ast.literal_eval(args["kafka_credentials"])
glue_context = GlueContext(spark.sparkContext)
spark = glue_context.spark_session

# Extract the Kafka credentials from the dictionary
kafka_key = kafka_credentials["KAFKA_WEB_TRAFFIC_KEY"]
kafka_secret = kafka_credentials["KAFKA_WEB_TRAFFIC_SECRET"]
kafka_bootstrap_servers = kafka_credentials["KAFKA_WEB_BOOTSTRAP_SERVER"]
kafka_topic = kafka_credentials["KAFKA_TOPIC"]

# Check if the Kafka credentials are not set
if kafka_key is None or kafka_secret is None:
    raise ValueError(
        "KAFKA_WEB_TRAFFIC_KEY and KAFKA_WEB_TRAFFIC_SECRET must be set as environment variables."
        "Please make sure to set these variables before running the job."
    )

start_timestamp = f"{run_date}T00:00:00.000Z"


schema = StructType(
    [
        StructField("user_id", StringType(), True),
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
        StructField("event_time", TimestampType(), True),
    ]
)

geocode_schema = StructType(
    [
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
    ]
)


spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS {output_table} (
            session_id VARCHAR,
            session_start TIMESTAMP,
            session_end TIMESTAMP,                  
            event_count BIGINT,          
            session_start_date DATE,            
            city VARCHAR,        
            state VARCHAR,                    
            country VARCHAR,                  
            os VARCHAR,
            browser VARCHAR,
            user_id VARCHAR,             
            is_logged_in BOOLEAN
)
USING ICEBERG
PARTITIONED BY (session_start_date)
TBLPROPERTIES ('write.format.default'='parquet')
"""
)


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


geocode_udf = udf(geocode_ip_address, geocode_schema)

tumbling_window_df = (
    kafka_df.withColumn("decoded_value", decode_udf(col("value")))
    .withColumn("value", from_json(col("decoded_value"), schema))
    .withColumn("geodata", geocode_udf(col("value.ip")))
    .withWatermark("timestamp", "2 minutes")
)


session_df = (
    (
        tumbling_window_df.groupBy(
            window(col("timestamp"), "5 minutes"), col("value.user_id"), col("value.ip")
        )
    )
    .agg(
        first("geodata.city").alias("city"),
        first("geodata.country").alias("country"),
        first("geodata.state").alias("state"),
        first("value.user_agent.os.family").alias("os"),
        first("value.user_agent.family").alias("browser"),
        count("*").alias("event_count"),
    )
    .select(
        hash(
            col("ip"),
            unix_timestamp("window.start"),
            coalesce(col("user_id"), lit("-")),
        )
        .cast(StringType())
        .alias("session_id"),
        col("window.start").alias("session_start"),
        col("window.end").alias("session_end"),
        col("event_count"),
        col("window.start").cast("date").alias("session_start_date"),
        col("city"),
        col("country"),
        col("state"),
        col("os"),
        col("browser"),
        hash(coalesce(col("user_id"), lit("no_user_id"))).alias("user_id"),
        col("user_id").isNotNull().alias("is_logged"),
    )
)


query = (
    session_df.writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .option("fanout-enabled", "true")
    .option("checkpointLocation", checkpoint_location)
    .toTable(output_table)
)

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# stop the job after 5 minutes
# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60 * 60)
