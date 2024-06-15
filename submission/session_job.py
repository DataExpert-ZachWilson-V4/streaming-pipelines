import sys
import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, first, count, window, when
from pyspark.sql.types import StructType, StructField, StringType, MapType, TimestampType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

GEOCODING_API_KEY = 'YOUR_API_KEY'
GEOCODING_API_URL = "https://api.ip2location.io"

def geocode_ip_address(ip_address):
    url = GEOCODING_API_URL
    try:
        response = requests.get(url, params={'ip': ip_address, 'key': GEOCODING_API_KEY})
        response.raise_for_status()
        data = response.json()
        return {
            'country': data.get('country_code', ''),
            'state': data.get('region_name', ''),
            'city': data.get('city_name', '')
        }
    except requests.exceptions.RequestException:
        return {'country': '', 'state': '', 'city': ''}

def create_spark_session():
    return SparkSession.builder.getOrCreate()

def initialize_glue_context(spark, args):
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    return glue_context, job

def load_kafka_config(args):
    kafka_credentials = json.loads(args['kafka_credentials'])
    return {
        "bootstrap_servers": kafka_credentials.get('KAFKA_WEB_BOOTSTRAP_SERVER'),
        "topic": kafka_credentials.get('KAFKA_TOPIC'),
        "key": kafka_credentials.get('KAFKA_WEB_TRAFFIC_KEY'),
        "secret": kafka_credentials.get('KAFKA_WEB_TRAFFIC_SECRET')
    }

def define_schema():
    return StructType([
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

def define_geocode_schema():
    return StructType([
        StructField("country", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
    ])

def create_geocode_udf():
    return udf(geocode_ip_address, define_geocode_schema())

def read_kafka_stream(spark, kafka_config):
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
        .option("subscribe", kafka_config["topic"]) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["key"]}" password="{kafka_config["secret"]}";') \
        .load()

def process_stream(kafka_df, schema, geocode_udf):
    return kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*") \
        .withColumn("os", col("user_agent.os.family")) \
        .withColumn("browser", col("user_agent.family")) \
        .withColumn("geodata", geocode_udf(col("ip"))) \
        .select(
            "*",
            col("geodata.city").alias("city"),
            col("geodata.state").alias("state"),
            col("geodata.country").alias("country")
        )

def aggregate_sessions(event_df, session_id_udf):
    return event_df.withWatermark("event_time", "10 minutes") \
        .groupBy(window(col("event_time"), "5 minutes"), col("user_id"), col("ip")) \
        .agg(
            first("city").alias("city"),
            first("state").alias("state"),
            first("country").alias("country"),
            first("os").alias("os"),
            first("browser").alias("browser"),
            count("*").alias("event_count")
        ) \
        .select(
            session_id_udf(col("user_id"), col("ip"), col("window.start")).alias("session_id"),
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
            when(col("user_id").isNotNull(), True).otherwise(False).alias("is_logged")
        )

def write_to_iceberg(session_df, output_table, checkpoint_location):
    return session_df.writeStream.format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("fanout-enabled", "true") \
        .option("checkpointLocation", checkpoint_location) \
        .toTable(output_table)

def main():
    spark = create_spark_session()
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table', 'kafka_credentials', 'checkpoint_location'])
    glue_context, job = initialize_glue_context(spark, args)

    kafka_config = load_kafka_config(args)
    if not kafka_config["key"] or not kafka_config["secret"]:
        raise ValueError("Kafka key and secret must be provided.")

    schema = define_schema()
    geocode_udf = create_geocode_udf()

    try:
        kafka_df = read_kafka_stream(spark, kafka_config)
        event_df = process_stream(kafka_df, schema, geocode_udf)
        session_df = aggregate_sessions(event_df)
        query = write_to_iceberg(session_df, args['output_table'], args['checkpoint_location'])

        query.awaitTermination(timeout=60*60)

    except Exception as e:
        print(f"Error processing stream: {e}", file=sys.stderr)

    finally:
        job.commit()

if __name__ == "__main__":
    main()