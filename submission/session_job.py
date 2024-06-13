from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, min, max, udf, lit, concat, sha2
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sessionization Job") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_bootstrap_servers") \
    .option("subscribe", "kafka_topic") \
    .load()

# Define schema and parse the Kafka value
schema = (
    StringType()  # This should be defined according to your Kafka message schema
)

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Create a session_id using hash of user_id, ip, session_start
def generate_session_id(user_id, ip, session_start):
    session_id = sha2(concat(user_id, ip, session_start.cast(StringType())), 256)
    return session_id

generate_session_id_udf = udf(generate_session_id, StringType())

# Add session_id column
parsed_df = parsed_df.withColumn(
    "session_id", generate_session_id_udf(col("user_id"), col("ip"), col("session_start"))
)

# Add user_status column
parsed_df = parsed_df.withColumn(
    "user_status", when(col("user_id").isNull(), lit("logged_out")).otherwise(lit("logged_in"))
)

# Sessionization
sessionized_df = parsed_df \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes").alias("session_window"),
        col("user_id"), col("ip")
    ) \
    .agg(
        count("*").alias("event_count"),
        min("event_time").alias("session_start"),
        max("event_time").alias("session_end"),
        first("city").alias("city"),
        first("state").alias("state"),
        first("country").alias("country"),
        first("operating_system").alias("operating_system"),
        first("browser").alias("browser")
    ) \
    .withColumn("session_id", generate_session_id_udf(col("user_id"), col("ip"), col("session_start"))) \
    .withColumn("session_date", col("session_start").cast("date"))

# Write to Iceberg
query = sessionized_df.writeStream \
    .outputMode("append") \
    .format("iceberg") \
    .option("path", "hadoop_catalog") \
    .option("table", "jrsarrat.streaming_hw_week5") \
    .start()

# Increase the timeout to 1 hour
query.awaitTermination(timeout=3600)
