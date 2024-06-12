import ast
import sys
import requests
import json
import boto3
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, window, from_json, udf
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, MapType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import NoCredentialsError, ClientError

def get_secret(secret_name, region_name='us-west-2'):

    full_secret_name = 'airflow/variables/' + secret_name

    # Create a Secrets Manager client
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=full_secret_name
        )
    except ClientError as e:
        # Handle exceptions
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # The requested secret was not found
            raise e
    else:
        # Secrets Manager decrypts the secret value using the associated KMS key
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = get_secret_value_response['SecretBinary']

    return secret

def upload_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(f"Upload Successful: {local_file} to {bucket}/{s3_file}")
        return f's3://{bucket}/{s3_file}'
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as e:
        print(f"Client error: {e}")
        return False

def display_logs_from(paginator, run_id, log_group: str, continuation_token):
    """Mutualize iteration over the 2 different log streams glue jobs write to."""
    fetched_logs = []
    next_token = continuation_token
    try:
        for response in paginator.paginate(
                logGroupName=log_group,
                logStreamNames=[run_id],
                PaginationConfig={"StartingToken": continuation_token},
        ):
            fetched_logs.extend(
                [event["message"] for event in response["events"]])
            # if the response is empty there is no nextToken in it
            next_token = response.get("nextToken") or next_token
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            # we land here when the log groups/streams don't exist yet
            print(
                "No new Glue driver logs so far.\n"
                "If this persists, check the CloudWatch dashboard at: %r.",
                f"https://us-west-2.console.aws.amazon.com/cloudwatch/home",
            )
        else:
            raise

    if len(fetched_logs):
        # Add a tab to indent those logs and distinguish them from airflow logs.
        # Log lines returned already contain a newline character at the end.
        messages = "\t".join(fetched_logs)
        print("Glue Job Run %s Logs:\n\t%s", log_group, messages)
    else:
        print("No new log from the Glue Job in %s", log_group)
    return next_token


def check_job_status(glue_client, job_name, job_run_id):
    response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    status = response['JobRun']
    return status


def create_glue_job(
    job_name,
    script_path,
    arguments,
    aws_access_key_id,
    aws_secret_access_key,
    tabular_credential,
    s3_bucket,
    catalog_name,
    aws_region,
    description='Transform CSV data to Parquet format',
    kafka_credentials=None,
    **kwargs
):
    script_path = upload_to_s3(script_path, s3_bucket, 'jobscripts/' + script_path)

    if not script_path:
        raise ValueError('Uploading PySpark script to S3 failed!!')
    # we check to see if you passed in any --conf parameters to override the output table
    output_table = kwargs['dag_run'].conf.get('output_table', arguments.get('--output_table', '')) if 'dag_run' in kwargs else arguments.get('--output_table', '')
    arguments['--output_table'] = output_table

    spark_configurations = [
        f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        f'spark.sql.defaultCatalog={catalog_name}',
        f'spark.sql.catalog.{catalog_name}=org.apache.iceberg.spark.SparkCatalog',
        f'spark.sql.catalog.{catalog_name}.credential={tabular_credential}',
        f'spark.sql.defaultCatalog={catalog_name}',
        f'spark.sql.catalog.{catalog_name}=org.apache.iceberg.spark.SparkCatalog',
        f'spark.sql.catalog.{catalog_name}.catalog-impl=org.apache.iceberg.rest.RESTCatalog',
        f'spark.sql.catalog.{catalog_name}.warehouse={catalog_name}',
        f'spark.sql.catalog.{catalog_name}.uri=https://api.tabular.io/ws/',
        f'spark.sql.shuffle.partitions=50'
    ]
    spark_string = ' --conf '.join(spark_configurations)

    # Adding compatibility with Kafka and Iceberg here
    extra_jars_list = [
        f"s3://{s3_bucket}/jars/iceberg-spark-runtime-3.3_2.12-1.5.2.jar",
        f"s3://{s3_bucket}/jars/iceberg-aws-bundle-1.5.2.jar",
        f"s3://{s3_bucket}/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar"
    ]

    extra_jars = ','.join(extra_jars_list)

    job_args = {
        'Description': description,
        'Role': 'AWSGlueServiceRole',
        'ExecutionProperty': {
            "MaxConcurrentRuns": 3
        },
        'Command': {
            "Name": "glueetl",
            "ScriptLocation": script_path,
            "PythonVersion": "3"
        },
        'DefaultArguments': {
            '--conf': spark_string,
            "--extra-jars": extra_jars
        },
        'GlueVersion': '4.0',
        'WorkerType': 'Standard',
        'NumberOfWorkers': 1
    }

    logs_client = boto3.client('logs',
                               region_name=aws_region,
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)
    glue_client = boto3.client("glue",
                               region_name=aws_region,
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)

    error_continuation_token = None
    output_continuation_token = None
    try:
        # Try to get the existing job
        glue_client.get_job(JobName=job_name)
        print(f"Job '{job_name}' already exists. Updating it.")

        # Update the existing job
        response = glue_client.update_job(JobName=job_name, JobUpdate=job_args)
        print("Job update response:", json.dumps(response, indent=4))

    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Job '{job_name}' does not exist. Creating a new job.")
            response = glue_client.create_job(Name=job_name, **job_args)
            print(response)
        else:
            print(f"Unexpected error: {e}")

    if kafka_credentials is not None:
        arguments['--kafka_credentials'] = kafka_credentials
        arguments['--checkpoint_location'] = f"""s3://{s3_bucket}/kafka-checkpoints/{job_name}"""

    run_response = glue_client.start_job_run(JobName=job_name,
                                             Arguments=arguments)
    log_group_default = "/aws-glue/jobs/output"
    log_group_error = "/aws-glue/jobs/error"
    job_run_id = run_response['JobRunId']
    paginator = logs_client.get_paginator("filter_log_events")
    while True:
        status = check_job_status(glue_client, job_name, job_run_id)
        print(f"Job status: {status['JobRunState']}")
        if status['JobRunState'] in ['SUCCEEDED']:
            break
        elif status['JobRunState'] in ['FAILED', 'STOPPED']:
            raise ValueError('Job has failed or stopped!')

        output_continuation_token = display_logs_from(
            paginator, job_run_id, log_group_default,
            output_continuation_token)
        error_continuation_token = display_logs_from(paginator, job_run_id,
                                                     log_group_error,
                                                     error_continuation_token)
        time.sleep(10)
    return job_name

s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
tabular_credential = get_secret("TABULAR_CREDENTIAL")
catalog_name = get_secret("CATALOG_NAME")  # "eczachly-academy-warehouse"
aws_region = get_secret("AWS_GLUE_REGION")  # "us-west-2"
aws_access_key_id = get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
kafka_credentials = get_secret("KAFKA_CREDENTIALS")

def create_and_run_glue_job(job_name, script_path, arguments):
    glue_job = create_glue_job(
                    job_name=job_name,
                    script_path=script_path,
                    arguments=arguments,
                    aws_region=aws_region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    tabular_credential=tabular_credential,
                    s3_bucket=s3_bucket,
                    catalog_name=catalog_name,
                    kafka_credentials=kafka_credentials
                    )

local_script_path = os.path.join("include", 'eczachly/scripts/kafka_spark_streaming_example.py')
create_and_run_glue_job('saismail_kafka_spark_streaming_example',
                        script_path=local_script_path,
                        arguments={'--ds': '2024-05-23', '--output_table': 'saismail.kafka_spark_streaming_example'})

# TODO PUT YOUR API KEY HERE
GEOCODING_API_KEY = 'q'

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
  host STRING,
  country STRING,
  state STRING,
  city STRING,
  window_start TIMESTAMP,
  event_count BIGINT
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

tumbling_window_df = kafka_df \
    .withColumn("decoded_value", decode_udf(col("value"))) \
    .withColumn("value", from_json(col("decoded_value"), schema)) \
    .withColumn("geodata", geocode_udf(col("value.ip"))) \
    .withWatermark("timestamp", "30 seconds")

by_country = tumbling_window_df.groupBy(window(col("timestamp"), "1 minute"),
                                        col("value.host"),
                                        col("geodata.country"),
                                        col("geodata.city"),
                                        col("geodata.state")
                                        ) \
    .count() \
    .select(
        col("host"),
        col("country"),
        col("state"),
        col("city"),
        col("window.start").alias("window_start"),
        col("count").alias("event_count")
    )

query = by_country \
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
query.awaitTermination(timeout=60*5)