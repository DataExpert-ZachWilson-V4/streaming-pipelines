from submission.aws_secret_manager import get_secret
from submission.glue_job import create_glue_job
import os

s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
tabular_credential = get_secret("TABULAR_CREDENTIAL")
catalog_name = get_secret("CATALOG_NAME")  # "eczachly-academy-warehouse"
aws_region = get_secret("AWS_GLUE_REGION")  # "us-west-2"
aws_access_key_id = get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
kafka_credentials = get_secret("KAFKA_CREDENTIALS")

def create_and_run_glue_job(job_name, script_path, arguments):
    create_glue_job(
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



local_script_path = 'submission/session_job.py'
create_and_run_glue_job('siawayforward_streaming_pipeline_hw',
                        script_path=local_script_path,
                        arguments={'--ds': '2024-06-16', '--output_table': 'siawayforward.data_expert_sessions'})
