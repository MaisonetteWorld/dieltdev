from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('s3_bucket_dag', default_args=default_args, schedule_interval=None) as dag:
    
    # Define the AWS connection ID
    aws_conn_id = 'airbyte_glue_data_flattening'
    
    # Create an S3 bucket
    create_bucket = S3CreateBucketOperator(
        task_id='create_bucket',
        bucket_name='my-s3-bucket',
        region_name='us-west-2',
        aws_conn_id=aws_conn_id
    )
    
    # Delete the S3 bucket
    delete_bucket = S3DeleteBucketOperator(
        task_id='delete_bucket',
        bucket_name='my-s3-bucket',
        force_delete=True,
        aws_conn_id=aws_conn_id
    )
    
    create_bucket >> delete_bucket
