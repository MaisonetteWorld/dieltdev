import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sensors import S3KeySensor
from datetime import datetime, timedelta

s3 = boto3.client('s3')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'read_s3_files',
    default_args=default_args,
    description='Read files from an S3 bucket',
    schedule_interval=None
)

def read_s3_folder():
    bucket_name = 'maisonette-airbyte-integration-landing-dev'
    folder_name = 'dummyfolder1/'
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    if 'Contents' not in response:
        print(f"No files found in the {folder_name} folder")
        return
    
    for file in response['Contents']:
        file_obj = s3.get_object(Bucket=bucket_name, Key=file['Key'])
        file_content = file_obj['Body'].read().decode('utf-8')
        print(file_content)

check_folder = S3KeySensor(
    task_id='check_folder',
    bucket_key='your-bucket-name/your-folder-name/',
    wildcard_match=True,
    aws_conn_id='aws_default',
    poke_interval=60 * 5,
    dag=dag,
)

read_files = PythonOperator(
    task_id='read_files',
    python_callable=read_s3_folder,
    dag=dag,
)

check_folder >> read_files
