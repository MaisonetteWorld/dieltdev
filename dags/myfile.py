import json
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
from airflow.operators import SimpleHttpOperator, HttpSensor, EmailOperator, S3KeySensor
import os

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
    'combine_json_files',
    default_args=default_args,
    description='Combine JSON files from an S3 bucket',
    schedule_interval='@daily'
)

def combine_json_files():
    bucket_name = 'maisonette-airbyte-integration-landing-dev'
    source_folder = 'dummyfolder1/'
    dest_folder = 'dummyfolder2/'

    # Get a list of all the JSON files in the source folder
    files = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)

    if 'Contents' not in files:
        print("No files found in the source folder.")
        return
    
    # Load the contents of each JSON file into a list
    combined = []
    for file in files['Contents']:
        if not file['Key'].endswith('.json'):
            continue
        
        file_obj = s3.get_object(Bucket=bucket_name, Key=file['Key'])
        file_content = json.loads(file_obj['Body'].read().decode('utf-8'))
        combined.extend(file_content)
        
    # Write the combined JSON data to a file in the dest folder
    dest_file_path = os.path.join(dest_folder, 'combined.json')
    s3.put_object(Bucket=bucket_name, Key=dest_file_path, Body=json.dumps(combined))
    print(f"Combined JSON file saved to s3://{bucket_name}/{dest_file_path}")

check_source_files = S3KeySensor(
    task_id='check_source_files',
    bucket_key=f"my-bucket/source_json_files/",
    wildcard_match=True,
    aws_conn_id='aws_default',
    poke_interval=60 * 5,
    dag=dag,
)

combine_files = PythonOperator(
    task_id='combine_files',
    python_callable=combine_json_files,
    dag=dag,
)

check_source_files >> combine_files
