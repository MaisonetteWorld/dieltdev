import json
import boto3
import glob
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

s3 = boto3.resource('s3')
bucket_name = 'maisonette-airbyte-integration-landing-dev'
input_prefix = 'dummyfolder1/'
output_prefix = 'dummyfolder2/'
output_file_name = 'combinedtest.json'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'combine_json_files',
    default_args=default_args,
    schedule_interval='@daily'
)

def create_output_directory():
    if not os.path.exists(output_prefix):
        os.makedirs(output_prefix)

create_dir = BashOperator(
    task_id='create_output_directory',
    bash_command=f'mkdir -p {output_prefix}',
    dag=dag
)

def combine_json_files():
    combined = []
    for json_file in glob.glob(input_prefix + "/*.json"):
        with open(json_file, "r") as infile:
            for line in infile:
                combined.append(json.loads(line))
    output_file_path = os.path.join(output_prefix, output_file_name)
    with open(output_file_path, "w") as outfile:
        for item in combined:
            outfile.write(json.dumps(item) + "\n")
    s3.meta.client.upload_file(output_file_path, bucket_name, output_file_path)

combine_files = PythonOperator(
    task_id='combine_files',
    python_callable=combine_json_files,
    dag=dag
)

create_dir >> combine_files
