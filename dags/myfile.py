import json
import boto3
import io
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

s3 = boto3.resource('s3')
bucket_name = 'maisonette-airbyte-integration-landing-dev'
input_prefix = 'dummyfolder1/'


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


def combine_json_files():
    combined = []
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=input_prefix):
        if obj.key.endswith('.json'):
            body = obj.get()['Body'].read()
            for line in body.decode().split('\n'):
                if line:
                    combined.append(json.loads(line))
    print(json.dumps(combined))

combine_files = PythonOperator(
    task_id='combine_files',
    python_callable=combine_json_files,
    dag=dag
)

