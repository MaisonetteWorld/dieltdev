from datetime import datetime
import json
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator

import demjson


DAG_ID = "new_mydemocombine_json_files"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 11, 10),
    "tags": ["example"],
    "catchup": False,
}


with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=None) as dag:

    def process_json_files():
        s3_hook = S3Hook(aws_conn_id='airbyte_glue_data_flattening')
        source_bucket_name = 'maisonette-airbyte-integration-landing-dev'
        source_prefix = 'input_folder/'
        destination_bucket_name = 'maisonette-airbyte-integration-landing-dev'
        destination_prefix = 'output_folder/'

        for json_file in s3_hook.list_keys(bucket_name=source_bucket_name, prefix=source_prefix):
            file_content = s3_hook.read_key(key=json_file, bucket_name=source_bucket_name)
            output_filename = json_file.split('/')[-1]  # Use the source filename as the basis for the output filename
            valid_json_objects = []
            for line in file_content.splitlines():
                try:
                    json_object = json.loads(line)
                    valid_json_objects.append(json_object)
                except json.JSONDecodeError:
                    # Try to fix invalid JSON with demjson
                    fixed_json = demjson.decode(line, strict=False)
                    valid_json_objects.append(fixed_json)
            if len(valid_json_objects) > 0:
                valid_json = json.dumps(valid_json_objects).encode('utf-8')
                s3_hook.load_bytes(
                    bytes_data=valid_json,
                    key=f"{destination_prefix}{output_filename}",
                    bucket_name=destination_bucket_name,
                    replace=True
                )
                print(f"Processed file {json_file} and saved valid JSON data to S3 bucket {destination_bucket_name} with key {destination_prefix}{output_filename}")

    process_json = PythonOperator(
        task_id="process_json",
        python_callable=process_json_files
    )

    process_json
