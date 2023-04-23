from datetime import datetime
import json
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator


DAG_ID = "new_mydemocombine_json_files"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 11, 10),
    "tags": ["example"],
    "catchup": False,
}


def fix_json(json_string):
    """
    Takes a JSON string and tries to fix any errors.
    """
    try:
        json_object = json.loads(json_string)
        return json_object
    except json.JSONDecodeError:
        fixed_json = None
        if json_string.startswith("{") and json_string.endswith("}"):
            # Try to fix JSON with missing quotes around keys
            fixed_json = json.loads(json_string.replace("'", "\""))
        elif json_string.startswith("[") and json_string.endswith("]"):
            # Try to fix JSON with missing quotes around keys and values
            fixed_json = json.loads(json_string.replace("'", "\"").replace("True", "\"True\"").replace("False", "\"False\"").replace("None", "\"None\""))
        elif not json_string.endswith(","):
            # Try to fix JSON with missing comma at the end
            fixed_json = json.loads(json_string + ",")
        if fixed_json:
            return fixed_json
        else:
            # If unable to fix JSON, return an empty dictionary
            return {}


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
                valid_json = fix_json(line)
                if valid_json:
                    valid_json_objects.append(valid_json)
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



    def combine_json_files():
        s3_hook = S3Hook(aws_conn_id='airbyte_glue_data_flattening')  # Make sure to create a connection to AWS in Airflow
        source_bucket_name = 'maisonette-airbyte-integration-landing-dev'
        source_prefix = 'output_folder/'
        destination_bucket_name = 'maisonette-airbyte-integration-landing-dev'
        destination_prefix = 'combined_folder/'
        destination_key = f"{destination_prefix}combined.json"
        
        combined = []
        for json_file in s3_hook.list_keys(bucket_name=source_bucket_name, prefix=source_prefix):
            file_content = s3_hook.read_key(key=json_file, bucket_name=source_bucket_name)
            if file_content.strip() == "":
                # Skip empty files
                continue
            try:
                combined.append(json.loads(file_content))
            except json.decoder.JSONDecodeError:
                # Skip malformed files
                print(f"Skipping malformed file: {json_file}")

        combined_json = json.dumps(combined).encode('utf-8')  # Convert string data to bytes
        s3_hook.load_bytes(
            bytes_data=combined_json,
            key=destination_key,
            bucket_name=destination_bucket_name,
            replace=True
        )

        print(f"Combined JSON data saved to S3 bucket {destination_bucket_name} with key {destination_key}")

    combine_files = PythonOperator(
        task_id="combine_files",
        python_callable=combine_json_files
    )

    
    process_json >> combine_files
