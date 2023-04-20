from datetime import datetime
import json
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator

DAG_ID = "mydemo_combine_json_files"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 11, 10),
    "tags": ["example"],
    "catchup": False,
}

with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=None) as dag:

    def combine_json_files():
        s3_hook = S3Hook(aws_conn_id='airbyte_glue_data_flattening')  # Make sure to create a connection to AWS in Airflow
        source_bucket_name = 'maisonette-airbyte-integration-landing-dev'
        source_prefix = 'dummyfolder1/'
        destination_bucket_name = 'maisonette-airbyte-integration-landing-dev'
        destination_prefix = 'dummyfolder2/'
        destination_key = f"{destination_prefix}combined.json"
        
        combined = []
        for json_file in s3_hook.list_keys(bucket_name=source_bucket_name, prefix=source_prefix):
            file_content = s3_hook.read_key(json_file)
            combined.append(json.loads(file_content))
        
        combined_json = json.dumps(combined)
        s3_hook.load_string(
            string_data=combined_json,
            key=destination_key,
            bucket_name=destination_bucket_name,
            replace=True
        )
        
        print(f"Combined JSON data saved to S3 bucket {destination_bucket_name} with key {destination_key}")

    combine_files = PythonOperator(
        task_id="combine_files",
        python_callable=combine_json_files
    )

    combine_files




# from datetime import datetime
# from typing import List, Optional, Tuple
# from airflow import DAG
# from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
# import os

# # This fixed NEGSIG.SIGEV error
# os.environ['no_proxy'] = '*'

# DAG_ID = "s3_file_transform"


# with DAG(
#     dag_id=DAG_ID,
#     schedule=None,
#     start_date=datetime(2022, 11, 10),
#     tags=["example"],
#     catchup=False,
# ) as dag:

#     move_files = S3FileTransformOperator(
#         task_id="move_files",
#         source_s3_key='s3://maisonette-airbyte-integration-landing-dev/dummyfolder1/myfile1.json',
#         dest_s3_key="s3://maisonette-airbyte-integration-landing-dev/dummyfolder2/myfile1.json",
#         transform_script="/bin/cp"
#     )

