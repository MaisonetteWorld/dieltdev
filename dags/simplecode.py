from datetime import datetime
from typing import List, Optional, Tuple
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
import os

# This fixed NEGSIG.SIGEV error
os.environ['no_proxy'] = '*'

DAG_ID = "s3_file_transform"


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2022, 11, 10),
    tags=["example"],
    catchup=False,
) as dag:

    move_files = S3FileTransformOperator(
        task_id="move_files",
        source_s3_key='s3://maisonette-airbyte-integration-landing-dev/dummyfolder1/myfile1.json',
        dest_s3_key="s3://maisonette-airbyte-integration-landing-dev/dummyfolder2/myfile1.json",
        transform_script="/bin/cp"
    )

