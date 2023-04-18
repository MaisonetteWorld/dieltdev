from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.providers.amazon.aws.operators.lambda import AWSLambdaInvokeFunctionOperator
import pendulum
#AIRBYTE_CONNECTION_ID = '91fb5891-7739-48aa-b03a-eb880aad839e'

with DAG(dag_id='klaviyo-sync-postgress-glue1',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

    submit_glue_job = GlueJobOperator(
    task_id="submit_glue_job",
    job_name="test_rds",
    script_location=f"s3://aws-glue-assets-276428873250-us-west-2/scripts/test_rds.py",
#     extra_python_libs=f"s3://maisonette-airbyte-integration-landing-dev/python-glue-dag-script/glue_code_files-0.1-py3-none-any.whl",
    s3_bucket="maisonette-airbyte-integration-landing-dev",
    iam_role_name="data-integration-glue-role",
    aws_conn_id='airbyte_glue_data_flattening',  # replace with your AWS connection ID
    region_name="us-west-2"  # replace with your AWS region
#     create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
)
# trigger_glue_job
submit_glue_job
