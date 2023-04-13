from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import pendulum
#AIRBYTE_CONNECTION_ID = '91fb5891-7739-48aa-b03a-eb880aad839e'

with DAG(dag_id='klaviyo-sync-postgress-glue1',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

    submit_glue_job = GlueJobOperator(
    task_id="submit_glue_job",
    job_name="Data-Flatten-klaviyo",
    script_location=f"s3://maisonette-airbyte-integration-landing-dev/Flattened-data-glue-dag/maisonette_codes-0.0.1-py3-none-any.whl",
    s3_bucket="maisonette-airbyte-integration-landing-dev",
    iam_role_name=data-integration-glue-role,
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
)
# trigger_glue_job
submit_glue_job
