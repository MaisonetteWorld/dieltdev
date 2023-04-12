from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.sensors.filesystem import FileSensor
from airflow.contrib.operators.aws_glue_job_operator import AwsGlueJobOperator
import pendulum

AIRBYTE_CONNECTION_ID = '91fb5891-7739-48aa-b03a-eb880aad839e'

with DAG(dag_id='klaviyo-sync-postgress-glue',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

   trigger_airbyte_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_sync',
       airbyte_conn_id='airbyteconnection',
       connection_id=AIRBYTE_CONNECTION_ID,
       asynchronous=True
   )

   wait_for_sync_completion = AirbyteJobSensor(
       task_id='airbyte_check_sync',
       airbyte_conn_id='airbyteconnection',
       airbyte_job_id=trigger_airbyte_sync.output
   )

   trigger_glue_job = AwsGlueJobOperator(
       task_id='trigger_glue_job',
       job_name='Data-Flatten-klaviyo',
       aws_conn_id='airbyte_glue_data_flattening',  # replace with your AWS connection ID
       region_name='us-west-2'  # replace with your AWS region
   )

   trigger_airbyte_sync >> wait_for_sync_completion >> trigger_glue_job