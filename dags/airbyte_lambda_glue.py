from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
import pendulum
import json

AIRBYTE_CONNECTION_ID = '91fb5891-7739-48aa-b03a-eb880aad839e'

with DAG(
        dag_id='airbyte_klaviyo',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
) as dag:
    
    trigger_airbyte_klaviyo_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_trigger_klaviyo_sync',
        airbyte_conn_id='airbyteconnection',
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=True
    )

    wait_for_klaviyo_sync_completion = AirbyteJobSensor(
    task_id='airbyte_check_klaviyo_sync',
    airbyte_conn_id='airbyteconnection',
    airbyte_job_id=trigger_airbyte_klaviyo_sync.output
    )


    trigger_airbyte_klaviyo_sync >> wait_for_klaviyo_sync_completion
