from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
import pendulum
import json




AIRBYTE_CONNECTION_ID = '91fb5891-7739-48aa-b03a-eb880aad839e'

with DAG(
    dag_id='airbyte_slasify_glue', 
    default_args={'owner': 'airflow'}, 
    schedule='@daily', 
    start_date=pendulum.today('UTC').add(days=-1)
    ) as dag:
    
    trigger_airbyte_sync = AirbyteTriggerSyncOperator(
       task_id='airbytsalsify_trigger_sync',
       airbyte_conn_id='airbyteconnection',
       connection_id=AIRBYTE_CONNECTION_ID,
       asynchronous=True
   )

   wait_for_sync_completion = AirbyteJobSensor(
       task_id='airbyte_salsify_check_sync',
       airbyte_conn_id='airbyteconnection',
       airbyte_job_id=trigger_airbyte_sync.output
   )

    

    invoke_lambda_function = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_function",
        function_name="data-integration-landing",
        payload=json.dumps({"key1" : "value1"}),
        aws_conn_id="airbyte_aws_data_flattening",

        dag=dag
    )

    trigger_airbyte_sync >> wait_for_sync_completion >> invoke_lambda_function