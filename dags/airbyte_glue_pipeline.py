from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import pendulum

AIRBYTE_CONNECTION_ID = 'd79acaf0-f71b-4d00-814b-1e952ff41675'


with DAG(
    dag_id='airbyte-glue-rds', 
    default_args={'owner': 'airflow'}, 
    schedule='@daily', 
    start_date=pendulum.today('UTC').add(days=-1)
    ) as dag:

   trigger_airbyte_salsify_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_salsify_sync',
       airbyte_conn_id='airbyteconnection',
       connection_id=AIRBYTE_CONNECTION_ID,
       asynchronous=True
   )

   wait_for_salsify_sync_completion = AirbyteJobSensor(
       task_id='airbyte_check_salsify_sync',
       airbyte_conn_id='airbyteconnection',
       airbyte_job_id=trigger_airbyte_salsify_sync.output
   )
   
   glue_job_operator_flatten = GlueJobOperator(
        task_id="glue_job_salsify_flatten",
        job_name="s3_ingestion_rds_final",
        script_location=f"s3://aws-glue-assets-276428873250-us-west-2/scripts/s3_ingestion_rds_final.py",
        s3_bucket="aws-glue-assets-276428873250-us-west-2",
        iam_role_name="glue-to-s3-rds-role",
        create_job_kwargs={"GlueVersion" : "3.0", "NumberOfWorkers" : 10, "WorkerType" : "G.1X"},
        aws_conn_id="airbyte_aws_data_flattening",
        dag=dag
    )

   trigger_airbyte_salsify_sync >> wait_for_salsify_sync_completion >> glue_job_operator_flatten
   
   
   
   
   



