from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.glue import AwsGlueJobHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 13),
}

dag = DAG(
    'glue_trigger_mydag',
    default_args=default_args,
    schedule_interval='@once'
)

def trigger_glue_job():
    job_name = 'Data-Flatten-klaviyo'
    glue_job_hook = AwsGlueJobHook(job_name=job_name,aws_conn_id='airbyte_glue_data_flattening') # Replace 'your-glue-job-name' with the name of your Glue job
    response = glue_job_hook.run_job()
    print(response)

trigger_glue = PythonOperator(
    task_id='trigger_glue',
    python_callable=trigger_glue_job,
    dag=dag
)
