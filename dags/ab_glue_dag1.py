from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime, timedelta


### glue job specific variables
glue_job_name = "data_ingestion_rds"
glue_iam_role = "integration-glue-role"
region_name = "us-west-2"
email_recipient = "asif.khan@coditas.com"

default_args = {
    'owner': 'me',
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'email': email_recipient,
    'email_on_failure': True
}


with DAG(dag_id = 'klaviyo-sync-postgress-glue2', default_args = default_args, schedule_interval = None) as dag:
    
    glue_job_step = AwsGlueJobOperator(
        job_name =glue_job_name,
        script_location = 's3://my-s3-location',
        region_name = region_name,
        iam_role_name = glue_iam_role,
        script_args=None,
        num_of_dpus=10,
        task_id = 'glue_job_step',
        dag = dag
        )
   
    glue_job_step
