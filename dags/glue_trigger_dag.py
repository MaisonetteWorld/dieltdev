from airflow import DAG
import pendulum
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


with DAG(
    dag_id='s3-glue-flatten-rds', 
    default_args={'owner': 'airflow'}, 
    schedule='@daily', 
    start_date=pendulum.today('UTC').add(days=-1)
    ) as dag:

    glue_job_operator = GlueJobOperator(
        task_id="glue_job_flatten",
        job_name="s3_ingestion_rds_final",
        script_location=f"s3://aws-glue-assets-276428873250-us-west-2/scripts/s3_ingestion_rds_final.py",
        s3_bucket="aws-glue-assets-276428873250-us-west-2",
        iam_role_name="glue-to-s3-rds-role",
        create_job_kwargs={"GlueVersion" : "3.0", "NumberOfWorkers" : 10, "WorkerType" : "G.1X"},
        aws_conn_id="airbyte_aws_data_flattening",
        dag=dag
    )

    glue_job_operator


