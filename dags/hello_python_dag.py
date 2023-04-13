from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG object
dag = DAG(
    'hello_python_dag',
    default_args=default_args,
    description='A simple DAG that prints "Hello, World!"',
    schedule_interval=timedelta(days=1)
)

# Define the Python function to be executed by the PythonOperator
def print_hello():
    print("Hello, World!")

# Define the PythonOperator that executes the print_hello() function
print_hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)
