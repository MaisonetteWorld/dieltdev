from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from test_python_script import my_print_func
from airflow.operators.python_operator import PythonOperator
import pendulum




with DAG(dag_id='my_python_script_dag',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

   run_this = PythonOperator(
      task_id='my_python_operator',
      python_callable= my_print_func,
      dag=dag
   )

   
run_this
