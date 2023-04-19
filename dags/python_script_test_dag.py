from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import pendulum



with DAG(dag_id='my_python_script_dag',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

   run_python_script = BashOperator(
      task_id= 'run_python_script',
      bash_command= 'python scripts/test_python_script.py'
      dag=dag
   )

   run_python_script