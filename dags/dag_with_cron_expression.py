from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner':'jk',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='dag_with_cron_expression_v01',
         default_args=default_args,
         start_date=datetime(2021, 11, 1),
         schedule_interval='0 0 * * *'
    ) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo dag with cron expression.'
    )
    
    task1