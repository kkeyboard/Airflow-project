from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'jk',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v5',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2022, 7, 29, 2),
    schedule_interval='@daily'
    
    ) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world'
    )
    
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo Konichiwa Sekai'
    )
    
    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo It will run after task1.'
    )

    # Method 1: using set_downstream()
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    
    # Method 2: using left shift bit operator
    # task1 >> task2
    # task1 >> task3
    
    # Method 3:
    task1 >> [task2, task3]
   