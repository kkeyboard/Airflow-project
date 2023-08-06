from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator 

default_args = {
    'owner':'jk',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def greet(ti):
    # name = ti.xcom_pull(task_ids='get_name')
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    
    print(f"Hello {first_name} {last_name}. You are {age} years old.")
    
def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerrey')
    ti.xcom_push(key='last_name', value='Tate')
    
def get_age(ti):
    ti.xcom_push(key='age', value=30)
    

with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v08',
    description='Our first dag using Python operator',
    start_date=datetime(2022, 7,29),
    schedule_interval='@daily'
    ) as dag:
    
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        #op_kwargs={'name':'John', 'age':30}
        # op_kwargs={'age':30}
        
    )
    
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    
    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
    
    #task1
    [task2, task3] >> task1
    