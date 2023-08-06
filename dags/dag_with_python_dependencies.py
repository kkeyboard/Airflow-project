from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'jk',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def get_sklearn():
    import numpy
    print(f"numpy version:{numpy.__version__}")
    
def get_matplotlib():
    import matplotlib
    print(f"matplotlib with version:{matplotlib.__version__}")

with DAG(
    dag_id='dag_with_python_dependencies_v02',
    default_args=default_args,
    start_date=datetime(2022, 7, 29, 2),
    schedule_interval='@daily'
    ) as dag:
    
    get_sklearn = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )
    
    get_matplotlib = PythonOperator(
        task_id='get_matplotlib',
        python_callable=get_matplotlib
    )
    
    get_sklearn >> get_matplotlib