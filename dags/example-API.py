from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner':'jk',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow_api_v02',
     default_args=default_args,
     start_date=datetime(2021, 10, 26),
     schedule_interval='@daily')
def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {'first_name':'Jerry', 'last_name':'Grant'}

    @task()
    def get_age():
        return 20
    
    @task()
    def greet(first_name, last_name, age):
        # print(f"Hello world, my name is {name} and I am {age} years old.")
        print(f"Hello world, my name is {first_name} {last_name} and I am {age} years old.")
        
    # name = get_name()
    name_dict = get_name()
    
    age = get_age()
    greet(name_dict['first_name'], name_dict['last_name'], age)

greet_dag = hello_world_etl()