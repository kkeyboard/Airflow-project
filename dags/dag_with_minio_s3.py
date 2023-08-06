from datetime import datetime, timedelta
from airflow import DAG

# Waits for one or multiple keys (a file-like instance on S3) to be present in a S3 bucket.
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner':'jk',
    'retries':5,
    'retry_delay':timedelta(minutes=10)
}

with DAG(dag_id='dag_with_minio_s3_v02',
         default_args = default_args,
         start_date=datetime(2021,10,10),
         schedule_interval='@daily',
    ) as dag:
    
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn',
        # mode='poke',
        # poke_interval=5,
        # timeout=30
    )
    
    task1