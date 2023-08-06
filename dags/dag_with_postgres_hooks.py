from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import csv
import logging
from tempfile import NamedTemporaryFile


default_args = {
    'owner':'jk',
    'retries':5,
    'retry_delay': timedelta(minutes=10)
}

# Use Airflow macros to get datetime values: ds_nodash, next_ds_nodash
def postgres_to_s3(ds_nodash, next_ds_nodash):
    # step 1: query data from postgresql db ans
    hook = PostgresHook(PostgresHook='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    # cursor.execute("select * from orders where data <= '20220501'")
    cursor.execute("select * from orders where date >= %s and date < %s", (ds_nodash, next_ds_nodash))
    
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    # with open("dags/get_orders_{ds_nodash}.txt","w") as f:
        csv_writer = csv.write(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)
        
        f.flush()
        
        cursor.close()
        conn.close()
        logging.info("Saved orders data in text file: %s", f"dags/get_orders_{ds_nodash}.txt")
        
        
        # step 2: upload text file into s3
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=f"dags/get_orders_{ds_nodash}.txt",
            key=f"orders/{ds_nodash}.txt",
            bucket_name="airflow",
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)
    

with DAG(dag_id='dag_with_postgres_hooks_v02',
         default_args=default_args,
         start_date=datetime(2021,10,10),
         schedule_interval='@daily'
         ) as dag:
    
    task1 = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3
    )
    
    task1