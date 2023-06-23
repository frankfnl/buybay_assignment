from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Francisco Nava',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def print_hello():
    print("hello")

with DAG(
    dag_id = 'buybay_pipeline',
    default_args=default_args,
    description='Coding assignment for BuyBay',
    start_date=datetime(2023,6,22),
) as dag:
    extract_job = PythonOperator(
        task_id="extract_zip_file",
        python_callable=print_hello,
    )