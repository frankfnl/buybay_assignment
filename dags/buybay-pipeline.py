from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from airflow import DAG
import pathlib
import inspect
import os
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Francisco Nava',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

platform_fee_dict = {
    'Bol':      0.10,
    'Amazon':   0.15,
    'Ebay':     0.09,
}

def data_dir():
    this_file_path = inspect.getframeinfo(inspect.currentframe()).filename
    parent_dir = os.path.abspath(os.path.join(this_file_path, os.pardir))
    parent_dir = parent_dir.replace('dags', 'data')
    return parent_dir

DATA_DIR = (
    pathlib.Path(os.environ.get("AIRFLOW_HOME", pathlib.Path("~/airflow").expanduser()))
    / "data"
)

def platform_fee(file):
    '''
    Returns a dataframe where platform_fee is filled in, if it wasn't already.

            Parameters:
                    csv (file): dataset where platform_fee can be empty

            Returns:
                    df (dataframe): dataset where platform_fee has no empty values
    '''

    df = pd.read_csv(file, parse_dates =['created_at', 'shipped_at'])
    df['platform_fee'] = df['platform_fee'].fillna(0)
    empty_rows_idx = list(df['platform_fee'].loc[lambda x: x==0].index)

    for index in empty_rows_idx:
        platform_name = df['platform'][index]
        if platform_name in platform_fee_dict.keys():
            df.loc[index, 'platform_fee'] = platform_fee_dict[platform_name]
        else:
            df.loc[index, 'platform_fee'] = 0.11

    print(df)
    return df

def total_fee():
    '''
    Returns the sum of two decimal numbers in binary digits.

            Parameters:
                    a (int): A decimal integer
                    b (int): Another decimal integer

            Returns:
                    binary_sum (str): Binary string of the sum of a and b
    '''
    ...

def total_transport():
    '''
    Returns the sum of two decimal numbers in binary digits.

            Parameters:
                    a (int): A decimal integer
                    b (int): Another decimal integer

            Returns:
                    binary_sum (str): Binary string of the sum of a and b
    '''
    ...

def total_payout():
    '''
    Returns the sum of two decimal numbers in binary digits.

            Parameters:
                    a (int): A decimal integer
                    b (int): Another decimal integer

            Returns:
                    binary_sum (str): Binary string of the sum of a and b
    '''
    ...

with DAG(
    dag_id = 'buybay_pipeline',
    default_args=default_args,
    description='Coding assignment for BuyBay',
    start_date=datetime(2023,6,22),
) as dag:
    extract_job = PythonOperator(
        task_id="platform_fee",
        python_callable=platform_fee,
        op_kwargs={
            "file": DATA_DIR / "sold_products.csv",
        },
    )
    extract_job = PythonOperator(
        task_id="total_fee",
        python_callable=total_fee,
    )
    extract_job = PythonOperator(
        task_id="total_transport",
        python_callable=total_transport,
    )