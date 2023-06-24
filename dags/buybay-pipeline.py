from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from airflow import DAG
import pathlib
from pathlib import Path
import inspect
import os
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DATA_DIR = (
    pathlib.Path(os.environ.get("AIRFLOW_HOME")) / "data"
)

default_args = {
    'owner': 'Francisco Nava',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
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

def platform_fee(prod_file):
    '''
    Creates a csv file ('report.csv') where platform_fee is filled in, if it wasn't already.

    File is saved to DATA_DIR.

            Parameters:
                    prod_file (csv file): dataset with data from sold products,
                    platform_fee has empty values
    '''

    df = pd.read_csv(prod_file, parse_dates =['created_at', 'shipped_at'])
    df['platform_fee'] = df['platform_fee'].fillna(0)
    empty_rows_idx = list(df['platform_fee'].loc[lambda x: x==0].index)

    for index in empty_rows_idx:
        platform_name = df['platform'][index]
        if platform_name in platform_fee_dict.keys():
            df.loc[index, 'platform_fee'] = platform_fee_dict[platform_name]
        else:
            df.loc[index, 'platform_fee'] = 0.11

    df.to_csv(DATA_DIR / 'report.csv', index=False)

def transport_fee(transport_file):
    '''
    Creates a csv file ('report.csv') where transport_cost is added as a new column.

    File is saved to DATA_DIR.

            Parameters:
                    transport_file (csv file): dataset that contains transport costs per country

    '''
    df = pd.read_csv(DATA_DIR / 'report.csv', parse_dates =['created_at', 'shipped_at'])
    df_cost = pd.read_csv(transport_file)
    costs_dict = dict(df_cost.values)
    costs_dict.pop('OTHER')
    df['transport_cost'] = df['country'].apply(lambda x: costs_dict[x] if (x in costs_dict.keys()) else 7.0)
    df.to_csv(DATA_DIR / 'report.csv', index=False)

def grading_fee(grading_fee_file, graded_products_file):
    '''
    Creates a csv file ('report.csv') where grading_fee is added as a new column.

    File is saved to DATA_DIR.

            Parameters:
                    df (dataframe): dataset with data from sold products
                    grading_fee_file (csv file): dataset that contains grading fees
                    graded_products_file (csv file): dataset that contains graded products

    '''
    df = pd.read_csv(DATA_DIR / 'report.csv', parse_dates =['created_at', 'shipped_at'])
    df_gfees = pd.read_csv(grading_fee_file)
    gfees_dict = dict(df_gfees.values)

    df_gproducts = pd.read_csv(graded_products_file)
    df_gproducts.columns= df_gproducts.columns.str.lower()
    df_gproducts = df_gproducts.drop(['grading_time'], axis=1)
    df_gproducts['grading_cat'] = df_gproducts['grading_cat'].str.upper()
    gproducts_dict = dict(df_gproducts.values)

    def grading_fee(x, gfees_dict, gproducts_dict):
        grading_cat = gproducts_dict[x]
        grading_fee = gfees_dict[grading_cat]
        return grading_fee

    df['grading_fee'] = df['license_plate'].apply(grading_fee, args=(gfees_dict, gproducts_dict))
    df.to_csv(DATA_DIR / 'report.csv', index=False)

def partner_payout():
    '''
    Creates a csv file ('report.csv') where partner_payout is added as a new column.

    File is saved to DATA_DIR.

            Requirements:
                    report.csv (file): file with data from sold products
                    Required columns: sold_price, transport_cost, platform_fee, grading_fee
    '''
    df = pd.read_csv(DATA_DIR / 'report.csv', parse_dates =['created_at', 'shipped_at'])
    df['partner_payout'] =  df['sold_price'] - (df['sold_price'] * 0.10) - df['transport_cost'] - df['platform_fee'] - df['grading_fee']
    df.to_csv(DATA_DIR / 'report.csv', index=False)

def total_fees():
    '''
    Creates a csv file ('report.csv') where total_fees is added as a new column.

    File is saved to DATA_DIR.

            Requirements:
                    report.csv (file): file with data from sold products
                    Required columns: sold_price, platform_fee, grading_fee
    '''
    df = pd.read_csv(DATA_DIR / 'report.csv', parse_dates =['created_at', 'shipped_at'])
    df['total_fees'] = (df['sold_price'] * 0.10) + df['platform_fee'] + df['grading_fee']
    df.to_csv(DATA_DIR / 'report.csv', index=False)

def export_report():
    '''
    Saves report file.

                Requirements:
                    report.csv (file): file with all required data for reporting
    '''
    if os.path.isdir(DATA_DIR):
        df = pd.read_csv(DATA_DIR / 'report.csv', parse_dates =['created_at', 'shipped_at'])
        df.to_csv(DATA_DIR / 'report.csv', index=False)
    else:
        raise Exception("Save directory does not exist")

with DAG(
    dag_id = 'buybay_pipeline',
    default_args=default_args,
    description='Coding assignment for BuyBay',
    start_date=datetime(2023,6,22),
) as dag:
    platform_fee_job = PythonOperator(
        task_id='platform_fee',
        python_callable=platform_fee,
        op_kwargs={
            'prod_file': DATA_DIR / 'sold_products.csv',
        },
    )
    transport_fee_job = PythonOperator(
        task_id='transport_fee',
        python_callable=transport_fee,
        op_kwargs={
            'transport_file': DATA_DIR / 'transport_cost.csv',
        },
    )
    grading_fee_job = PythonOperator(
        task_id='grading_fee',
        python_callable=grading_fee,
        op_kwargs={
            'grading_fee_file': DATA_DIR / 'grading_fees.csv',
            'graded_products_file': DATA_DIR / 'graded_products.csv',
        },
    )
    partner_payout_job = PythonOperator(
        task_id='partner_payout',
        python_callable=partner_payout,
    )
    total_fees_job = PythonOperator(
        task_id='total_fees',
        python_callable=total_fees,
    )
    export_report_job = PythonOperator(
        task_id='export_report',
        python_callable=export_report,
    )

    #Task Dependency
    platform_fee_job >> transport_fee_job >> grading_fee_job
    grading_fee_job >> partner_payout_job >> total_fees_job >> export_report_job
