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
    Returns a dataframe where platform_fee is filled in, if it wasn't already.

            Parameters:
                    prod_file (csv file): dataset with data from sold products,
                    platform_fee has empty values

            Returns:
                    df (dataframe): dataset with data from sold products
                    ,platform_fee has no empty values
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

    return df

def transport_fee(df, transport_file):
    '''
    Returns a dataframe where transport_cost is added as a new column.

            Parameters:
                    df (dataframe): dataset with data from sold products
                    transport_file (csv file): dataset that contains transport costs per country

            Returns:
                    df (dataframe): dataset with total transport_cost column
    '''
    df_cost = pd.read_csv(transport_file)
    costs_dict = dict(df_cost.values)
    costs_dict.pop('OTHER')
    df['transport_cost'] = df['country'].apply(lambda x: costs_dict[x] if (x in costs_dict.keys()) else 7.0)
    return df

def grading_fee(df, grading_fee_file, graded_products_file):
    '''
    Returns a dataframe where grading_fee is added as a new column.

            Parameters:
                    df (dataframe): dataset with data from sold products
                    grading_fee_file (csv file): dataset that contains grading fees
                    graded_products_file (csv file): dataset that contains graded products

            Returns:
                    df (dataframe): dataset with grading_fee column
    '''
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
    return df

def partner_payout(df):
    '''
    Returns a dataframe where partner_payout is added as a new column.

            Parameters:
                    df (dataframe): dataset with data from sold products
                    Required columns: sold_price, transport_cost, platform_fee, grading_fee

            Returns:
                    df (dataframe): dataset with partner_payout column
    '''
    df['partner_payout'] =  df['sold_price'] - (df['sold_price'] * 0.10) - df['transport_cost'] - df['platform_fee'] - df['grading_fee']
    return df

def total_fees(df):
    '''
    Returns a dataframe where total_fees is added as a new column.

            Parameters:
                    df (dataframe): dataset with data from sold products
                    Required columns: sold_price, platform_fee, grading_fee

            Returns:
                    df (dataframe): dataset with total_fees column
    '''
    df['total_fees'] = (df['sold_price'] * 0.10) + df['platform_fee'] + df['grading_fee']
    return df

def export_report(df, save_path):
    '''
    Converts report from dataframe to csv file.

            Parameters:
                    df (dataframe): dataset with all required data for reporting
                    save_path (path): destination path

            Returns:
                    report_file (csv file): dataset with all required data for reporting
    '''
    if os.path.isdir(save_path):
        df.to_csv(save_path / 'report.csv')
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
            'file': DATA_DIR / 'sold_products.csv',
        },
    )
    transport_fee_job = PythonOperator(
        task_id='transport_fee',
        python_callable=transport_fee,
        op_kwargs={
            'df': None,
            'file': DATA_DIR / 'transport_cost.csv',
        },
    )
    grading_fee_job = PythonOperator(
        task_id='grading_fee',
        python_callable=grading_fee,
        op_kwargs={
            'df': None,
            'file': DATA_DIR / 'grading_fees.csv',
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