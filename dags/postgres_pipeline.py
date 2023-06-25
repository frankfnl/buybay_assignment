from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import logging
from airflow import settings
from airflow.models import Connection

def create_conn(conn_id, conn_type, host, schema, login, pwd, port, desc):
    '''
    Create a connection object programmatically 
    '''
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      host=host,
                      schema=schema,
                      login=login,
                      password=pwd,
                      port=port,
                      description=desc)
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn.conn_id):
        logging.warning(f"Connection {conn.conn_id} already exists")
        return None

    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn_id} is created')
    return conn
#----------------------------------------------------------------------------------------------------
import pathlib
import os
from sqlalchemy import create_engine
import pandas as pd

DATA_DIR = (
    pathlib.Path(os.environ.get("AIRFLOW_HOME")) / "data"
)

def fill_table():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')
    df = pd.read_csv(DATA_DIR / 'report.csv', parse_dates =['created_at', 'shipped_at'])
    df.to_sql('report', engine, if_exists='replace', index=False)
#----------------------------------------------------------------------------------------------------

default_args = {
    'owner': 'Francisco Nava',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id = 'postgres_pipeline',
    default_args=default_args,
    description='Postgres Pipeline',
    start_date=datetime(2023,6,22),
) as dag:
    create_conn_job = PythonOperator(
        task_id='create_conn',
        python_callable=create_conn,
        op_kwargs={
            'conn_id': 'postgres_localhost',
            'conn_type': 'postgres',
            'host': 'postgres',
            'schema': 'postgres',
            'login': 'airflow',
            'pwd': 'airflow',
            'port': 5432,
            'desc': 'Postgres Airflow Connection'
        },
    )
    # create_table_job=PostgresOperator(
    #     task_id='create_postgres_table',
    #     postgres_conn_id = 'postgres_localhost',
    #     sql="""
    #         CREATE TABLE IF NOT EXISTS report (
    #             license_plate VARCHAR PRIMARY KEY,
    #             status VARCHAR,
    #             platform VARCHAR,
    #             created_at DATE,
    #             shipped_at DATE,
    #             sold_price INT,
    #             country VARCHAR,
    #             channel_ref VARCHAR,
    #             platform_fee FLOAT,
    #             transport_cost FLOAT,
    #             grading_fee FLOAT,
    #             partner_payout FLOAT,
    #             total_fees FLOAT);
    #     """
    # )
    fill_table_job = PythonOperator(
        task_id='fill_table',
        python_callable=fill_table,
    )

    #Task Dependency
    create_conn_job >> fill_table_job