from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import logging
from airflow import settings
from airflow.models import Connection

def create_conn(conn_id, conn_type, host, schema, login, pwd, port, desc):
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
    create_table_job=PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id = 'postgres_localhost',
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    #Task Dependency
    create_conn_job >> create_table_job