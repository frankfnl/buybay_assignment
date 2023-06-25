from fastapi import FastAPI
from os import environ
from sqlalchemy import create_engine
import pandas as pd
import json

app = FastAPI()

db_host = environ.get("DB_HOST", "postgres")
db_port = environ.get("DB_PORT", 5432)
db_user = environ.get("DB_USER", "airflow")
db_database = environ.get("DB_DATABASE", "postgres")
db_password = environ.get("DB_PASSWORD", "airflow")

SQLALCHEMY_DATABASE_URI = environ.get(
    "DB_URI",
    f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}",
)


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/shipped/{license_plate}")
async def read_item(license_plate):
    engine = create_engine('postgresql://airflow:airflow@localhost:5432/postgres')
    df = pd.read_sql_query(f'select * from "report"',con=engine)
    df = df.drop_duplicates()
    df = df.loc[df['license_plate'] == license_plate]
    df = df.reset_index()

    if df.iloc[0]['status'] == 'shipped':
        x = {
            'product': {
                'license_plate':    license_plate,
                'sold_price' :      df['sold_price'][0],
                'buybay_fee':       df['sold_price'][0]*0.10,
                'transport_cost':   df['transport_cost'][0],
                'platform_fee':     df['platform_fee'][0],
                'grading_fee':      df['grading_fee'][0],
                'partner_payout':   df['partner_payout'][0]
            },
            'metadata': {
                'last_update': 'last_update'
            }
        }
        #convert into JSON:
        response = json.dumps(x)

        return response
    else:
        return 'Cannot retrieve data. The order was either cancelled or is not available.'