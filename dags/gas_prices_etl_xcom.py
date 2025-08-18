from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd 
import http.client

load_dotenv()


def extract_gas_prices(**kwargs):
    conn = http.client.HTTPSConnection("api.collectapi.com")

    headers = {
        'content-type': "application/json",
        'authorization': f"apikey {os.getenv('COLLECT_API_KEY')}"
        }

    conn.request("GET", "/gasPrice/allUsaPrice", headers=headers)

    res = conn.getresponse()
    data = res.read()
    data_json = data.decode('utf-8')

    # json.loads(data)
    kwargs['ti'].xcom.push(key="data" ,value=data_json)
    

def transform_gas_prices(**kwargs):
    data = kwargs['ti'].xcom.pull(task_ids='extract_gas_prices', key='data')
    gas_data = json.loads(data)["result"]
    gas_df = pd.DataFrame(gas_data)
    gas_df = gas_df.rename(columns={"name":"city"}, inplace=True)
    gas_data=gas_df.to_json(orient='records') 
    kwargs['ti'].xcom.push(key="gas_df", value=gas_data)
    
    
def load_gas_prices(**kwargs):
    gas_data = kwargs['ti'].xcom.pull(task_ids="transform_gas_prices" , key='gas_data')
    gas_df=pd.read_json(gas_data, orient='records')
    db_ur = os.getenv("DATA_BASE_URI2")
    engine = create_engine(url=db_ur)   
           
    gas_df.to_sql(name='gas_price_data', con=engine, if_exists='append', index=False)



#args
default_args = {
    "owner":"Walter",
    "start_date": datetime(2025,8,18),
    "retries":3,
    "retry_delay":timedelta(minutes=3),
        
}
# Dag Definition 

with DAG(
    dag_id='gas_pipeline', 
    default_args=default_args, 
    schedule_interval='@daily',
    catchup=False,
    description='A to get gas prices'
) as dag:
    extract_task = PythonOperator(
        task_id = 'extract_gas_prices',
        python_callable=extract_gas_prices
    )
    
    transform_task = PythonOperator(
        task_id="transform_gas_prices",
        python_callable=transform_gas_prices
    )
    
    load_task = PythonOperator(
        task_id="load_gas_prices",
        python_callable=load_gas_prices
    )


#scheduling
extract_task >> transform_task >>load_task

