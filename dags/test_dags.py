from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests


default_args = {
    "owner":"Walter",
    "start_date": datetime(2025,7,14),
    "retries":3,
    "retry_delay":timedelta(minutes=3),
        
}


#Functions (ETL)

def extract():
    print("Hello there, this is a Extract Test Dag")
    

def print_name():
    print(f"My name is {default_args['owner']}")
    

# Dag Definition 

with DAG(
    dag_id='printer_code', 
    default_args=default_args, 
    schedule_interval='@daily',
    catchup=False,
    description='A simple DAG ti say hello'
) as dag:
    extract_task = PythonOperator(
        task_id = 'say_extract',
        python_callable=extract
    )
    
    print_task = PythonOperator(
        task_id="print_name",
        python_callable=print_name
    )


#scheduling
extract_task >> print_task
