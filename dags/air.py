from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from sqlalchemy import create_engine


default_args={
    "owner":"Walter",
    "start_date":datetime(2025,7,17),
    "retries":3,
    "retry_delay":timedelta(minutes=3)
}

def fetch_and_post(): 

    #fetch data
    url="https://api.openweathermap.org/data/2.5/weather?q=nairobi&appid=c32170926bb267e9e09b323867c11c47&units=metric"
    response = requests.get(url)
    data = response.json()
    
    #data required: description, temp, 
    record = {"datetime":datetime.fromtimestamp(data['dt'],tz=timezone.utc).strftime('%d-%m-%Y %H:%M:%S'),
          "feels_like": data["main"]["feels_like"],
          "temp_min": data["main"]["temp_min"],
          "temp_max":data["main"]["temp_max"],
          "pressure":data["main"]["pressure"],
          "humidity":data["main"]["humidity"],
          "visibility":data["visibility"],
          "wind_speed":data["wind"]["speed"],
          "wind_gust":data["wind"]["gust"]
          
    
        }
    
    #Convert record to dataframe
    df = pd.DataFrame([record])
    
    #Connect to DB
    db_uri="postgresql://avnadmin:AVNS_OQ5_dSlMAfLfQcQEE-b@walter-walterbilionnaire-4951.j.aivencloud.com:11167/defaultdb?sslmode=require"
    engine =  create_engine(db_uri)

    #add to db
    df.to_sql('weather', con=engine, if_exists='append', index=False)
    


#dag definition
with DAG(
    dag_id='save_weather',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description='Save Nairobi weather hourly'
) as dag:
    extract_task = PythonOperator(
        task_id='save_weather',
        python_callable=fetch_and_post
    )
    

#scheduling
extract_task
    
