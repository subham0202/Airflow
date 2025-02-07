from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import json
from sqlalchemy import create_engine, text
import pandas as pd


count=1000


default_args = {
    'owner': 'Nitish',
    'start_date': days_ago(5)
}

with DAG(dag_id='har_ghar_nal_ka_jal_pay_jal',
         default_args=default_args,
         schedule_interval='*/2 * * * *',
         max_active_runs=1,
         catchup=False) as dags:
    
    @task()
    def get_offset(name: str) -> int:
        # Initialize the Postgres hook
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        # Execute the query to count rows
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT offset_of_table FROM bipard_staging.table_status where table_name='{name}';"))
                offset = result.scalar()  # Fetch the scalar value of the count
            return offset
        except Exception as e:
            print(f"Error fetching count from table {name}: {e}")
            return -1



    @task()
    def extract_data(offset):
        """Extract weather data from NIC API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id='nic_api',method='GET')

        ## Build the API endpoint
        ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        # http://3.7.100.74:3000/api
        endpoint=f'/nic-data?url=http://164.100.251.114/pr/pr/scheme-profile-payjal&count={count}&offset={offset}'

        #change your end point from // to count=

        ## Make the request via the HTTP Hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")


    @task()
    def transform_data(data):
        ####This Function will contain the code for tranformation when injesting raw to AI####
        df=pd.DataFrame(data)
        print(df.head())
        return df

    @task()
    def load_data(df, table_name):
            if df.empty:
                raise AirflowFailException("DataFrame is empty. Failing the task.")

            mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
            engine = mysql_hook.get_sqlalchemy_engine()
        
            try:
                # Use MySqlHook to get SQLAlchemy engine
                

                with engine.begin() as conn:
                
                # Insert data into the database
                    df.to_sql(
                        table_name,
                        con=conn,
                        if_exists='append',
                        index=False,
                        chunksize=10000,
                schema='bipard_staging'                )
                    print(f"Inserted {len(df)} rows successfully into {table_name}")

                # Update the mauja_code table
                query = text(f"update bipard_staging.table_status set offset_of_table=offset_of_table+{int(len(df))} where table_name='{table_name}';")
                print(query)
             
                with engine.begin() as con:
                    con.execute(query)
                            
            except Exception as e:
                print(f"Error loading data to {table_name}: {str(e)}")
                raise







    offset=get_offset('har_ghar_nal_ka_jal_pay_jal') #change table name here 
    t_data=extract_data(offset=offset)
    trans_data=transform_data(t_data)
    load_data(trans_data,'har_ghar_nal_ka_jal_pay_jal')
