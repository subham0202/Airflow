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
from datetime import datetime, timedelta


count=1000


default_args = {
    'owner': 'Raghvendra',
    'start_date': days_ago(5)
}

with DAG(dag_id='service_plus',
         default_args=default_args,
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False) as dags:
    
    @task()
    def get_offset(name: str):
        # Initialize the Postgres hook
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        # Execute the query to count rows
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT extra_data FROM bipard_staging.table_status where table_name='{name}';"))
                offset = result.scalar()  # Fetch the scalar value of the count
            return offset
        except Exception as e:
            print(f"Error fetching count from table {name}: {e}")
            return -1



    @task()
    def extract_data(offset):
        """Extract weather data from NIC API using Airflow Connection."""

        # API endpoint
        endpoint = 'https://swcs.bihar.gov.in/RtpsReportView/restJson/spWS/codebucket/getData'
        from_date = str(offset)
        date_obj = datetime.strptime(from_date, "%Y-%m-%d")
        to_date = date_obj + timedelta(days=6)
        from_date=str(from_date)
        to_date=str(to_date)
        # Assume `fromDate` is fixed as the beginning date (or fetch it dynamically if needed)
        # Example: If the beginning date is known to be "2000-01-01"
         # Replace with dynamic logic if necessary

        # JSON payload
        payload = {
            "fromDate": from_date,
            "toDate": to_date
        }        


        # Headers
        headers = {
            "Content-Type": "application/json"
        }

        # Disable SSL verification (for debugging purposes only)
        verify_ssl = False  # Change to True when using a valid certificate

        try:
            # Use Airflow's HttpHook
            http_hook = HttpHook(http_conn_id='id_1234', method='POST')

            # Make the request via requests (bypassing HttpHook for better control)
            session = http_hook.get_conn()
            response = session.post(endpoint, headers=headers, json=payload, verify=verify_ssl)

            # Check response status
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

        except requests.exceptions.SSLError:
            raise Exception("SSL certificate verification failed. Try disabling SSL verification temporarily.")
        except Exception as e:
            raise Exception(f"Error during API call: {str(e)}")

            
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
                    # offset=str(offset)
                # date_obj = datetime.strptime(offset, "%Y-%m-%d")
                # extra_data = date_obj + timedelta(days=7)
                # print(extra_data)
                query = text(f"update bipard_staging.table_status set extra_data= DATE_ADD(extra_data, INTERVAL 7 DAY) where table_name='{table_name}';")
                print(query)
             
                with engine.begin() as con:
                    con.execute(query)
                            
            except Exception as e:
                print(f"Error loading data to {table_name}: {str(e)}")
                raise



    offset=get_offset('service_plus') #change table name here 
    t_data=extract_data(offset=offset)
    trans_data=transform_data(t_data)
    load_data(trans_data,'service_plus')