from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
import json
from sqlalchemy import text
import pandas as pd

count = 1000

default_args = {
    'owner': 'Harsh',
    'start_date': days_ago(5)
}

with DAG(dag_id='bcl_administrative_approval',
         default_args=default_args,
         schedule_interval='*/2 * * * *',
         max_active_runs=1,
         catchup=False) as dags:
    
    @task()
    def get_offset(name: str) -> int:
        # Initialize the MySql hook
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        try:
            with mysql_hook.get_sqlalchemy_engine().connect() as conn:
                result = conn.execute(text(f"SELECT offset_of_table FROM bipard_staging.table_status where table_name='{name}';"))
                offset = result.scalar()  # Fetch the scalar value of the count
            return offset
        except Exception as e:
            print(f"Error fetching count from table {name}: {e}")
            return -1

    @task()
    def extract_data(offset):
        # Use HTTP Hook to get connection details from Airflow connection
        http_hook = HttpHook(http_conn_id='nic_api', method='GET')

        # Build the API endpoint
        endpoint = f'//nic-data?url=http://pmisbcd.bihar.gov.in/bcdapi/pmis/bcl-administrative-approval&count={count}&offset={offset}'

        # Make the request via the HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    @task()
    def transform_data(data):
        # This function will contain the code for transformation when ingesting raw to AI
        df = pd.DataFrame(data)
        print(df.head())
        return df

    @task()
    def load_data(df, table_name):
        if df.empty:
            raise AirflowFailException("DataFrame is empty. Failing the task.")

        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        try:
            with mysql_hook.get_sqlalchemy_engine().begin() as conn:
                # Insert data into the database
                df.to_sql(
                    table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    chunksize=10000,
                    schema='bipard_staging'
                )
                print(f"Inserted {len(df)} rows successfully into {table_name}")

                # Update the offset in the table_status
                query = text(f"UPDATE bipard_staging.table_status SET offset_of_table = offset_of_table + {int(len(df))} WHERE table_name='{table_name}';")
                conn.execute(query)
        except Exception as e:
            print(f"Error loading data to {table_name}: {str(e)}")
            raise

    # DAG task dependencies
    offset = get_offset('bcl_administrative_approval')  # Change table name here
    t_data = extract_data(offset=offset)
    trans_data = transform_data(t_data)
    load_data(trans_data, 'bcl_administrative_approval')
