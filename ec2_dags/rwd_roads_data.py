from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# API URL and credentials
api_url = "https://sqmsmobile.rwdbihar.gov.in/api/MyApi/GetRoadDetails"
credentials = {
    "username": "SQMS@MIS",
    "password": "MIS@9876"
}
count = 1000  # Number of records to fetch in each schedule

default_args = {
    'owner': 'Nitish',
    'start_date': days_ago(5)
}

with DAG(dag_id='rwd_roads_data',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         max_active_runs=1,
         catchup=False) as dag:
    
    @task()
    def get_offset(name: str) -> int:
        if df.empty:
                raise AirflowFailException("DataFrame is empty. Failing the task.")
        """Fetch the current offset from the database."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name='{name}';")
                )
                offset = result.scalar()
            return offset
        except Exception as e:
            print(f"Error fetching offset for table {name}: {e}")
            return -1

    @task()
    def fetch_data(offset: int):
        """Fetch data from the API using POST request."""
        try:
            # Add count and offset as additional parameters
            payload = credentials.copy()
            payload['count'] = count
            payload['offset'] = offset

            response = requests.post(api_url, data=payload)
            if response.status_code == 200:
                data = response.json()
                print("Data fetched successfully.")
                return data
            else:
                raise Exception(f"Failed to fetch data (HTTP {response.status_code}): {response.text}")
        except Exception as e:
            raise Exception(f"An error occurred while fetching data: {e}")

    @task()
    def transform_data(data):
        """Transform the data into a structured DataFrame."""
        if data and data.get("Success") and "RoadDetails" in data:
            road_details = data["RoadDetails"]
            if isinstance(road_details, list):
                df = pd.DataFrame(road_details)
            elif isinstance(road_details, dict):
                df = pd.DataFrame([road_details])
            else:
                raise ValueError("Unexpected data format in RoadDetails.")
            return df
        else:
            raise ValueError("No valid data found in the response.")

    @task()
    def load_data(df, table_name):
        """Load the DataFrame into MySQL."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        try:
            with engine.begin() as conn:
                df.to_sql(
                    table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    chunksize=1000,
                    schema='bipard_staging'
                )
                print(f"Inserted {len(df)} rows into {table_name} successfully.")

                # Update the offset in the database
            query = text(
                f"UPDATE bipard_staging.table_status SET offset_of_table=offset_of_table+{len(df)} WHERE table_name='{table_name}';"
            )
            with engine.begin() as conn:
                conn.execute(query)
        except Exception as e:
            raise Exception(f"Error loading data into {table_name}: {e}")

    offset = get_offset('rwd_roads_data')  # Update table name here
    raw_data = fetch_data(offset=offset)
    transformed_data = transform_data(raw_data)
    load_data(transformed_data, 'rwd_roads_data')  # Update table name here
