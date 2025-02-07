from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from sqlalchemy import text
import pandas as pd
import json

count = 500

default_args = {
    'owner': 'Nitish',
    'start_date': days_ago(5)
}

with DAG(dag_id='abhiyaan_basera',
         default_args=default_args,
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False) as dags:

    @task()
    def get_offset(name: str) -> int:
        # Initialize the MySQL hook
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name='{name}';"))
                offset = result.scalar()  # Fetch the scalar value of the offset
            return offset
        except Exception as e:
            print(f"Error fetching offset for table {name}: {e}")
            return -1

    @task()
    def extract_data(offset,table_name):
        # Use HTTP Hook to get connection details
        http_hook = HttpHook(http_conn_id='abhiyan_basera', method='GET')

        # Build the API endpoint
        endpoint = f"/admin/general/basera-data?securityKey=LbQo5B5NjnJhCZMSAQsv8OzaxcrG9jZ5&start={offset}&length={count}"

        # Make the request via the HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data["data"])
        if df is empty:
              raise AirflowFailException("DataFrame is empty. Failing the task.")# Extract the 'data' field as-is

        # Handle the JSON-like columns by converting them to JSON strings
        for column in ['familyDetails', 'landAllotment']:  # Add more columns if needed
            if column in df.columns:
                df[column] = df[column].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()

        try:
           
            with engine.begin() as conn:
            # Insert raw data into the target table
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    chunksize=10000,
                    schema='bipard_staging'
                )
                print(f"Inserted {len(df)} rows into {table_name}")

            # Update the offset in the table_status
            query = text(f"UPDATE bipard_staging.table_status SET offset_of_table = offset_of_table + {len(df)} WHERE table_name = '{table_name}';")
            with engine.begin() as con:
                con.execute(query)

        except Exception as e:
            print(f"Error loading data into {table_name}: {str(e)}")
            raise
  # Return the complete raw JSON response
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

        
    # DAG task dependencies
    t_offset = get_offset('abhiyaan_basera')  # Change table name here
    extract_data(t_offset,'abhiyaan_basera')

