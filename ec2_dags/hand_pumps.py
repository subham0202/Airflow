from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, text
import pandas as pd

count = 1000

default_args = {
    'owner': 'Nitish',
    'start_date': days_ago(5)
}

with DAG(dag_id='hand_pumps',
         default_args=default_args,
         schedule_interval='*/2 * * * *',
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
    def extract_data(offset):
        # Use HTTP Hook to get connection details
        http_hook = HttpHook(http_conn_id='hand_pump', method='GET')

        # Build the API endpoint
        endpoint = f"/DepartmentData.asmx/GetDetailsHandPump?count={count}&offset={offset}"

        # Make the request via the HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            data = response.json()
            shikayat_data = data.get('Shikayat', [])  # Extract the 'Shikayat' key safely
            return shikayat_data
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

    @task()
    def transform_data(data):
        # Transform raw data into a DataFrame
        df = pd.DataFrame(data)
        print(df.head())
        return df

    @task()
    def load_data(df, table_name):
        if df.empty:
            raise ValueError("DataFrame is empty. Failing the task.")

        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()

        try:
            with engine.begin() as conn:
                df.to_sql(
                    table_name,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    chunksize=10000,
                    schema='bipard_staging'
                )
                print(f"Inserted {len(df)} rows successfully into {table_name}")

            # Update the offset in the table_status
            query = text(f"UPDATE bipard_staging.table_status SET offset_of_table = offset_of_table + {int(len(df))} WHERE table_name = '{table_name}';")
            with engine.begin() as con:
                con.execute(query)

        except Exception as e:
            print(f"Error loading data into {table_name}: {str(e)}")
            raise

    # DAG task dependencies
    offset = get_offset('hand_pumps')  # Change table name here
    t_data = extract_data(offset=offset)
    trans_data = transform_data(t_data)
    load_data(trans_data, 'hand_pumps')
