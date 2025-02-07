from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from sqlalchemy import create_engine, text
import pandas as pd

# Constants
COUNT = 1000

default_args = {
    'owner': 'Gaurav Sinha',
    'start_date': days_ago(5)
}

with DAG(dag_id='scheme_profile_payjal',
         default_args=default_args,
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False) as dag:

    @task()
    def get_offset(name: str) -> int:
        """Fetch the current offset for the table."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name='{name}';"))
                offset = result.scalar()
            return offset
        except Exception as e:
            print(f"Error fetching offset for table {name}: {e}")
            return 0

    @task()
    def extract_data(offset: int):
        """Extract data from NIC API using Airflow connection."""
        http_hook = HttpHook(http_conn_id='nic_api', method='GET')

        # Build the API endpoint
        endpoint = f'nic-data?url=http://164.100.251.114/pr/pr/scheme-profile-payjal&count={COUNT}&offset={offset}'

        try:
            response = http_hook.run(endpoint)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"Error during API request: {e}")
            raise

    @task()
    def transform_data(data):
        """Transform raw data into a DataFrame."""
        try:
            df = pd.DataFrame(data)
            print(f"Transformed DataFrame: {df.head()}")
            return df.to_dict(orient='records')
        except Exception as e:
            print(f"Error transforming data: {e}")
            raise

    @task()
    def load_data(data: list, table_name: str):
        if df.empty:
                raise AirflowFailException("DataFrame is empty. Failing the task.")
        """Load data into the MySQL table."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        df = pd.DataFrame(data)

        try:
            
            with engine.begin() as conn:# Insert data into the database
                df.to_sql(
                    table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    chunksize=1000,
                    schema='bipard_staging'
                )
                print(f"Inserted {len(df)} rows successfully into {table_name}")

            # Update the offset in the table_status
            query = text(f"UPDATE bipard_staging.table_status SET offset_of_table = offset_of_table + {len(df)} WHERE table_name = '{table_name}';")
            with engine.begin() as conn:
                conn.execute(query)
        except Exception as e:
            print(f"Error loading data to {table_name}: {e}")
            raise

    # Task pipeline
    offset = get_offset('scheme_profile_payjal')  # Replace with your table name
    raw_data = extract_data(offset=offset)
    transformed_data = transform_data(raw_data)
    load_data(transformed_data, 'scheme_profile_payjal')
