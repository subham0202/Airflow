from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowFailException  # Fixed Import
import pandas as pd
from sqlalchemy import text
import json

# Batch size for API requests
COUNT = 1000

default_args = {
    'owner': 'Subham',
    'start_date': days_ago(5),
    'depends_on_past': False,
    # 'on_failure_callback': task_failure_alert,
    # 'retries': 3,
    # 'retry_delay': timedelta(seconds=3)
}

with DAG(
    dag_id='ela_bharti',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    max_active_runs=1,
    catchup=False
) as dag:  # Fixed reference from `dags` to `dag`
    
    @task()
    def get_offset(name: str) -> int:
        """Fetch offset from MySQL for pagination."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        try:
            with mysql_hook.get_sqlalchemy_engine().connect() as conn:
                result = conn.execute(text(f"SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name=:name"), {'name': name})
                offset = result.scalar()  # Fetch the scalar value
            return offset if offset is not None else 0  # Default to 0 if no value found
        except Exception as e:
            print(f"Error fetching offset from table {name}: {e}")
            return -1

    @task()
    def extract_data(offset: int, table_name: str):
        """Extract data from NIC API using Airflow Connection."""
        if offset == -1:
            raise AirflowFailException("Invalid offset retrieved, skipping execution.")

        # Use HTTP Hook to get connection details
        http_hook = HttpHook(http_conn_id='nic_api', method='GET')

        # Build the API endpoint dynamically
        base_url = "http://164.100.251.114/ssd/ssd/beneficiary-pension-data-400"
        endpoint = f'/nic-data?url={base_url}&count={COUNT}&offset={offset}'

        # Make the request via the HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            if df.empty:
                raise AirflowFailException("DataFrame is empty. Failing the task.")

            mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")

            try:
                # Load data into MySQL
                with mysql_hook.get_sqlalchemy_engine().begin() as conn:
                    df.to_sql(
                        table_name,
                        con=conn,
                        if_exists='append',
                        index=False,
                        chunksize=10000,
                        schema='bipard_staging'
                    )
                    print(f"Inserted {len(df)} rows successfully into {table_name}")

                # Update the offset in table_status
                update_query = text(
                    "UPDATE bipard_staging.table_status SET offset_of_table = offset_of_table + :rows WHERE table_name = :table"
                )
                with mysql_hook.get_sqlalchemy_engine().begin() as conn:
                    conn.execute(update_query, {"rows": len(df), "table": table_name})

            except Exception as e:
                print(f"Error loading data to {table_name}: {str(e)}")
                raise

        else:
            raise AirflowFailException(f"Failed to fetch data: HTTP {response.status_code}")

    # Define task dependencies
    t_offset = get_offset('ela_bharti')
    extract_data(t_offset, 'ela_bharti')
