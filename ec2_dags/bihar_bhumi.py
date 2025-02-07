from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
import json
import pandas as pd
from sqlalchemy import text


default_args = {
    'owner': 'Subham',
    'start_date': days_ago(5)
}

with DAG(dag_id='land_records',
         default_args=default_args,
         schedule_interval='* * * * *',
         max_active_runs=1,
         catchup=False) as dag:

    @task()
    def get_offset() -> int:
        """Fetches the next village LGD code where `is_completed` is NULL."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        try:
            with mysql_hook.get_sqlalchemy_engine().begin() as conn:
                result = conn.execute(text("SELECT code FROM bipard_staging.mauja_code WHERE is_completed IS NULL LIMIT 1"))
                offset = result.scalar()
            return offset if offset else -1
        except Exception as e:
            print(f"Error fetching village LGD code: {e}")
            return -1

    @task()
    def extract_data(village_lgd_code: int, table_name: str):
        """Extracts land records data from NIC API and loads it into MySQL."""
        if village_lgd_code == -1:
            raise AirflowFailException("No pending village LGD code found.")

        # Initialize HTTP and MySQL Hooks
        http_hook = HttpHook(http_conn_id='bihar_bhumi', method='POST')
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")

        payload = {
            "User": "Admin",
            "Password": "LrcBih@JM_Data",
            "village_lgd_code": village_lgd_code
        }
        endpoint = '/GetUserApi/api/GetMaujaWiseJamabandi'

        # Make API Request
        response = http_hook.run(endpoint, data=json.dumps(payload), headers={"Content-Type": "application/json"})

        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}")

        data = response.json().get('Jamabandi_Details', [])

        df = pd.DataFrame(data)
        if df.empty:
            query = text("UPDATE bipard_staging.mauja_code SET is_completed = :status WHERE code = :code")
            with mysql_hook.get_sqlalchemy_engine().begin() as conn:
                conn.execute(query, {"status": "Empty data", "code": village_lgd_code})
            raise AirflowFailException("DataFrame is empty. Marking LGD code as completed.")

        # Convert specific columns to string
        for col in ['Owner_Details', 'Plot_Details', 'Lagaan_Details']:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Insert data into MySQL
        try:
            with mysql_hook.get_sqlalchemy_engine().begin() as conn:
                df.to_sql(table_name, con=conn, if_exists='append', index=False, chunksize=10000)
                print(f"Inserted {len(df)} rows into {table_name}")

                # Update mauja_code table
                query = text("UPDATE bipard_staging.mauja_code SET is_completed = :status WHERE code = :code")
                conn.execute(query, {"status": "Yes", "code": village_lgd_code})

        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
            raise AirflowFailException(f"Failed to load data for LGD code {village_lgd_code}")

    # Define task dependencies
    lgd_code = get_offset()
    extract_data(lgd_code, 'bihar_bhumi')
