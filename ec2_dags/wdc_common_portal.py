from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from sqlalchemy import text
from datetime import timedelta
import pandas as pd
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'Gaurav',
    'start_date': days_ago(5),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define constants
base_url = "http://3.7.100.74:3000/api/nic-data?url=http://164.100.251.114/wdc/wdc"
prefix = "wdc_common_portal"
count = 1000  # Number of rows to fetch

# Define the list of API endpoints
apis = [
    "category",
    "districts",
    "eupi-agency",
    "eupi-payment-bill",
    "eupi-payment-bill-details",
    "gender",
    "mst-bank-master",
    "mst-branch-master",
    "mst-exam-type",
    "mst-fin-year",
    "pesonal-details",
]

# Create the DAG
with DAG(
    dag_id='wdc_common_portal',
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
) as dag:

    @task()
    def get_offset(table_name: str) -> int:
        try:
            mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id_staging")
            engine = mysql_hook.get_sqlalchemy_engine()
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name=:table_name"),
                    {"table_name": table_name},
                )
                offset = result.scalar()
            if offset is None:
                logging.warning(f"No offset found for table {table_name}. Using default offset: 0")
                return 0
            logging.info(f"Offset for table {table_name}: {offset}")
            return offset
        except Exception as e:
            logging.error(f"Error fetching offset for table {table_name}: {e}")
            raise AirflowException(f"Error fetching offset for table {table_name}: {e}")

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def extract_data(api_endpoint: str, offset: int):
        try:
            http_hook = HttpHook(http_conn_id='nic_api', method='GET')
            endpoint = f'nic-data?url={base_url}{api_endpoint}&count={count}&offset={offset}'
            logging.info(f"Making request to endpoint: {endpoint}")
            response = http_hook.run(endpoint, extra_options={"timeout": 600})
            logging.info(f"HTTP Response Status Code: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                if not data:
                    logging.warning(f"No data returned from API {api_endpoint} for offset {offset}. Skipping...")
                    return []
                return data
            else:
                raise AirflowException(f"HTTP Error {response.status_code}: {response.text}")
        except Exception as e:
            logging.error(f"Error extracting data from {api_endpoint} with offset {offset}: {e}")
            raise

    @task()
    def transform_data(data):
        try:
            logging.info("Transforming data...")
            if not data:
                logging.warning("Empty data received for transformation. Returning an empty DataFrame.")
                return pd.DataFrame()
            df = pd.DataFrame(data)
            logging.info(f"Transformed DataFrame:\n{df.head()}")
            return df
        except Exception as e:
            logging.error(f"Error transforming data: {e}")
            raise

    @task()
    def load_data(df, table_name: str):
        try:
            if df.empty:
                logging.warning(f"DataFrame is empty for table {table_name}. Skipping load task.")
                return
            mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id_staging")
            engine = mysql_hook.get_sqlalchemy_engine()
            with engine.begin() as conn:
                df.to_sql(
                    table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    chunksize=10000,
                    schema='bipard_staging',
                )
                logging.info(f"Inserted {len(df)} rows successfully into {table_name}")
            query = text(
                f"UPDATE bipard_staging.table_status SET offset_of_table=offset_of_table+:row_count WHERE table_name=:table_name"
            )
            with engine.begin() as conn:
                conn.execute(query, {"row_count": len(df), "table_name": table_name})
        except Exception as e:
            logging.error(f"Error loading data into {table_name}: {e}")
            raise

    for api_endpoint in apis:
        table_name = prefix + "_" + api_endpoint.replace("-", "_")
        offset = get_offset(table_name=table_name)
        extracted_data = extract_data(api_endpoint=api_endpoint, offset=offset)
        transformed_data = transform_data(extracted_data)
        load_data(transformed_data, table_name)
