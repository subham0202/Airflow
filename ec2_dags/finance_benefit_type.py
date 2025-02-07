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
    'owner': 'Harsh',
    'start_date': days_ago(5),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define constants
count = 1000  # Number of rows to fetch

# Create the DAG
with DAG(
    dag_id='finance_benefit_type',
    default_args=default_args,
    schedule_interval='*/2 * * * *',
    max_active_runs=1,
    catchup=False,
) as dag:

    @task()
    def get_offset(name: str) -> int:
        """
        Fetch the offset value from the MySQL table.
        """
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name='{name}';")
                )
                offset = result.scalar()  # Fetch the scalar value
            if offset is None:
                logging.warning(f"No offset found for table {name}. Using default offset: 0")
                return 0
            logging.info(f"Offset for table {name}: {offset}")
            return offset
        except Exception as e:
            logging.error(f"Error fetching offset for table {name}: {e}. Using default offset: 0")
            return 0

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def extract_data(offset: int):
        """
        Extract data from the API.
        """
        if offset is None:
            raise AirflowException("Offset is None. Failing the task.")

        http_hook = HttpHook(http_conn_id='nic_api', method='GET')

        # Define the endpoint
        endpoint = f'nic-data?url=http://164.100.251.114/finance/finance/edbt/benefit-type&count={count}&offset={offset}'
        logging.info(f"Making request to endpoint: {endpoint}")

        # Perform the HTTP request
        response = http_hook.run(endpoint, extra_options={"timeout": 600})

        # Handle the response
        logging.info(f"HTTP Response Status Code: {response.status_code}")
        logging.info(f"Response Content: {response.text}")

        if response.status_code == 200:
            return response.json()
        else:
            raise AirflowException(f"HTTP Error {response.status_code}: {response.text}")

    @task()
    def transform_data(data):
        """
        Transform the extracted data into a DataFrame.
        """
        logging.info("Transforming data...")
        df = pd.DataFrame(data)
        logging.info(f"Transformed DataFrame:\n{df.head()}")
        return df

    @task()
    def load_data(df, table_name: str):
        """
        Load the transformed data into the MySQL table.
        """
        if df.empty:
            raise AirflowException("DataFrame is empty. Failing the task.")

        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()

        try:
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

            # Update the offset in the table_status table
            query = text(
                f"UPDATE bipard_staging.table_status SET offset_of_table=offset_of_table+{int(len(df))} WHERE table_name='{table_name}';"
            )
            logging.info(f"Executing query: {query}")

            with engine.begin() as conn:
                conn.execute(query)
        except Exception as e:
            logging.error(f"Error loading data to {table_name}: {e}")
            raise

    # Define task dependencies
    offset = get_offset('finance_benefit_type')  # Change the table name as required
    extracted_data = extract_data(offset=offset)
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data, 'finance_benefit_type')

