from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import pandas as pd
import json

count = 1000  # Number of records per API call

default_args = {
    'owner': 'Nitish',
    'start_date': days_ago(5)
}

with DAG(dag_id='industry_data',
         default_args=default_args,
         schedule_interval='*/2 * * * *',  # Runs every 2 minutes
         max_active_runs=1,
         catchup=False) as dag:

    @task()
    def get_offset(name: str) -> int:
        """Fetches the last processed offset for the given table."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name='{name}';"))
                offset = result.scalar()  # Fetch the scalar value of the offset
            return offset if offset is not None else 0  # Default to 0 if no offset is found
        except Exception as e:
            print(f"Error fetching offset for table {name}: {e}")
            return 0  # Default to 0 on error
        finally:
            engine.dispose()  # Ensure the engine is disposed of properly

    @task()
    def extract_data(offset: int, dropdown_type: str):
        """Calls the API to fetch data based on the dropdown type and offset."""
        http_hook = HttpHook(http_conn_id='industries_api', method='POST')

        # Build the API request payload
        payload = json.dumps({"dropdownType": dropdown_type, "offset": offset, "count": count})

        response = http_hook.run(
            endpoint="/v2/incentive11/get-dropdown-values",
            data=payload,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            data = response.json()
            return data.get('data', [])  # Extract 'data' from the response
        else:
            raise Exception(f"Failed to fetch {dropdown_type}: {response.status_code}, {response.text}")

    @task()
    def transform_data(data):
        """Transform raw data into a DataFrame."""
        if data:
            df = pd.DataFrame(data)
            print(f"Data Preview:\n{df.head()}")
            return df
        else:
            raise ValueError("No data received, cannot proceed with transformation.")

    @task()
    def load_data(df, table_name):
        """Load transformed data into MySQL."""
        if df.empty:
            raise ValueError("DataFrame is empty. Cannot insert into database.")

        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")

        # Connection pooling with SQLAlchemy
        engine = create_engine(
            mysql_hook.get_uri(),
            poolclass=QueuePool,  # Use QueuePool for connection pooling
            pool_size=10,          # Max 10 connections in pool
            max_overflow=20        # Allow 20 extra connections
        )

        try:
            with engine.begin() as conn:  # Ensures connection cleanup
                # Insert data into the specified MySQL table (append mode)
                df.to_sql(
                    table_name,
                    con=conn,  # Use connection instead of engine
                    if_exists='replace',  # Append data to avoid overwriting
                    index=False,
                    chunksize=10000,  # Insert data in chunks
                    schema='bipard_staging'
                )
                print(f"Inserted {len(df)} rows successfully into {table_name}")

                # Update the offset in the table_status
                query = text(f"UPDATE bipard_staging.table_status SET offset_of_table = offset_of_table + {len(df)} WHERE table_name = '{table_name}';")
                conn.execute(query)  # Use the same connection for executing update query

        except Exception as e:
            print(f"Error loading data into {table_name}: {str(e)}")
            raise
        finally:
            engine.dispose()  # Ensure the engine is disposed of properly

    # Task dependencies for industries data
    offset_investment = get_offset('industry_investment')  # Replace with correct table name
    offset_employment = get_offset('industry_employment')  # Replace with correct table name

    investment_data = extract_data(offset=offset_investment, dropdown_type="investment_data")
    employment_data = extract_data(offset=offset_employment, dropdown_type="employment_data")

    trans_investment_data = transform_data(investment_data)
    trans_employment_data = transform_data(employment_data)

    load_data(trans_investment_data, 'industry_investment')  # Change the table name as needed
    load_data(trans_employment_data, 'industry_employment')  # Change the table name as needed
