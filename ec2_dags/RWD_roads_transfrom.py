from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from sqlalchemy import text
import pandas as pd
import logging

COUNT = 1000

default_args = {
    'owner': 'Nitish',
    'start_date': days_ago(5)
}

with DAG(dag_id='rwd_roads_transform',
         default_args=default_args,
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False) as dags:
    
    @task()
    def get_offset(name: str) -> int:
        # Initialize the MySQL hook
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        # Execute the query to get offset
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name='{name}';"))
                offset = result.scalar()  # Fetch the scalar value
            return offset
        except Exception as e:
            logging.error(f"Error fetching offset from table {name}: {e}")
            return -1

    @task()
    def transform_data(offset: int) -> pd.DataFrame:
        """Fetch and transform data from the source table."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        query = f"SELECT * FROM bipard_staging.rwd_roads_data LIMIT {COUNT} OFFSET {offset}"
        try:
            df = pd.read_sql(query, engine)
            logging.info(f"Fetched {len(df)} rows for transformation.")

            # Drop unnecessary columns
            columns_to_drop = ['Wing', 'Project_Id', 'NameOfContractor']
            df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

            # Rename columns
            df.rename(columns={
                'Circle': 'circle_name',
                'Division': 'division_name',
                'District': 'district_name',
                'Block': 'block_name',
                'RoadName': 'road_name',
                'LENGTH': 'length_in_kilometers',
                'SchemeName': 'scheme_name',
                'WorkStatusName': 'work_status',
                'PDOC': 'year'
            }, inplace=True)

            # Extract year from the 'year' column
            if 'year' in df.columns:
                df['year'] = df['year'].str[-4:]

            # Add new columns
            df['focus_area'] = 'Overview of roads in Bihar under RWD'
            df['state_name'] = 'Bihar'

            logging.info("Transformation completed successfully.")
            return df
        except Exception as e:
            logging.error(f"Error during data transformation: {str(e)}")
            raise

    @task()
    def load_data(df, table_name):
        if df.empty:
            raise Exception("DataFrame is empty. Failing the task.")

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
                    schema='bipard_staging'
                )
            logging.info(f"Inserted {len(df)} rows successfully into {table_name}")
            query = text(f"UPDATE bipard_staging.table_status SET offset_of_table=offset_of_table+{len(df)} WHERE table_name='{table_name}';")
            with engine.begin() as con:
                con.execute(query)
        except Exception as e:
            logging.error(f"Error loading data to {table_name}: {str(e)}")
            raise

    offset = get_offset('rwd_roads_transform')  # Update table name here
    trans_data = transform_data(offset=offset)
    load_data(trans_data, 'rwd_roads_transform')  # Update table name here
