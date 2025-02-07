from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta

default_args = {
    'owner': 'Nitish',
    'start_date': days_ago(5),
}

def update_table(table_name,offset):
    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
    engine = mysql_hook.get_sqlalchemy_engine()
    update_query = text(f"""
                    UPDATE bipard_staging.table_status 
                    SET offset_of_table = '{offset}' 
                    WHERE table_name = '{table_name}';
                """)
    with engine.begin() as conn:
        conn.execute(update_query)
        print(f"Updated status table for date: {offset}")    





with DAG(
    dag_id='Bmsk_weather',
    default_args=default_args,
    schedule_interval='*/2 * * * *',  # Runs every 2 minutes
    max_active_runs=1,
    catchup=False
) as dag:

    @task()
    def get_last_date() -> str:
        """Fetch the last processed date from the table_status table."""
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()

        query = """
            SELECT offset_of_table FROM bipard_staging.table_status WHERE table_name='weather_bmsk';
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            last_date = result.scalar()
        if last_date:
            return str(last_date).zfill(8)
        else:
            return "0"

    @task()
    def generate_next_date(last_date: str) -> str:
        """Generate the next date in DDMMYYYY format from the given last_date."""
        last_date_dt = datetime.strptime(last_date, "%d%m%Y")
        next_date_dt = last_date_dt + timedelta(days=1)
        
        if next_date_dt > datetime.now():
            raise AirflowFailException("Next date is beyond today. Stopping the task.")
        
        next_date_str = next_date_dt.strftime("%d%m%Y")
        return next_date_str

    @task()
    def extract_and_load(date: str):




        """Fetch and load weather data for the given date."""
        # First, update the date in status table
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        
        
        # Now fetch the data
        http_hook = HttpHook(http_conn_id='nic_api', method='GET')
        endpoint = f"/nic-data?url=https://www.mausamsewa.bihar.gov.in/BMSK_API/api/DAILYDATA/{date}"
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            if df.empty:
                update_table('weather_bmsk',date)
                print(f"Empty DataFrame for date: {date}")
                

            try:
                with engine.begin() as conn:
                    df.to_sql(
                        'weather_bmsk',
                        con=conn,
                        if_exists='append',
                        index=False,
                        chunksize=1000,
                        schema='bipard_staging'
                    )
                    print(f"Inserted {len(df)} rows for date {date} into weather_bmsk.")
                    update_table('weather_bmsk',date)
                


            except Exception as e:
                print(f"Error loading data for date {date}: {str(e)}")
                raise
        else:
            print(f"Failed to fetch data for date {date}. Status code: {response.status_code}")
            return

    # DAG execution flow
    last_date = get_last_date()
    next_date = generate_next_date(last_date)
    extract_and_load(next_date)