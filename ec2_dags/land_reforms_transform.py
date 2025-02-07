from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import json
from sqlalchemy import create_engine, text
import pandas as pd
import ast


count=1000


default_args = {
    'owner': 'Subham',
    'start_date': days_ago(5)
}

with DAG(dag_id='land_reforms_transform',
         default_args=default_args,
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False) as dags:
    
    @task()
    def get_offset(name: str) -> int:
        # Initialize the Postgres hook
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        # Execute the query to count rows
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT offset_of_table FROM bipard_staging.table_status where table_name='{name}';"))
                offset = result.scalar()  # Fetch the scalar value of the count
            return offset
        except Exception as e:
            print(f"Error fetching count from table {name}: {e}")
            return -1


    @task()
    def transform_data(offset):
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        query=f"SELECT District, SubDivision, Circle, Halka, Mauja, MaujaCode, TotalArea, Lagaan_Details FROM bipard_staging.bihar_bhumi limit {count} offset {offset};"


        def extract_tax_amount(row):
            try:
                # Convert string representation of list to actual list of dictionaries
                lagaan_details = ast.literal_eval(row) if isinstance(row, str) else row
                if isinstance(lagaan_details, list) and len(lagaan_details) > 0:
                    return lagaan_details[0].get('Tax_Amount', '0')  # Default to '0' if not found
            except (ValueError, SyntaxError):
                pass  # Handle cases where parsing fails
            return '0'  
        
        
        
        def convert_to_acres(row):
            parts = row.split()  # Split the string into parts
            acres = float(parts[0])  # Extract the acres value
            dismil = float(parts[2])  # Extract the dismil value
            # Convert dismil to acres (1 dismil = 0.01 acres)
            total_acres = acres + (dismil * 0.01)
            return total_acres
        with engine.begin() as conn:
            df=pd.read_sql(query,engine)
            df['Tax_Amount_Extracted'] = df['Lagaan_Details'].apply(extract_tax_amount)
                #  Replace Hindi words with English equivalents
            df['TotalArea'] = df['TotalArea'].str.replace('एकड़', 'acres')
            df['TotalArea'] = df['TotalArea'].str.replace('डिसमील', 'dismil')

                # Remove any unnecessary spaces
            df['TotalArea'] = df['TotalArea'].str.replace(' ', '')
            df['TotalArea'] = df['TotalArea'].str.replace(r'(\d)([a-zA-Z])', r'\1 \2', regex=True)
            df['TotalArea'] = df['TotalArea'].str.replace(r'([a-zA-Z])(\d)', r'\1 \2', regex=True)
            df['total_area_in_acres'] = df['TotalArea'].apply(convert_to_acres)
            df['state_name']='Bihar'
            df['focus_area']='Bihar Land Reforms'
            df.rename(columns={'District':'district_name', 'SubDivision':'sub_divisison_name', 'Circle':'circle_name', 'Halka':'halka_name', 'Mauja':'mauja_name', 'MaujaCode':'mauja_code',
            'Tax_Amount_Extracted':'tax_amount_in_ruppes', 'total_area_in_acres':'total_area_in_acres'},inplace=True)
            #     df.drop(columns={'VolumeNumber', 'PageNumber', 'JamabandiNumber',
        #    'ComputerizedJamabandiNumber', 'TotalArea', 'Lagaan_Details'},inplace=True)
        return df    
            

    @task()
    def load_data(df, table_name):
            if df.empty:
                raise AirflowFailException("DataFrame is empty. Failing the task.")

            mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
            engine = mysql_hook.get_sqlalchemy_engine()
        
            try:
                with engine.begin() as conn:
                    df.to_sql(
                        table_name,
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=10000,
                    schema='bipard_staging'                )
                    print(f"Inserted {len(df)} rows successfully into {table_name}")
                    query = text(f"update bipard_staging.table_status set offset_of_table=offset_of_table+{int(len(df))} where table_name='{table_name}';")
                print(query)
             
                with engine.begin() as con:
                    con.execute(query)
                            
            except Exception as e:
                print(f"Error loading data to {table_name}: {str(e)}")
                raise







    offset=get_offset('land_reforms_transform') #change table name here 
    trans_data=transform_data(offset=offset)
    load_data(trans_data,'land_reforms_transform')
