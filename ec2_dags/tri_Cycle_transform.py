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


count=1000

district_map={'203':'Pashchim Champaran',
'204':'Purbi Champaran',
'205':'Sheohar',
'206':'Sitamarhi',
'207':'Madhubani',
'208':'Supaul',
'209':'Araria',
'210':'Kishanganj',
'211':'Purnia',
'212':'Katihar',
'213':'Madhepura',
'214':'Saharsa',
'215':'Darbhanga',
'216':'Muzaffarpur',
'217':'Gopalganj',
'218':'Siwan',
'219':'Saran',
'220':'Vaishali',
'221':'Samastipur',
'222':'Begusarai',
'223':'Khagaria',
'224':'Bhagalpur',
'225':'Banka',
'226':'Munger',
'227':'Lakhisarai',
'228':'Sheikhpura',
'229':'Nalanda',
'230':'Patna',
'231':'Bhojpur',
'232':'Buxar',
'233':'Kaimur (Bhabua)',
'234':'Rohtas',
'235':'Aurangabad',
'236':'Gaya',
'237':'Nawada',
'238':'Jamui',
'239':'Jehanabad',
'240':'Arwal',
}

category_dict = {
    1: "GEN",
    2: "EBC", 
    3: "BC",
    4: "SC", 
    5: "ST",
    6: "EWS",
    7: "BC (W)"
}
gender_dict = {
   2: "Female",
   1: "Male", 
   99: "Transgender"
}



default_args = {
    'owner': 'Subham',
    'start_date': days_ago(5)
}

with DAG(dag_id='tri_cycle_transform',
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
        query=f"select * from bipard_staging.tri_cycle limit {count} offset {offset}"

        with engine.begin() as conn:
            df=pd.read_sql(query,engine)
        df['district_name'] = df['hdistrict_code'].map(district_map)
        df['state_name']='Bihar'
        df['focus_area']='Tri-Cycle Distribution'    
        df['category_name']=df['category_code'].map(category_dict)
        df['gender']=df['gender_code'].map(gender_dict)
        df['physically_handicapped']=df['physically_handicapped'].replace({"Y":"Yes",'N':'No'})
        df['physically_handicapped_catagory']=df['physically_handicapped_catagory'].replace(1,'Locomotor disability')
        df['state_of_domicile']=df['state_of_domicile'].replace({"Y":"Yes",'N':'No'})
        df.drop(columns=['sl_no', 'application_id', 'dob', 'category_code',
                'gender_code', 'isfreedomfighter', 'exserviceman', 'marital_status',
                'p_code', 'isfinal', 'created_by', 'created_on', 'hdistrict_code',
                'other_district','physically_handicapped_percentage'],inplace=True)
            
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
                        con=conn,
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







    offset=get_offset('tri_cycle_transform') #change table name here 
    trans_data=transform_data(offset=offset)
    load_data(trans_data,'tri_cycle_transform')
