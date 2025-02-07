from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from sqlalchemy import text

# Constants
COUNT = 1000

category_mapping = {
    # Abbreviations to English
    "GEN": "General",
    "ST": "Scheduled Tribe",
    "EBC": "Extremely Backward Class",
    "SC": "Scheduled Caste",
    "BC": "Backward Class",
    
    # Hindi to English
    "पिछड़ा वर्ग": "Backward Class",
    "सामान्य": "General",
    "अनुसूचित जाति": "Scheduled Caste",
    "अति पिछड़ा वर्ग": "Extremely Backward Class",
    "अनुसूचित जनजाति": "Scheduled Tribe",
    "अल्पसंख्यक": "Minority"
}

GENDER_DICT = {
    1: "Male",
    2: "Female",
    99: "Transgender"
}


crop_type = {
  1:"Kharif",
  2:"Rabi"
}


DISTRICT_MAP = {'203':'Pashchim Champaran',
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

default_args = {
    'owner': 'Vishnu',
    'start_date': days_ago(5)
}

with DAG(dag_id='BRFSY_KHARIF_RABI_transform',
         default_args=default_args,
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False) as dag:

    @task()
    def transform_data():
        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()
        query = fquery="""SELECT  distCode,distName,blockName ,panchayatName ,seasionYear,seasionid ,category_Name ,seasion,gender_Name,count(1) as 'total_farmers',
         sum(totalPaidAmount) as total_amount_paid_in_rupees FROM bipard_staging.Cooperative_BRFSY_KHARIF 
         group by distName ,blockName ,panchayatName ,seasionYear ,seasion,category_Name ,gender_Name
         UNION
         SELECT  distCode,distName,blockName ,panchayatName ,seasionYear,seasionid ,category_Name ,seasion,gender_Name,count(1) as 'total_farmers',
         sum(totalPaidAmount) as total_amount_paid_in_rupees FROM bipard_staging.Cooperative_BRFSY_RABI 
         group by distName ,blockName ,panchayatName ,seasionYear ,seasion,category_Name ,gender_Name"""

        
        with engine.begin() as conn:
            df = pd.read_sql(query, conn)
            if df.empty:
                raise ValueError("No data fetched from the source table.")

            # Apply transformations
            df.rename(columns={'blockName':'block_name','panchayatName':'panchayat_name','gender_Name':'gender'},inplace=True)
            # Apply transformations
            df['scheme_name']='Bihar Rajya Fasal Sahayata Yojna'
            df['focus_area']='Loan given to farmers for crop loss'
            df['category_name'] = df['category_Name'].map(category_mapping)
            df['seasionid'] = df['seasion'].map(crop_type)
            df['district_name'] = df['distCode'].astype(str).map(DISTRICT_MAP)
            df['year'] = df['seasionYear'].astype(str).apply(lambda x: f"20{x[0:2]}-{int(x[-2:])}")
            df['state_name'] = 'Bihar'
            # Drop unnecessary columns
            df.drop(columns=[ 'distName','distCode','category_Name','seasionid','seasionYear'], inplace=True)
        
        
        return df

    @task()
    def load_data(df, table_name):
        if df.empty:
            raise ValueError("DataFrame is empty. Failing the task.")

        mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()

        try:
            with engine.begin() as conn:
                df.to_sql(
                    table_name,
                    con=engine,
                    if_exists='replace',
                    index=False,
                    chunksize=10000,
                    schema='bipard_staging'
                )
                print(f"Inserted {len(df)} rows successfully into {table_name}")
            
            # Update the offset
            query = text(f"UPDATE bipard_staging.table_status SET offset_of_table = offset_of_table + {len(df)} WHERE table_name = '{table_name}';")
            with engine.begin() as conn:
                conn.execute(query)
        except Exception as e:
            print(f"Error loading data to {table_name}: {e}")
            raise


    transformed_data = transform_data()
    load_data(transformed_data, 'BRFSY_KHARIF_RABI_transform')
