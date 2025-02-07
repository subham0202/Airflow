from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import requests
import psycopg2
import json
from typing import Dict, Any

# Database connection parameters


def get_db_hook():
    """Get MySQL connection hook"""
    return MySqlHook(mysql_conn_id='mysql_conn_id_staging')


def get_table_offset(table_name: str) -> int:
    """Get current offset for the specified table"""
    hook = get_db_hook()
    result = hook.get_first(
        "SELECT offset_of_table FROM table_status WHERE table_name = %s",
        parameters=(table_name,)
    )
    return result[0] if result else 0

def update_table_offset(table_name: str, new_offset: int):
    """Update offset for the specified table"""
    hook = get_db_hook()
    hook.run(
        "UPDATE table_status SET offset_of_table = %s WHERE table_name = %s",
        parameters=(new_offset, table_name)
    )




def create_data_table(table_name: str, data: Dict[str, Any]):
    """Create data table based on JSON structure with all columns as TEXT."""
    # Extract column names and set all columns as TEXT
    columns = [f"{key} TEXT" for key in data.keys()]
    
    # Generate the SQL CREATE TABLE statement
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"        
    
    hook = get_db_hook()
    hook.run(create_table_sql)





def insert_data(table_name: str, data_list: list):
    """Insert data into specified table"""
    if not data_list:
        return 0
    
    # Generate dynamic INSERT statement
    columns = data_list[0].keys()
    placeholders = ', '.join(['%s'] * len(columns))
    insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
    """
    
    values = [tuple(str(value) for value in record.values()) for record in data_list]
    
    hook = get_db_hook()
    hook.run(insert_sql, parameters=values)
    return len(data_list)


def get_auth_token() -> str:
    """
    Get authentication token from the API
    
    Returns:
        str: Authentication token
    """

    
    AUTH_URL = 'https://startup.bihar.gov.in/api/v1/startup-bihar-open-api/get-token'
    
    # Ensure content is properly formatted as form data
    auth_payload = "username=StartUpBihar"
    
    try:
        response = requests.post(
            url=AUTH_URL,
            data=auth_payload,  # Send as form data
            headers={
                'Content-Type': 'application/x-www-form-urlencoded'  # Set correct content type
            },
            verify=False
        )
        response.raise_for_status()
        
        # Print response for debugging
        # print(f"Response status: {response.status_code}")
        # print(f"Response content: {response.text}")
        
        # Parse token from response
        token_data = response.json()
        return token_data.get('data')
        
    except requests.exceptions.RequestException as e:
        print(f"Error getting authentication token: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing token response: {e}")
        raise

def fetch_and_store_data(table_name, data_type: str = "district") -> Dict[str, Any]:
    """
    Main function to fetch data from API using authentication and store in database
    
    Args:
        table_name (str): Name of the table to store data
        **context: Additional context parameters
        
    Returns:
        Dict[str, Any]: Response data from the API
    """
    current_offset = get_table_offset(table_name)
    # API configuration
    API_URL = 'https://startup.bihar.gov.in/api/v1/startup-bihar-open-api'
    BATCH_SIZE = 1000
    
    try:
        # Get authentication token
        token = get_auth_token()
        
        # Request parameters
        form_data = {
            'token': token,
            'type': data_type
        }
        
        # Make authenticated POST request
        response = requests.post(
            url=API_URL,
            data=form_data,
            verify=False
        )
        response.raise_for_status()
        
        # Parse response data
        data = response.json()
        data=data.get('data')
        if not data:
            return "No new data to process"
    
    # Create table if it doesn't exist (using first record as schema)
        create_data_table(table_name, data[0])


        inserted_count = insert_data(table_name, data)
        new_offset = current_offset + inserted_count
        update_table_offset(table_name, new_offset)
        
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    # 'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'start_up_bihar_district_wise',
    default_args=default_args,
    description='Pipeline to fetch and store API data',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False
)

# Create tables operator

# Fetch and store data operator
fetch_store_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_and_store_data,
    op_kwargs={'table_name': 'start_up_bihar_district_wise'},
    dag=dag
)

# Set task dependencies
fetch_store_data