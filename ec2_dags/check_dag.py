from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 1),
}

def check_table_names():
    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn_id")
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    for table in tables:
        print(table)

with DAG(
    dag_id="check_mysql_rds_tables",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    check_task = PythonOperator(
        task_id="check_table_names",
        python_callable=check_table_names,
    )
