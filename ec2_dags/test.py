from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from alert_utils import task_failure_alert


def testing_email_notify():
    a = 3 + '4'
    return a

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    # 'email': ['negisubham1999@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'on_failure_callback': task_failure_alert
    # 'retries': 1,
    # 'retry_delay': timedelta(seconds=3)
}


with DAG('email_notification_testing',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        tsk_email_on_retry_on_fail = PythonOperator(
            task_id= 'tsk_email_on_retry_on_fail',
            python_callable=testing_email_notify
            )