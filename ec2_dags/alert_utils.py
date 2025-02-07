from airflow.utils.email import send_email
from airflow.models import Variable

def task_failure_alert(context):
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    subject = f"Airflow Alert Task Failure:  in DAG {dag_id}"

    html_content = f"""
    <h3>Task Failed</h3>
    <p><b>Task:</b> {task_id}</p>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p>View logs: <a href="http://ec2-3-110-132-234.ap-south-1.compute.amazonaws.com:8080">{log_url}</a></p>
    """

    # Fetch email recipients from Airflow Variables
    email_recipients = Variable.get("alert_email_recipients")

    send_email(to=email_recipients, subject=subject, html_content=html_content)
