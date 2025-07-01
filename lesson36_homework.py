from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG(
    dag_id='send_email',
    start_date=datetime(2025, 6, 27),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    send_email = EmailOperator(
        task_id='send_email_task',
        to='polinakhmysko@gmail.com',
        subject='Airflow Notification',
        html_content='<h3>Тестовое уведомление. Отправка выполнена успешно.</h3>',
    )

send_email