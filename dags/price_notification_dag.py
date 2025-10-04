"""
Airflow DAG to schedule the price notification service.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Import the main function from our notification service
from notification_service.main import run_notification_service

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'price_change_notification',
    default_args=default_args,
    description='A DAG to send price change notifications to users',
    schedule_interval='0 10 * * *',  # Run at 10:00 AM every day
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['price_pulse', 'notification'],
)

# Create a task to run the notification service
run_notification_task = PythonOperator(
    task_id='send_price_notifications',
    python_callable=run_notification_service,
    dag=dag,
)

# The task doesn't have any dependencies, so we don't need to set any.