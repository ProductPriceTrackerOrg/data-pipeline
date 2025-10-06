from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="daily_data_categorization",
    start_date=pendulum.datetime(2025, 9, 11, tz="UTC"),
    schedule="0 5 * * *",  # This is the key part
    catchup=False,
    doc_md="""
    ## Daily Data Categorization DAG
    This DAG runs the scrapers for all e-commerce sites and initiates the Bronze layer creation.
    """,
    tags=["categorization", "Gold"],
) as dag:
    # Dummy start task for better visualization
    start_task = BashOperator(
        task_id="start", 
        bash_command="echo 'Starting data categorization pipeline...'"
    )


    # Task to categorize data
    categorize_task = BashOperator(
        task_id="categorize",
        bash_command="cd /opt/airflow/product_categorization && python -u main.py",
    )
    
            
    # Dummy end task for better visualization
    end_task = BashOperator(
        task_id="end", 
        bash_command="echo 'Data categorization pipeline completed successfully.'"
    )

    # Set the dependencies

    start_task >> categorize_task >> end_task

