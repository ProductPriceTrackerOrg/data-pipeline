from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="daily_data_ingestion",
    start_date=pendulum.datetime(2025, 8, 22, tz="Asia/Colombo"),
    schedule_interval="@daily",
    catchup=False,
    doc_md="""
    ## Daily Data Ingestion DAG
    This DAG runs the scrapers for all e-commerce sites and initiates the Bronze layer creation.
    """,
    tags=["ingestion", "bronze"],
) as dag:
    # Dummy start task for better visualization
    start_task = BashOperator(
        task_id="start", 
        bash_command="echo 'Starting data ingestion pipeline...'"
    )

    # # Task to run the AppleMe scraper
    # scrape_appleme_task = BashOperator(
    #     task_id="scrape_appleme",
    #     bash_command="cd /opt/airflow && python -c 'import sys; sys.path.append(\"/opt/airflow\"); import asyncio; from scrapers.appleme.run_complete_scrape import run_complete_scrape; asyncio.run(run_complete_scrape())'",
    # )

    # Task to run the SimplyTek scraper
    scrape_simplytek_task = BashOperator(
        task_id="scrape_simplytek",
        bash_command="cd /opt/airflow/scrapers/simplytek && python -u main.py",
    )

    # Dummy end task for better visualization
    end_task = BashOperator(
        task_id="end", 
        bash_command="echo 'Data ingestion pipeline completed successfully.'"
    )

    # Set the dependencies
    # start_task >> [scrape_appleme_task, scrape_simplytek_task] >> end_task
    start_task >> scrape_simplytek_task >> end_task
