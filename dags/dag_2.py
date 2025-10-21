from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from plugins.supabase_logger import log_pipeline_run

with DAG(
    dag_id="daily_data_2",
    start_date=pendulum.datetime(2025, 9, 11, tz="UTC"),
    schedule="0 1 * * *",  # This is the key part
    catchup=False,
    doc_md="""
    ## Daily Data Ingestion DAG
    This DAG runs the scrapers for all e-commerce sites and initiates the Bronze layer creation.
    """,
    tags=["ingestion", "bronze"],
    on_success_callback=log_pipeline_run,  # Add this line to log successful runs
    on_failure_callback=log_pipeline_run,  # Add this line to log failed runs
) as dag:
    # Dummy start task for better visualization
    start_task = BashOperator(
        task_id="start", 
        bash_command="echo 'Starting data ingestion pipeline...'"
    )

    # Task to run the AppleMe scraper
    # scrape_appleme_task = BashOperator(
    #     task_id="scrape_appleme",
    #     bash_command="cd /opt/airflow/scrapers/appleme && python -u turbo_scraper.py",
    # )

    # # Task to run the SimplyTek scraper
    # scrape_simplytek_task = BashOperator(
    #     task_id="scrape_simplytek",
    #     bash_command="cd /opt/airflow/scrapers/simplytek && python -u main.py",
    # )

    # Task to run the Onei.lk scraper
    # scrape_onei_task = BashOperator(
    #     task_id="scrape_onei",
    #     bash_command="cd /opt/airflow/scrapers/Onei.lk && python -u main.py",
    # )

    # Task to run the lifeMobile scraper
    # scrape_lifemobile_task = BashOperator(
    #     task_id="scrape_lifemobile",
    #     bash_command="cd /opt/airflow/scrapers/lifeMobile && python -u main.py",
    # )

    # lifemobile scraper is currently down due to site issues scrapers\lifemobile_parallel_running\main.py
    scrape_lifemobile_task = BashOperator(
        task_id="scrape_lifemobile",
        bash_command="cd /opt/airflow/scrapers/lifemobile_parallel_running && python -u main.py",
    )

    # # Task to run the CyberDeals scraper
    # scrape_cyberdeals_task = BashOperator(
    #     task_id="scrape_cyberdeals",
    #     bash_command="cd /opt/airflow/scrapers/cyberdeals && python -u main.py",
    # )
    
    # Task to run the laptops.lk scraper
    # scrape_laptoplk_task = BashOperator(
    #     task_id="scrape_laptoplk",
    #     bash_command="cd /opt/airflow/scrapers/laptoplk && python -u main.py",
    # )
    
    # Delay task (20 seconds)
    # delay_task = BashOperator(
    #     task_id="delay",
    #     bash_command="sleep 10",
    # )

    # # environment setup task docker compose exec airflow-worker bash -c "python /opt/airflow/init_gcp_creds.py" 
    # setup_env_task = BashOperator(
    #     task_id="setup_env",
    #     bash_command="python /opt/airflow/init_gcp_creds.py",
    # )

    # # Task to load data into staging tables
    # load_staging_task = BashOperator(
    #     task_id="load_staging",
    #     bash_command="cd /opt/airflow/transformations/loading/bigquery && python -u loader.py",
    # )
    
    # # Transformation tasks
    # transform_dim_date_task = BashOperator(
    #     task_id="transform_dim_date",
    #     bash_command="cd /opt/airflow/transformations/warehouse/dimensions && python -u dim_date.py",
    # )

    # transform_dim_shop_task = BashOperator(
    #     task_id="transform_dim_shop",
    #     bash_command="cd /opt/airflow/transformations/warehouse/dimensions && python -u dim_shop.py",
    # )

    # transform_dim_shop_product_task = BashOperator(
    #     task_id="transform_dim_shop_product",
    #     bash_command="cd /opt/airflow/transformations/warehouse/dimensions && python -u dim_shop_product.py",
    # )
    
    # transform_dim_variant_task = BashOperator(
    #     task_id="transform_dim_variant",
    #     bash_command="cd /opt/airflow/transformations/warehouse/dimensions && python -u dim_variant.py",
    # )

    # transform_dim_product_image_task = BashOperator(
    #     task_id="transform_dim_product_image",
    #     bash_command="cd /opt/airflow/transformations/warehouse/dimensions && python -u dim_product_image.py",
    # )

    # transform_fact_product_price_task = BashOperator(
    #     task_id="transform_fact_product_price",
    #     bash_command="cd /opt/airflow/transformations/warehouse/facts && python -u fact_product_price.py",
    # )

    # # Task to categorize data
    # categorize_task = BashOperator(
    #     task_id="categorize",
    #     bash_command="cd /opt/airflow/product_categorization && python -u main.py",
    # )

    # # Task to match products product_matching\daily_main.py
    # matching_task = BashOperator(
    #     task_id="match_products",
    #     bash_command="cd /opt/airflow/product_matching && python -u daily_main.py",
    # )

    # # GNN prediction task GNN_training\prediction_main.py
    # gnn_prediction_task = BashOperator(
    #     task_id="gnn_prediction",
    #     bash_command="cd /opt/airflow/GNN_training && python -u prediction_main.py",
    # )

    # # price forecasting  priceforecasting\main.py
    # price_forecasting_task = BashOperator(
    #     task_id="price_forecasting",
    #     bash_command="cd /opt/airflow/priceforecasting && python -u main.py",
    # )

    # # anomaly detection  anomaly_detection\main.py
    # anomaly_detection_task = BashOperator(
    #     task_id="anomaly_detection",
    #     bash_command="cd /opt/airflow/anomaly_detection && python -u main.py",
    # )

    # # notification service notification_service\main.py
    # notification_service_task = BashOperator(
    #     task_id="notification_service",
    #     bash_command="cd /opt/airflow/notification_service && python -u main.py",
    # )

    
            
    # # Dummy end task for better visualization
    # end_task = BashOperator(
    #     task_id="end", 
    #     bash_command="echo 'Data ingestion pipeline completed successfully.'"
    # )

    # Set the dependencies

    start_task >> [
                    # scrape_appleme_task, 
                    # scrape_simplytek_task, 
                    # scrape_onei_task, 
                    scrape_lifemobile_task,  
                    # scrape_laptoplk_task
                    # scrape_cyberdeals_task 
                    ]
    # ] >> delay_task >> setup_env_task >> load_staging_task >> [
    #                                         transform_dim_date_task,
    #                                         transform_dim_shop_task,
    #                                         transform_dim_shop_product_task,
    #                                         transform_dim_variant_task,
    #                                         transform_dim_product_image_task,
    #                                         transform_fact_product_price_task,
    #                                         ] >> end_task

