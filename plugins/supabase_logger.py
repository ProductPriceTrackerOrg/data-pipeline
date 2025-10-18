"""
Supabase Pipeline Logger
Logs Airflow pipeline execution data to Supabase
"""

import os
import json
import logging
import datetime
import requests
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.hooks.base import BaseHook
from dotenv import load_dotenv

# Setup logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_API_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.warning("Supabase credentials not found in environment variables")

def log_pipeline_run(dag_run, **kwargs):
    """
    Log pipeline execution data to Supabase
    
    Args:
        dag_run: The DagRun object from Airflow context
    """
    try:
        # Extract required information
        dag_id = dag_run.dag_id
        run_id = dag_run.run_id
        run_date = dag_run.execution_date.isoformat()
        status = dag_run.state
        
        # Calculate duration
        if dag_run.end_date and dag_run.start_date:
            duration_seconds = int((dag_run.end_date - dag_run.start_date).total_seconds())
        else:
            duration_seconds = 0
        
        # Count failed tasks
        tasks_failed = 0
        error_message = None
        
        # Get all task instances for this run
        task_instances = dag_run.get_task_instances()
        for ti in task_instances:
            if ti.state == State.FAILED:
                tasks_failed += 1
                # Get the first error message
                if not error_message and ti.error:
                    error_message = str(ti.error)[:500]  # Truncate long messages
        
        # Prepare data for Supabase
        pipeline_data = {
            "dag_id": dag_id,
            "run_id": run_id,
            "run_date": run_date,
            "status": status,
            "duration_seconds": duration_seconds,
            "tasks_failed": tasks_failed,
            "error_message": error_message,
            "run_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
        # Send data to Supabase
        headers = {
            "apikey": SUPABASE_KEY,
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/pipelinerunlog",
            headers=headers,
            json=pipeline_data
        )
        
        if response.status_code >= 300:
            logger.error(f"Failed to log pipeline run to Supabase: {response.text}")
        else:
            logger.info(f"Successfully logged pipeline run to Supabase: {dag_id} - {run_id}")
            
    except Exception as e:
        logger.error(f"Error logging pipeline run to Supabase: {str(e)}")