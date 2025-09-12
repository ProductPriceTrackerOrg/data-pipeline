#!/bin/bash
# Custom entrypoint script for Airflow containers with GCP authentication fix

set -e

# First ensure GCP credentials are properly set up
echo "Checking GCP credentials..."
python /opt/airflow/init_gcp_creds.py

# Then run the original Airflow entrypoint with all arguments
echo "Starting Airflow entrypoint..."
exec /entrypoint "$@"
