# Use official Airflow image as base
FROM apache/airflow:2.9.2

# Switch to root to install system packages
USER root

# Install build dependencies for selectolax
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
