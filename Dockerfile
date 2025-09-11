
# ./Dockerfile

FROM apache/airflow:2.9.2

# Switch to the root user to install system packages or pip packages globally
USER root

# Copy your requirements file
COPY requirements.txt .

# Install python dependencies with a longer timeout and no cache
RUN pip install --no-cache-dir --timeout=100 -r requirements.txt

# Switch back to the non-root airflow user
USER airflow
