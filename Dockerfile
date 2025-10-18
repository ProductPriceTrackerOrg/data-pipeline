FROM apache/airflow:2.9.2

USER root

# Copy project-specific dependencies and adjust ownership so the airflow user can install them.
COPY requirements.txt /tmp/requirements.txt
RUN chown airflow:0 /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir --requirement /tmp/requirements.txt