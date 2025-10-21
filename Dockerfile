FROM apache/airflow:2.9.2

USER root

# Copy project-specific dependencies and adjust ownership so the airflow user can install them.
COPY requirements.txt /tmp/requirements.txt
RUN chown airflow:0 /tmp/requirements.txt

USER airflow

ENV PIP_DEFAULT_TIMEOUT=1200

RUN pip install --no-cache-dir --timeout=1200 --retries 5 --requirement /tmp/requirements.txt