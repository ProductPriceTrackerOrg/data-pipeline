# ./Dockerfile (Corrected)
FROM apache/airflow:2.9.2

# Copy the requirements file and ensure the 'airflow' user owns it.
COPY --chown=airflow:root requirements.txt /requirements.txt

# Explicitly switch to the airflow user for clarity.
USER airflow

# Install packages using the --user flag. This installs packages for the
# current user (airflow) without needing root permissions, which is what the image expects.
RUN pip install --no-cache-dir --user -r /requirements.txt