# Product Price Tracker Data Pipeline

This project uses Docker and Airflow to manage data pipelines for product price tracking.

## Setup

### .env Configuration

```bash
# User ID for Airflow (important for Linux users)
AIRFLOW_UID=

# Azure Storage connection string (for ADLS integration)
AZURE_STORAGE_CONNECTION_STRING=
```

## Running the Project

```bash
# Initialize Airflow (first time only)
docker-compose up airflow-init

# Start all services
docker-compose up -d
```

Access the Airflow web interface at: http://localhost:8080

## Adding New Python Dependencies

We use a custom Dockerfile to manage dependencies, making it easy to add new packages.

### Simple Process for Adding Dependencies

1. Add the package to `requirements.txt`
2. Rebuild and restart the containers:
   ```bash
   docker-compose build && docker-compose up -d
   ```

Example of adding a dependency:

1. Edit requirements.txt and add: `scrapy==2.11.0`
2. Run: `docker-compose build && docker-compose up -d`

## Troubleshooting

### Module Not Found Errors

If you encounter a "ModuleNotFoundError" in your DAGs or scrapers:

1. Make sure the package is in `requirements.txt`
2. Rebuild the Docker containers:
   ```bash
   docker-compose build && docker-compose up -d
   ```
3. Check if the package was installed correctly:

   ```bash
   docker-compose exec airflow-worker pip list | grep package_name


   docker compose exec airflow-worker bash -c "python /opt/airflow/init_gcp_creds.py"
   ```
