# Price Change Notification Service

This service monitors price changes in the product data warehouse and sends email notifications to users who have subscribed to those products.

## Overview

The notification service consists of four main components:

1. **BigQuery Query Module**: Queries the BigQuery data warehouse to find products with price changes
2. **PostgreSQL Query Module**: Finds users who have subscribed to products with price changes
3. **Email Service Module**: Sends email notifications to subscribed users using SendGrid
4. **Main Orchestration Module**: Coordinates the entire notification process

## Requirements

- Python 3.8+
- Google Cloud BigQuery
- PostgreSQL database
- SendGrid account for sending emails

## Installation

1. Install the required packages:

```
pip install -r requirements.txt
```

2. Set up environment variables:

```
# BigQuery credentials
PROJECT_ID=your-project-id
WAREHOUSE=your-warehouse-dataset-name

# PostgreSQL database credentials
DB_NAME=your-db-name
DB_USER=your-db-user
DB_PASSWORD=your-db-password
DB_HOST=your-db-host
DB_PORT=your-db-port

# SendGrid credentials
SENDGRID_API_KEY=your-sendgrid-api-key
SENDER_EMAIL=your-sender-email
```

## Usage

To run the notification service manually:

```bash
python -m notification_service.main
```

## Testing

To run the unit tests:

```bash
python -m unittest discover -s notification_service/tests
```

## Architecture

- **bigquery_queries.py**: Contains the `get_price_changes()` function that queries BigQuery to find products with price changes
- **postgres_queries.py**: Contains the `get_subscribed_users()` function that queries PostgreSQL to find subscribed users
- **email_service.py**: Contains the `send_notification_email()` function that sends email notifications using SendGrid
- **main.py**: Contains the `run_notification_service()` function that orchestrates the entire process

## Notes

- This service is designed to be run as an Airflow DAG task.
- The service tracks price changes at the variant level in the data warehouse, but users track products (variants) in the operational database.
- The service checks for price changes by comparing the latest price with the immediately preceding price for each variant.
- The service sends email notifications only to users who have enabled price drop notifications in their settings.
