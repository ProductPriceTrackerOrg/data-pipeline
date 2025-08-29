[BigQuery Schemas]

This folder contains the Data Warehouse table definitions for the analytical layer of the project. Each `.sql` file defines a single table in Google BigQuery, following best practices for modularity, version control, and collaboration.

## Structure

- Each table’s schema is in a separate `.sql` file.
- Only fact and dimension tables for the analytical warehouse are included here.
- Operational database tables (Users, Roles, etc.) are managed separately in Supabase.

## Usage

1. **Edit Schemas**
	- Update or add new table definitions in their respective `.sql` files.
	- Use BigQuery-compatible data types (`STRING`, `INT64`, `FLOAT64`, `BOOL`, `DATE`, `TIMESTAMP`).

2. **Deploy Schemas**
	- Use the `deploy_schemas.sh` script to create all tables in your BigQuery dataset:
	  ```sh
	  ./deploy_schemas.sh
	  ```
	- Or run individual files using the BigQuery CLI:
	  ```sh
	  bq query --use_legacy_sql=false < DimShop.sql
	  ```
    1. Authenticate with Google Cloud:
        gcloud auth login

    2. Set your project:
        gcloud config set project your_project_id

    3. Make the script executable:
        chmod +x deploy_schemas.sh

    4. Run the deployment script:
        ./deploy_schemas.sh

3. **Version Control**
	- All schema changes are tracked in Git.
	- Use pull requests for updates to ensure review and collaboration.

## Table List

- DimShop
- DimDate
- DimCategory
- DimModel
- DimCanonicalProduct
- DimVariant
- FactProductPrice
- FactPriceForecast
- FactPriceAnomaly
- FactProductRecommendation

## Best Practices

- Keep each table definition atomic (one file per table).
- Document table and column purposes in comments within each `.sql` file.
- Use consistent naming conventions.
- Automate deployments for reproducibility.
