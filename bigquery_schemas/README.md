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
		- Authenticate with Google Cloud:
			```bash
			gcloud auth login
			```
		- Set your project:
			```bash
			gcloud config set project price-pulse-470211
			```
		- Run the deployment script using Bash or Git Bash:
			```bash
			bash bigquery_schemas/deploy_schemas.sh
			```
		- Or run individual files using the BigQuery CLI:
			```bash
			bq query --use_legacy_sql=false < bigquery_schemas/DimShop.sql
			```

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
