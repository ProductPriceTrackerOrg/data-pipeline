#!/bin/bash
# Script to deploy all BigQuery schema files in the bigquery_schemas folder

PROJECT_ID="price-pulse-470211"
DATASET="warehouse"

for file in ./bigquery_schemas/*.sql; do
  echo "Deploying $file..."
  bq query --project_id="$PROJECT_ID" --dataset_id="$DATASET" --use_legacy_sql=false < "$file"
done

echo "All schemas deployed."