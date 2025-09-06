# PowerShell script to deploy all BigQuery schema files for the data warehouse

$PROJECT_ID = "price-pulse-470211"
$DATASET = "warehouse"

Write-Host "Deploying BigQuery warehouse schemas to $PROJECT_ID.$DATASET..."

Get-ChildItem -Path "*.sql" | ForEach-Object {
    Write-Host "Deploying $($_.Name)..."
    $content = Get-Content $_.FullName -Raw
    $content | bq query --project_id="$PROJECT_ID" --use_legacy_sql=false --format=none
}

Write-Host "All schemas deployed successfully!"
