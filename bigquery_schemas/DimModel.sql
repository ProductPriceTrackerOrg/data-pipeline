-- Table: DimModel
-- Centralizes ML model information for MLOps and model tracking.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimModel` (
  model_id INT64 NOT NULL,                    -- Surrogate key for the model
  model_name STRING NOT NULL,                 -- Name of the ML model
  model_version STRING NOT NULL,              -- Version identifier for the model
  model_type STRING,                          -- Type of model (e.g., 'classification', 'regression')
  training_date DATE,                         -- Date when the model was trained
  performance_metrics_json STRING,            -- JSON string with model performance metrics
  hyperparameters_json STRING                 -- JSON string with model hyperparameters
)
OPTIONS(
  description="Dimension table for ML model metadata and tracking."
);

-- Note: BigQuery does not enforce primary keys or unique constraints.
-- Ensure (model_name, model_version) is unique via ETL logic.
