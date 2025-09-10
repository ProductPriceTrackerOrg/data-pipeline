-- Table: FactPriceForecast
-- Stores the output of the Price Forecasting ML model for product variants.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.FactPriceForecast` (
  forecast_id INT64 NOT NULL,                 -- Surrogate key for the forecast record
  variant_id INT64 NOT NULL,                  -- References DimVariant.variant_id (not enforced)
  model_id INT64 NOT NULL,                    -- References DimModel.model_id (not enforced)
  forecast_date DATE NOT NULL,                -- Date for which the price is forecasted
  predicted_price FLOAT64,              -- Predicted price for the variant
  confidence_upper FLOAT64,             -- Upper confidence bound
  confidence_lower FLOAT64,             -- Lower confidence bound
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the forecast was created
)
OPTIONS(
  description="Fact table for ML-generated price forecasts, including confidence intervals and model reference."
);

-- Note: BigQuery does not enforce primary or foreign key constraints.
-- variant_id and model_id should reference their respective dimension tables, but this is not enforced.