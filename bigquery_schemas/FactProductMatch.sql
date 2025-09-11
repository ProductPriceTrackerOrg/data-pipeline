-- Table: FactProductMatch
-- [ML Output] Stores match results between products from different shops.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.FactProductMatch` (
  match_id INT64 NOT NULL,                     -- Surrogate key for the match
  shop_product_1_id INT64 NOT NULL,            -- Foreign key to DimShopProduct (first product)
  shop_product_2_id INT64 NOT NULL,            -- Foreign key to DimShopProduct (second product)
  model_id INT64 NOT NULL,                     -- Foreign key to DimModel
  match_score FLOAT64,                         -- Confidence score of the match (0.0 to 1.0)
  match_type STRING,                           -- Type of match (e.g., 'exact', 'fuzzy', 'semantic')
  is_confirmed BOOLEAN,                        -- Whether the match has been manually verified
  created_at TIMESTAMP,                       -- When the match was identified
  updated_at TIMESTAMP                        -- When the match was last updated
)
OPTIONS(
  description="Fact table for ML-identified product matches between different shops."
);

-- Note: BigQuery does not enforce foreign key constraints.
-- shop_product_1_id and shop_product_2_id should reference DimShopProduct.shop_product_id
-- model_id should reference DimModel.model_id
