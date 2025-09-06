-- Table: FactProductRecommendation
-- [ML Output] Stores product-to-product recommendations.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.FactProductRecommendation` (
  recommendation_id INT64 NOT NULL,             -- Surrogate key for the recommendation
  source_shop_product_id INT64 NOT NULL,       -- Foreign key to DimShopProduct (source product)
  recommended_shop_product_id INT64 NOT NULL,   -- Foreign key to DimShopProduct (recommended product)
  model_id INT64 NOT NULL,                     -- Foreign key to DimModel
  recommendation_score FLOAT64,                -- Recommendation strength score
  recommendation_type STRING,                  -- Type of recommendation (e.g., 'similar', 'complementary')
  created_at TIMESTAMP                         -- When the recommendation was generated
)
OPTIONS(
  description="Fact table for ML-generated product-to-product recommendations."
);

-- Note: BigQuery does not enforce foreign key constraints.
-- source_shop_product_id and recommended_shop_product_id should reference DimShopProduct.shop_product_id
-- model_id should reference DimModel.model_id