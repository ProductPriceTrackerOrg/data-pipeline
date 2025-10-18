-- Table: FactPersonalizedRecommendation
-- [ML Output] Stores personalized recommendations for users

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.FactPersonalizedRecommendation` (
  recommendation_id INT64 NOT NULL,             -- Primary key
  user_id INT64 NOT NULL,                       -- The ID of the user (from the User Database)
  recommended_variant_id INT64 NOT NULL,        -- Foreign key to DimVariant
  model_id INT64 NOT NULL,                      -- Foreign key to DimModel
  recommendation_score FLOAT64,                 -- Recommendation score
  recommendation_type STRING,                   -- Type of recommendation
  created_at TIMESTAMP                          -- When the recommendation was created
)
OPTIONS(
  description="Fact table for ML-generated personalized user recommendations."
);

-- Note: BigQuery does not enforce foreign key constraints.
-- recommended_variant_id should reference DimVariant.variant_id
-- model_id should reference DimModel.model_id
