-- Table: DimCanonicalProduct
-- Master product table created by the Product Matching ML model.
-- Stores canonical products that group similar variants across shops.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimCanonicalProduct` (
  canonical_product_id INT64 NOT NULL,         -- Surrogate key for the canonical product
  product_title STRING NOT NULL,               -- Clean, representative product title
  brand STRING,                               -- Brand name of the product
  category_id INT64,                          -- References DimCategory.category_id (not enforced)
  description STRING,                         -- Product description
  master_image_url STRING                     -- URL for the master product image
)
OPTIONS(
  description="Dimension table for canonical (master) products, grouping similar variants across retailers."
);

-- Note: BigQuery does not enforce primary or foreign key constraints.
-- category_id should reference DimCategory.category_id, but this is not enforced.