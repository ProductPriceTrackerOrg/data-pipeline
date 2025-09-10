-- Table: DimProductImage
-- Stores all image URLs for a specific shop product.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimProductImage` (
  image_id INT64 NOT NULL,                  -- Surrogate key for the image
  shop_product_id STRING NOT NULL,          -- Foreign key to DimShopProduct (MD5 hash)
  image_url STRING NOT NULL,                -- URL to the product image (max 1024 chars)
  sort_order INT64,                         -- Display order of the image (1 = primary)
  scraped_date DATE                         -- Date when this image was scraped
)
OPTIONS(
  description="Dimension table for product images from shop listings."
);

-- Note: BigQuery does not enforce foreign key constraints.
-- shop_product_id should reference DimShopProduct.shop_product_id
