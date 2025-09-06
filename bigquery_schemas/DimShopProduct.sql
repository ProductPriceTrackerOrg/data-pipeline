-- Table: DimShopProduct
-- Stores each unique product listing exactly as it appears on the retailer's site.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimShopProduct` (
  shop_product_id STRING NOT NULL,              -- MD5 hash of business key (source_website|product_id_native)
  shop_id INT64 NOT NULL,                       -- Foreign key to DimShop
  product_title_native STRING NOT NULL,         -- Product title as it appears on shop (max 500 chars)
  brand_native STRING,                          -- Brand name as it appears on shop
  description_native STRING,                    -- Product description from shop
  product_url STRING,                           -- URL to the product page (max 1024 chars)
  scraped_date DATE,                            -- Date when this product was first scraped
  predicted_master_category_id INT64            -- ML-predicted category (references DimCategory)
)
OPTIONS(
  description="Dimension table for individual product listings from each shop."
);

-- Note: BigQuery does not enforce foreign key constraints.
-- shop_id should reference DimShop.shop_id
-- predicted_master_category_id should reference DimCategory.category_id
