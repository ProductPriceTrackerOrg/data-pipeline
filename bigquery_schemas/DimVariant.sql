-- Table: DimVariant
-- Stores each unique product variant listing from every retailer.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimVariant` (
  variant_id INT64 NOT NULL,                   -- Surrogate key for the product variant
  canonical_product_id INT64,                  -- References DimCanonicalProduct.canonical_product_id (not enforced)
  shop_id INT64,                              -- References DimShop.shop_id (not enforced)
  variant_title STRING NOT NULL,               -- Title of the product variant
  sku_native STRING,                          -- Native SKU from the retailer
  product_url STRING NOT NULL,                -- URL to the product page
  image_url STRING                            -- URL to the product image
)
OPTIONS(
  description="Dimension table for product variants, representing unique listings from each retailer."
);

-- Note: BigQuery does not enforce primary or foreign key constraints.
-- canonical_product_id and shop_id should reference their respective dimension tables, but this is not enforced.