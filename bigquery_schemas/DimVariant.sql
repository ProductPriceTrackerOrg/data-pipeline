-- Table: DimVariant
-- Stores each unique variant for a specific shop product.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimVariant` (
  variant_id INT64 NOT NULL,               -- MD5 hash of business key (source_website|product_id_native|variant_id_native)
  shop_product_id INT64 NOT NULL,          -- Foreign key to DimShopProduct (MD5 hash)
  variant_title STRING NOT NULL             -- Title/description of the variant
)
OPTIONS(
  description="Dimension table for product variants (color, size, model variations)."
);

-- Note: BigQuery does not enforce foreign key constraints.
-- shop_product_id should reference DimShopProduct.shop_product_id
