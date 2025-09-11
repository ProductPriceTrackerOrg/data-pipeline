-- Table: DimShop
-- Stores information about each retailer/shop in the platform.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimShop` (
  shop_id INT64 NOT NULL,                -- Surrogate key for the shop
  shop_name STRING NOT NULL,             -- Unique name of the shop (e.g., 'nanotek.lk')
  website_url STRING,                    -- Shop website URL
  contact_phone STRING,                  -- Shop contact phone number
  contact_whatsapp STRING                -- Shop WhatsApp contact
)
OPTIONS(
  description="Dimension table for retailer/shop metadata."
);

-- Note: BigQuery does not enforce primary keys or uniqueness constraints.
-- Ensure shop_id and shop_name are unique via ETL logic.