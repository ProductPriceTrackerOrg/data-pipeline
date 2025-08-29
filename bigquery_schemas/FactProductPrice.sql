-- Table: FactProductPrice
-- Central fact table, recording daily price and availability for each product variant.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.FactProductPrice` (
  price_fact_id INT64 NOT NULL,                -- Surrogate key for the price record
  variant_id INT64 NOT NULL,                   -- References DimVariant.variant_id (not enforced)
  date_id INT64 NOT NULL,                      -- References DimDate.date_id (not enforced)
  current_price NUMERIC(10,2) NOT NULL,        -- Current price of the product
  original_price NUMERIC(10,2),                -- Original price (if available)
  is_available BOOL NOT NULL                   -- Availability status (true/false)
)
OPTIONS(
  description="Fact table for daily product prices and availability, linked to variants and dates."
);

-- Note: BigQuery does not enforce primary or foreign key constraints.
-- variant_id and date_id should reference their respective dimension tables, but this is not enforced.