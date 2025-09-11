-- Table: DimCategory
-- Stores hierarchical category data for products in the warehouse.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimCategory` (
  category_id INT64 NOT NULL,                -- Surrogate key for the category
  category_name STRING NOT NULL,             -- Name of the category (e.g., 'Laptops')
  parent_category_id INT64                   -- References category_id of parent category (for hierarchy)
)
OPTIONS(
  description="Dimension table for product categories, supporting hierarchical relationships."
);

-- Note: BigQuery does not enforce foreign key constraints.
-- The parent_category_id should reference another category_id in this table for hierarchy, but this is not enforced by BigQuery.