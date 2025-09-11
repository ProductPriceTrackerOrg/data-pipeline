-- Table: DimDate
-- Standard date dimension table for time-based analysis in the data warehouse.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.DimDate` (
  date_id INT64 NOT NULL,                -- Surrogate key for the date (e.g., 20250802)
  full_date DATE NOT NULL,               -- Actual calendar date
  year INT64 NOT NULL,                   -- Year component
  month INT64 NOT NULL,                  -- Month component
  day INT64 NOT NULL,                    -- Day component
  day_of_week STRING NOT NULL            -- Day of the week (e.g., 'Monday')
)
OPTIONS(
  description="Dimension table for calendar dates, supporting time-based analytics."
);

-- Note: BigQuery does not enforce primary keys. Ensure date_id is unique via ETL logic.