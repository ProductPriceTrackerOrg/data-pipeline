-- DimDate Daily Update Transformation
-- Adds current date to DimDate if it doesn't exist

INSERT INTO `price-pulse-470211.warehouse.DimDate` (
    date_id,
    full_date,
    year,
    month,
    day,
    day_of_week
)
SELECT 
    CAST(FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS INT64) AS date_id,
    CURRENT_DATE() AS full_date,
    EXTRACT(YEAR FROM CURRENT_DATE()) AS year,
    EXTRACT(MONTH FROM CURRENT_DATE()) AS month,
    EXTRACT(DAY FROM CURRENT_DATE()) AS day,
    FORMAT_DATE('%A', CURRENT_DATE()) AS day_of_week
WHERE NOT EXISTS (
    SELECT 1 
    FROM `price-pulse-470211.warehouse.DimDate` 
    WHERE date_id = CAST(FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS INT64)
);
