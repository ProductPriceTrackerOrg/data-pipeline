-- DimShop Transformation
-- Extract unique shops from staging data

INSERT INTO `price-pulse-470211.warehouse.DimShop` (
    shop_id,
    shop_name,
    website_url,
    contact_phone,
    contact_whatsapp
)
WITH shop_mapping AS (
    SELECT 'appleme' AS source_name, 1 AS shop_id, 'AppleMe' AS shop_name, 'https://appleme.lk' AS website_url, NULL AS contact_phone, NULL AS contact_whatsapp
    UNION ALL
    SELECT 'simplytek', 2, 'SimplyTek', 'https://simplytek.lk', NULL, NULL
    UNION ALL
    SELECT 'onei.lk', 3, 'Onei.lk', 'https://onei.lk', NULL, NULL
),
current_shops AS (
    SELECT DISTINCT source_website
    FROM `price-pulse-470211.staging.stg_raw_appleme`
    UNION DISTINCT
    SELECT DISTINCT source_website
    FROM `price-pulse-470211.staging.stg_raw_simplytek`
    UNION DISTINCT
    SELECT DISTINCT source_website
    FROM `price-pulse-470211.staging.stg_raw_onei_lk`
)
SELECT 
    sm.shop_id,
    sm.shop_name,
    sm.website_url,
    sm.contact_phone,
    sm.contact_whatsapp
FROM shop_mapping sm
INNER JOIN current_shops cs ON sm.source_name = cs.source_website
WHERE NOT EXISTS (
    SELECT 1 FROM `price-pulse-470211.warehouse.DimShop` ds 
    WHERE ds.shop_id = sm.shop_id
);
