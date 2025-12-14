INSERT INTO warehouse.fact_promotion_eligibility (
    campaign_key,
    order_key
)
SELECT
    COALESCE(dim_campaign.campaign_key, -1),                         
    COALESCE(dim_order.order_key, -1)
FROM (SELECT * 
        FROM staging.transactional_campaign_data AS tcd 
        WHERE tcd.transact_availed = true) AS fpe
LEFT JOIN warehouse.dim_campaign
    ON fpe.campaign_id = warehouse.dim_campaign.campaign_id
JOIN warehouse.dim_order
    ON fpe.order_id = warehouse.dim_order.order_id;

TRUNCATE TABLE staging.transactional_campaign_data;