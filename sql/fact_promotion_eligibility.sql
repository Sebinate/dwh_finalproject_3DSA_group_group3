INSERT INTO warehouse.fact_promotion_eligibility (
    campaign_key,
    order_key
)
SELECT
    dim_campaign.campaign_key,                         
    dim_order.order_key
FROM (SELECT * 
        FROM staging.transactional_campaign_data AS tcd 
        WHERE tcd.transact_availed = true) AS fpe
JOIN warehouse.dim_campaign
    ON fpe.campaign_id = warehouse.dim_campaign.campaign_id
JOIN warehouse.dim_order
    ON fpe.order_id = warehouse.dim_order.order_id;