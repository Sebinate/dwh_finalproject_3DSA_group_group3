CREATE VIEW analytics.view2 AS 
SELECT
    campaign_name, campaign_desc, campaign_discount, order_id, dod.date_full AS "date"
FROM warehouse.fact_promotion_eligibility AS fpe
JOIN warehouse.dim_campaign AS campaign
    ON campaign.campaign_key = fpe.campaign_key
JOIN warehouse.dim_order AS orders
    ON orders.order_key = fpe.order_key
JOIN warehouse.dim_order_date AS dod
    ON dod.date_key = orders.order_transac_date_key;