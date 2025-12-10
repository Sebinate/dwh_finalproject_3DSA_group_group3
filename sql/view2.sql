CREATE OR REPLACE VIEW analytics.campaign_data AS 
SELECT
    campaign_name,
    SUM((quantity * (price - (price * campaign_discount)))) AS revenue,
    COUNT(order_id) AS volume
FROM warehouse.fact_promotion_eligibility AS fpe
JOIN warehouse.dim_campaign AS campaign
    ON campaign.campaign_key = fpe.campaign_key
JOIN warehouse.dim_order AS orders
    ON orders.order_key = fpe.order_key
JOIN warehouse.fact_transaction AS transactions
    ON transactions.order_key = orders.order_key
GROUP BY campaign_name;