CREATE VIEW analytics.ML_view_t2023 AS
SELECT 
    dates.date_full AS "date",
    users.user_type AS user_type,
    COUNT(orders.order_id) AS order_counts,
    SUM(transactions.price * transactions.quantity) AS revenue,
    AVG(transactions.delay) AS "average_delays",
    COALESCE(AVG(campaign.campaign_discount), 0) AS "average_discount"

FROM warehouse.fact_transaction AS transactions
JOIN warehouse.dim_date AS dates
    ON dates.date_key = transactions.date_key
JOIN warehouse.dim_order AS orders
    ON orders.order_key = transactions.order_key
LEFT JOIN warehouse.dim_user AS users
    ON users.user_key = transactions.user_key
LEFT JOIN warehouse.fact_promotion_eligibility AS eligibility
    ON transactions.order_key = eligibility.order_key
LEFT JOIN warehouse.dim_campaign AS campaign
    ON eligibility.campaign_key = campaign.campaign_key
GROUP BY "date", user_type;