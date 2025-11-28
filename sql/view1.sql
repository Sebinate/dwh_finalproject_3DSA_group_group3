CREATE VIEW analytics.view1 AS
SELECT
    user_id, user_ccn, user_bank, user_gender, user_country, user_type, user_job_title, user_job_level
    merchant_name, merchant_country,
    staff_id, staff_job_level, staff_country,
    product_id, product_name, product_type,
    order_estimated_arrival,
    dates.date_full AS "date",
    transactions."delay" AS "delay (in days)", transactions.price AS price, transactions.quantity AS quantity

FROM warehouse.fact_transaction AS transactions
JOIN warehouse.dim_order AS orders --
    ON orders.order_key = transactions.order_key
JOIN warehouse.dim_user AS users
    ON users.user_key = transactions.user_key
JOIN warehouse.dim_merchant AS merchant
    ON merchant.merchant_key = transactions.merchant_key
JOIN warehouse.dim_staff AS staff
    ON staff.staff_key = transactions.staff_key
JOIN warehouse.dim_product AS product
    ON product.product_key = transactions.product_key
JOIN warehouse.dim_date AS dates
    ON dates.date_key = transactions.date_key;