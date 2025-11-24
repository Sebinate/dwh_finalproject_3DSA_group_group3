INSERT INTO warehouse.fact_transaction (
    date_key,
    staff_key,
    merchant_key,
    user_key,
    product_key,
    order_key,
    "delay",
    price,
    quantity
)
SELECT 
    COALESCE(warehouse.dim_date.date_key, -1) AS date_key, --
    COALESCE(staff.staff_key, -1) AS staff_key,
    COALESCE(merchant.merchant_key, -1) AS merchant_key,
    COALESCE(warehouse.dim_user.user_key, -1) AS user_key, --
    COALESCE(warehouse.dim_product.product_key, -1) AS product_key, --
    COALESCE(warehouse.dim_order.order_key, -1) AS order_key, --
    COALESCE(order_delay.order_delay_days, 0), --
    line_prices.product_price, --
    line_prices.order_quantity --
    
FROM 
    staging.line_item_data_prices AS line_prices

LEFT JOIN staging.order_delays AS order_delay
    ON line_prices.order_id = order_delay.order_id

-- Joining on products
LEFT JOIN staging.line_item_data_products AS line_products
    ON line_prices.order_id = line_products.order_id
LEFT JOIN warehouse.dim_product
    ON warehouse.dim_product.product_id = line_products.product_id

-- Joining on users
LEFT JOIN staging.order_data AS order_data 
    ON line_prices.order_id = order_data.order_id
LEFT JOIN warehouse.dim_user
    ON warehouse.dim_user.user_id = order_data.user_id

-- Joining on Orders
LEFT JOIN warehouse.dim_order
    ON warehouse.dim_order.order_id = line_prices.order_id
    LEFT JOIN warehouse.dim_order_date
        ON warehouse.dim_order.order_transac_date_key = warehouse.dim_order_date.date_key

-- Joining on Date
LEFT JOIN warehouse.dim_date
    ON warehouse.dim_order_date.date_full = order_data.transact_date

-- Joining on Merchant
LEFT JOIN staging.order_with_merchant AS owm
    ON owm.order_id = line_prices.order_id
LEFT JOIN warehouse.dim_merchant AS merchant
    ON merchant.merchant_id = owm.merchant_id

-- Joining on Staff
LEFT JOIN warehouse.dim_staff AS staff
    ON staff.staff_id = owm.staff_id