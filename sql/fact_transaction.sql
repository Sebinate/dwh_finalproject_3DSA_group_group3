-- 1. Index the Staging Tables (This makes joins instant)
CREATE INDEX IF NOT EXISTS idx_stg_prices_order ON staging.line_item_data_prices(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_delays_order ON staging.order_delays(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_products_order ON staging.line_item_data_products(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_products_prod ON staging.line_item_data_products(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_data_order ON staging.order_data(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_data_user ON staging.order_data(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_merch_order ON staging.order_with_merchant(order_id);

-- 2. Update Statistics
ANALYZE staging.line_item_data_prices;
ANALYZE staging.order_delays;
ANALYZE staging.line_item_data_products;
ANALYZE staging.order_data;
ANALYZE staging.order_with_merchant;

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
    COALESCE(warehouse.dim_date.date_key, 19000101) AS date_key, --
    COALESCE(staff.staff_key, -1) AS staff_key,
    COALESCE(merchant.merchant_key, -1) AS merchant_key,
    COALESCE(warehouse.dim_user.user_key, -1) AS user_key, --
    COALESCE(warehouse.dim_product.product_key, -1) AS product_key, --
    COALESCE(warehouse.dim_order.order_key, -1) AS order_key, --
    COALESCE(order_delay.order_delay_days, 0), --
    prices.product_price, --
    prices.order_quantity --
    
FROM 
    (
        SELECT 
            order_id, 
            product_price, 
            order_quantity,
            ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY ctid) as rn
        FROM staging.line_item_data_prices
    ) AS prices

INNER JOIN 
    (
        SELECT 
            order_id, 
            product_id,
            ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY ctid) as rn
        FROM staging.line_item_data_products
    ) AS products
    ON prices.order_id = products.order_id 
    AND prices.rn = products.rn

LEFT JOIN staging.order_delays AS order_delay --
    ON prices.order_id = order_delay.order_id

LEFT JOIN staging.order_data AS order_data 
    ON prices.order_id = order_data.order_id

LEFT JOIN staging.order_with_merchant AS owm
    ON owm.order_id = prices.order_id
--
LEFT JOIN warehouse.dim_date
    ON warehouse.dim_date.date_full = order_data.transact_date::DATE

LEFT JOIN warehouse.dim_user
    ON warehouse.dim_user.user_id = order_data.user_id

LEFT JOIN warehouse.dim_order
    ON warehouse.dim_order.order_id = prices.order_id

LEFT JOIN warehouse.dim_merchant AS merchant
    ON merchant.merchant_id = owm.merchant_id

LEFT JOIN warehouse.dim_staff AS staff
    ON staff.staff_id = owm.staff_id
    
LEFT JOIN warehouse.dim_product
    ON warehouse.dim_product.product_id = products.product_id;



TRUNCATE TABLE staging.order_with_merchant;
TRUNCATE TABLE staging.line_item_data_prices;
TRUNCATE TABLE staging.line_item_data_products;
TRUNCATE TABLE staging.order_delays;
TRUNCATE TABLE staging.order_data;