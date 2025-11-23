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
    COALESCE(warehouse.dim_date.date_key, -1) AS date_key,
    COALESCE(warehouse.dim_staff.staff_key, -1) AS staff_key,
    COALESCE(warehouse.dim_merchant.merchant_key, -1) AS merchant_key,
    COALESCE(warehouse.dim_user.user_key, -1) AS user_key,
    COALESCE(warehouse.dim_product.product_key, -1) AS product_key,
    COALESCE(warehouse.dim_order.order_key, -1) AS order_key,
    
    staging.transaction."delay",
    staging.transaction.price,
    staging.transaction.quantity
FROM 
    staging.transaction

LEFT JOIN 
    warehouse.dim_date
    ON staging.transaction.transaction_date = warehouse.dim_date.date_full 

LEFT JOIN 
    warehouse.dim_staff
    ON staging.transaction.staff_id = warehouse.dim_staff.staff_id

LEFT JOIN 
    warehouse.dim_merchant
    ON staging.transaction.merchant_id = warehouse.dim_merchant.merchant_id

LEFT JOIN 
    warehouse.dim_user
    ON staging.transaction.user_id = warehouse.dim_user.user_id

LEFT JOIN 
    warehouse.dim_product
    ON staging.transaction.product_id = warehouse.dim_product.product_id

LEFT JOIN 
    warehouse.dim_order
    ON staging.transaction.order_id = warehouse.dim_order.order_id;