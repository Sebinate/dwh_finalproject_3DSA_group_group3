INSERT INTO warehouse.dim_merchant (merchant_id, merchant_name, merchant_street, 
                                    merchant_state, merchant_city, merchant_country,
                                    merchant_contact, merchant_creation_date_key)

SELECT DISTINCT s.merchant_id, s.merchant_name, s.merchant_street, 
                s.merchant_state, s.merchant_city, s.merchant_country,
                s.merchant_number, w.date_key

FROM staging.merchant_data AS s
LEFT JOIN warehouse.dim_merchant_date AS w
    ON s.merchant_creation_date = w.date_full
ON CONFLICT (merchant_id) DO NOTHING;

TRUNCATE TABLE staging.merchant_data;