INSERT INTO warehouse.dim_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT,
    datum,
    EXTRACT(ISODOW FROM datum),
    EXTRACT(MONTH FROM datum),
    TO_CHAR(datum, 'Day'),
    EXTRACT(DAY FROM datum),
    EXTRACT(DOY FROM datum),
    EXTRACT(WEEK FROM datum),
    TO_CHAR(datum, 'Month'),
    EXTRACT(QUARTER FROM datum),
    EXTRACT(YEAR FROM datum)

FROM (
    SELECT generate_series(
        '2015-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::INTERVAL
    ) AS datum
) date_sequence
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_order_date
SELECT * FROM warehouse.dim_date
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_user_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT,
    datum,
    EXTRACT(ISODOW FROM datum),
    EXTRACT(MONTH FROM datum),
    TO_CHAR(datum, 'Day'),
    EXTRACT(DAY FROM datum),
    EXTRACT(DOY FROM datum),
    EXTRACT(WEEK FROM datum),
    TO_CHAR(datum, 'Month'),
    EXTRACT(QUARTER FROM datum),
    EXTRACT(YEAR FROM datum)
FROM (
    SELECT generate_series(
        '1970-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::INTERVAL
    ) AS datum
) date_sequence
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_staff_date
SELECT * FROM warehouse.dim_date
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_merchant_date
SELECT * FROM warehouse.dim_date
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_date (date_key, date_full, date_day_of_week, date_month_num, date_day_name, date_day_of_year, date_week_of_year, date_day_of_month, 
                                date_month_name, date_quarter, date_year)
VALUES (19000101, '1900-01-01', 0, 0, 'Unknown', 0, 0, 0, 'Unknown', 0, 1900) -- siguro naman wala nang buhay 1900s kid, should be a safe date
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_order_date SELECT * FROM warehouse.dim_date WHERE date_key = 19000101 ON CONFLICT (date_key) DO NOTHING;
INSERT INTO warehouse.dim_user_date SELECT * FROM warehouse.dim_date WHERE date_key = 19000101 ON CONFLICT (date_key) DO NOTHING;
INSERT INTO warehouse.dim_merchant_date SELECT * FROM warehouse.dim_date WHERE date_key = 19000101 ON CONFLICT (date_key) DO NOTHING;
INSERT INTO warehouse.dim_staff_date SELECT * FROM warehouse.dim_date WHERE date_key = 19000101 ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_product (product_key, product_id, product_name, product_type, product_price)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 0.00) ON CONFLICT (product_key) DO NOTHING;

INSERT INTO warehouse.dim_campaign (campaign_key, campaign_id, campaign_name, campaign_desc, campaign_discount)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 0.00) ON CONFLICT (campaign_key) DO NOTHING;

INSERT INTO warehouse.dim_staff (staff_key, staff_id, staff_name, staff_job_level, staff_street, staff_state, staff_city,
                                 staff_country, staff_contact_number, staff_creation_date_key)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
        'Unknown', 'Unknown', 19000101) ON CONFLICT (staff_key) DO NOTHING;

INSERT INTO warehouse.dim_user (user_key, user_id, user_name, user_ccn, user_bank, user_street, user_city, user_country, user_gender, 
                                user_device_type, user_type, user_job_title, user_job_level, user_birthdate_key, user_creation_date_key)

VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
        'Unknown', 'Unknown', 'Unknown', 'Unknown', 19000101, 19000101) ON CONFLICT (user_key) DO NOTHING;

INSERT INTO warehouse.dim_merchant (merchant_key, merchant_id, merchant_street, merchant_state, merchant_city, merchant_contact,
                                    merchant_name, merchant_country, merchant_creation_date_key)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
        'Unknown','Unknown', 19000101) ON CONFLICT (merchant_key) DO NOTHING;

INSERT INTO warehouse.dim_order (order_key, order_id, order_estimated_arrival, order_transac_date_key)
VALUES (-1, 'Unknown', 0, 19000101) ON CONFLICT (order_key) DO NOTHING;