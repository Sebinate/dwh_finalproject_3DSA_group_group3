INSERT INTO warehouse.dim_user (user_id, user_name, user_ccn, user_bank, 
                                user_street, user_city, user_country, user_birthdate_key, 
                                user_gender, user_type, user_job_title, user_job_level, user_creation_date_key, user_device)

SELECT DISTINCT ud.user_id, ud.user_name, 
                COALESCE(ucc.user_ccn, 'None'), COALESCE(ucc.user_issuing_bank, 'None'), 
                ud.user_street, ud.user_city, ud.user_country, 
                COALESCE(birthdate.date_key, 19000101), ud.user_gender, ud.user_type, 
                COALESCE(uj.user_job_title, 'None'), COALESCE(uj.user_job_level, 'None'),
                COALESCE(creation.date_key, -1), ud.user_device_address

FROM staging.user_data AS ud
LEFT JOIN staging.user_credit_card AS ucc
    ON ud.user_id = ucc.user_id
LEFT JOIN staging.user_job AS uj
    ON ud.user_id = uj.user_id
LEFT JOIN warehouse.dim_user_date AS birthdate
    ON birthdate.date_full = ud.user_birthdate
LEFT JOIN warehouse.dim_user_date AS creation
    ON creation.date_full = ud.user_creation_date
ON CONFLICT (user_id) DO NOTHING;

TRUNCATE TABLE staging.user_credit_card;

TRUNCATE TABLE staging.user_data;

TRUNCATE TABLE staging.user_job;