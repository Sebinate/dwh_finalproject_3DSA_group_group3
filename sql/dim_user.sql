INSERT INTO warehouse.dim_user (user_id, user_name, user_ccn, user_bank, 
                                user_street, user_city, user_country, user_birthdate, 
                                user_gender, user_type, user_job_title, user_job_level user_creation_date)

SELECT DISTINCT user_id, user_name, user_ccn, user_bank, 
                user_street, user_city, user_country, user_birthdate, 
                user_gender, user_type, user_job_title, user_job_level user_creation_date

FROM staging.user;