INSERT INTO warehouse.dim_staff (staff_id, staff_name, staff_job_level, 
                                 staff_street, staff_state, staff_city, 
                                 staff_country, staff_contact_number, staff_creation_date_key)

SELECT DISTINCT s.staff_id, s.staff_name, s.staff_job_level, 
                s.staff_street, s.staff_state, s.staff_city, 
                s.staff_country, s.staff_contact_number, w.date_key 

FROM staging.staff_data AS s
LEFT JOIN warehouse.dim_staff_date AS w
    ON s.staff_creation_date = w.date_full
ON CONFLICT (staff_id) DO NOTHING;

TRUNCATE TABLE staging.staff_data;