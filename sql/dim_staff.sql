INSERT INTO warehouse.dim_staff (staff_id, staff_name, staff_job_level, 
                                 staff_street, staff_state, staff_city, 
                                 staff_country, staff_contact_number, staff_creation_date)

SELECT DISTINCT staff_id, staff_name, staff_job_level, 
                staff_street, staff_state, staff_city, 
                staff_country, staff_contact_number, staff_creation_date 

FROM staging.staff_data;