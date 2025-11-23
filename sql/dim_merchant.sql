INSERT INTO warehouse.dim_merchant (merchant_id, merchant_name, merchant_street, 
                                    merchant_state, merchant_city, merchant_country,
                                    merchant_contact, merchant_creation_date)

SELECT DISTINCT merchant_id, merchant_name, merchant_street, 
                merchant_state, merchant_city, merchant_country,
                merchant_contact, merchant_creation_date

FROM staging.merchant;