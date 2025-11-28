INSERT INTO warehouse.dim_campaign (campaign_id, campaign_name, campaign_desc, campaign_discount)
SELECT DISTINCT campaign_id, campaign_name, campaign_description, campaign_discount
FROM staging.campaign_data
ON CONFLICT (campaign_id) DO NOTHING;

TRUNCATE TABLE staging.campaign_data;