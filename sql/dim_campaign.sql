INSERT INTO warehouse.dim_campaign (campaign_id, campaign_name, campaign_desc, campaign_discount)

SELECT DISTINCT campaign_id, campaign_name, campaign_desc, campaign_discount

FROM staging.campaign;