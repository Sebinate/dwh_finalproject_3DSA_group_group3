    INSERT INTO warehouse.Fact_Promotion_Eligibility (
        CampaignKey,
        OrderKey
    )
    SELECT
        dim_campaign.CampaignKey,
        dim_order.OrderKey
    FROM staging.promotion_eligibility
    LEFT JOIN warehouse.dim_campaign
        ON staging.promotion_eligibility.campaign_id = dim_campaign.campaign_id
    LEFT JOIN warehouse.dim_order
        ON staging.promotion_eligibility.order_id = dim_order.order_id;