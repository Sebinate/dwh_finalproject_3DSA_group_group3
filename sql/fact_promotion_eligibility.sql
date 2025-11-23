    INSERT INTO Fact_Promotion_Eligibility (
        CampaignKey,
        OrderKey
    )
    SELECT
        dim_campaign.CampaignKey,
        dim_order.OrderKey
        FROM staging.promotion_eligibility
    JOIN dim_campaign
        ON staging.promotion_eligibility.campaign_id = dim_campaign.campaign_id
    JOIN dim_order
        ON staging.promotion_eligibility.order_id = dim_order.order_id;