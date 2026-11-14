

SELECT
    mastercategory,
    subcategory,
    articletype,
    cart_events,
    unique_cart_sessions,
    total_quantity,
    observed_cart_item_price_sum,
    average_cart_event_item_price,
    ROW_NUMBER() OVER (
        ORDER BY cart_events DESC
    ) AS cart_activity_rank
FROM default.stg_category_cart_analysis