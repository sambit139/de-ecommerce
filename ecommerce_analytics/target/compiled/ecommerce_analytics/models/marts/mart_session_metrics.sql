

SELECT
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(has_cart_event) AS sessions_with_cart,
    SUM(has_booking_event) AS sessions_with_booking,
    SUM(
        CASE
            WHEN has_cart_event = 1 AND has_booking_event = 1
            THEN 1 ELSE 0
        END
    ) AS sessions_with_both_events,
    ROUND(AVG(CAST(total_events AS DOUBLE)), 2) AS average_events_per_session
FROM default.stg_session_summary