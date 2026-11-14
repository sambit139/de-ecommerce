{{ config(materialized='view') }}

SELECT
    session_id,
    total_events,
    cart_event_count,
    booking_event_count,
    has_cart_event,
    has_booking_event,
    first_event_time,
    last_event_time
FROM {{ source('gold', 'gold_session_summary') }}
