{{ config(materialized='view') }}

SELECT
    payment_status,
    total_bookings,
    unique_booking_sessions,
    ROUND(
        total_bookings * 100.0 / SUM(total_bookings) OVER (),
        2
    ) AS booking_status_share_pct
FROM {{ ref('stg_booking_analysis') }}
