{{ config(materialized='view') }}

SELECT
    payment_status,
    total_bookings,
    unique_booking_sessions
FROM {{ source('gold', 'gold_booking_analysis') }}
