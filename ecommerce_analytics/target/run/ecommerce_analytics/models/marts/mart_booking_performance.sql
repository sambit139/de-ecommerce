create or replace view default.mart_booking_performance
  
  
  as
    

SELECT
    payment_status,
    total_bookings,
    unique_booking_sessions,
    ROUND(
        total_bookings * 100.0 / SUM(total_bookings) OVER (),
        2
    ) AS booking_status_share_pct
FROM default.stg_booking_analysis
