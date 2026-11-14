create or replace view default.mart_event_performance
  
  
  as
    

SELECT
    event_name,
    total_events,
    unique_sessions,
    ROUND(
        total_events * 100.0 / SUM(total_events) OVER (),
        2
    ) AS event_share_pct
FROM default.stg_event_summary
