{{ config(materialized='view') }}

SELECT
    event_name,
    total_events,
    unique_sessions,
    ROUND(
        total_events * 100.0 / SUM(total_events) OVER (),
        2
    ) AS event_share_pct
FROM {{ ref('stg_event_summary') }}
