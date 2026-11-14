{{ config(materialized='view') }}

SELECT
    event_name,
    total_events,
    unique_sessions
FROM {{ source('gold', 'gold_event_summary') }}
