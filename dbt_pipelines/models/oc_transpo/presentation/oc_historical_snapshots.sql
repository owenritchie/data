{{ config(
    materialized='incremental',
    unique_key='snapshot_timestamp'
) }}

with latest_snapshot as (
    select
        MAX(updated_at) as snapshot_timestamp,
        COUNT(DISTINCT vehicle_id) as active_buses,
        COUNT(DISTINCT route_id) as active_routes,
        AVG(speed) as average_speed,
        COUNT(DISTINCT vehicle_id) FILTER (WHERE speed = 0) as stopped_vehicles,
        COUNT(DISTINCT vehicle_id) FILTER (WHERE speed > 50) as fast_vehicles,
        AVG(speed) FILTER (WHERE speed > 0) as average_moving_speed
    from {{ ref('fct_active_vehicles') }}
)

select * from latest_snapshot

{% if is_incremental() %}
    WHERE snapshot_timestamp > (SELECT MAX(snapshot_timestamp) FROM {{ this }})
{% endif %}
