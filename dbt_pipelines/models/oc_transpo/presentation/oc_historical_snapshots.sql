{{ config(
    materialized='incremental',
    unique_key='snapshot_timestamp',
    pre_hook="CREATE TABLE IF NOT EXISTS {{ this }} (
        snapshot_timestamp TIMESTAMP PRIMARY KEY,
        active_buses INTEGER NOT NULL,
        active_routes INTEGER NOT NULL,
        average_speed DECIMAL(6, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )",
    indexes=[
        {'columns': ['snapshot_timestamp'], 'unique': True}
    ]
) }}

with latest_snapshot as (
    select
        MAX(updated_at) as snapshot_timestamp,
        COUNT(DISTINCT vehicle_id) as active_buses,
        COUNT(DISTINCT route_id) as active_routes,
        AVG(speed) as average_speed
    from {{ ref('fct_active_vehicles') }}
)

select * from latest_snapshot

{% if is_incremental() %}
    WHERE snapshot_timestamp > (SELECT MAX(snapshot_timestamp) FROM {{ this }})
{% endif %}
