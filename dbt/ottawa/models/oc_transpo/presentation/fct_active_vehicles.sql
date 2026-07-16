{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (vehicle_id)",
        "CREATE INDEX IF NOT EXISTS idx_active_vehicles_route ON {{ this }} (route_id)",
        "CREATE INDEX IF NOT EXISTS idx_active_vehicles_updated ON {{ this }} (updated_at)"
    ]
) }}

-- Create final fact table from staging view
SELECT
    vehicle_id,
    route_id,
    trip_id,
    trip_direction,
    scheduled_start_time,
    scheduled_start_date,
    latitude,
    longitude,
    bearing,
    speed,
    current_status,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Toronto' AS updated_at
FROM {{ ref('stg_active_vehicles') }}
