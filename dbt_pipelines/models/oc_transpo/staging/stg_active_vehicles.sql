{{ config(materialized='view') }}

SELECT
    CAST(vehicle__vehicle__id AS INTEGER) AS vehicle_id,
    CAST(vehicle__trip__route_id AS VARCHAR) || '-' || CAST(vehicle__trip__direction_id AS VARCHAR) AS route_id,
    vehicle__trip__trip_id AS trip_id,
    vehicle__trip__start_time AS scheduled_start_time,
    vehicle__trip__start_date AS scheduled_start_date,
    CAST(vehicle__position__latitude AS DECIMAL(10, 8)) AS latitude,
    CAST(vehicle__position__longitude AS DECIMAL(11, 8)) AS longitude,
    CAST(vehicle__position__bearing AS DECIMAL(5, 2)) AS bearing,
    CAST(vehicle__position__speed AS DECIMAL(6, 2)) AS speed,
    CAST(vehicle__current_status AS INTEGER) AS current_status,
    vehicle__timestamp AS last_update_timestamp,
    _dlt_load_id AS load_id,
    _dlt_id AS row_id
FROM {{ source('raw', 'raw_active_vehicles') }}
WHERE (is_deleted = false OR is_deleted IS NULL)
    AND vehicle__trip__route_id IS NOT NULL
