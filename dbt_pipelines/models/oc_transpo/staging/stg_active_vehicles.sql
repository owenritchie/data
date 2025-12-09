{{ config(materialized='view') }}

SELECT
    CAST("vehicle.vehicle.id" AS INTEGER) AS vehicle_id,
    CAST("vehicle.trip.route_id" AS VARCHAR) || '-' || CAST("vehicle.trip.direction_id" AS VARCHAR) AS route_id,
    "vehicle.trip.trip_id" AS trip_id,
    "vehicle.trip.start_time" AS scheduled_start_time,
    "vehicle.trip.start_date" AS scheduled_start_date,
    CAST("vehicle.position.latitude" AS DECIMAL(10, 8)) AS latitude,
    CAST("vehicle.position.longitude" AS DECIMAL(11, 8)) AS longitude,
    CAST("vehicle.position.bearing" AS DECIMAL(5, 2)) AS bearing,
    CAST("vehicle.position.speed" AS DECIMAL(6, 2)) AS speed,
    CAST("vehicle.current_status" AS INTEGER) AS current_status,
    "vehicle.timestamp" AS last_update_timestamp
FROM {{ source('raw', 'raw_active_vehicles') }}
WHERE "vehicle.trip.route_id" IS NOT NULL
