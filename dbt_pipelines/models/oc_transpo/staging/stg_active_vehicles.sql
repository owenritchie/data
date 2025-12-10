{{ config(materialized='view') }}

SELECT
    CAST("Vehicle.Vehicle.Id" AS INTEGER) AS vehicle_id,
    CAST("Vehicle.Trip.RouteId" AS VARCHAR) || '-' || CAST("Vehicle.Trip.DirectionId" AS VARCHAR) AS route_id,
    "Vehicle.Trip.TripId" AS trip_id,
    "Vehicle.Trip.StartTime" AS scheduled_start_time,
    "Vehicle.Trip.StartDate" AS scheduled_start_date,
    CAST("Vehicle.Position.Latitude" AS DECIMAL(10, 8)) AS latitude,
    CAST("Vehicle.Position.Longitude" AS DECIMAL(11, 8)) AS longitude,
    CAST("Vehicle.Position.Bearing" AS DECIMAL(5, 2)) AS bearing,
    CAST("Vehicle.Position.Speed" AS DECIMAL(6, 2)) * CAST(3.6 AS DECIMAL(3,1)) AS speed,
    CAST("Vehicle.CurrentStatus" AS INTEGER) AS current_status,
    "Vehicle.Timestamp" AS last_update_timestamp
FROM {{ source('raw', 'raw_active_vehicles') }}
WHERE "Vehicle.Trip.RouteId" IS NOT NULL
