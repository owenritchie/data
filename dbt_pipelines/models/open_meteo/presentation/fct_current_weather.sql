{{ config(materialized='table') }}

SELECT
    temperature_celsius,
    precipitation_mm,
    weather_description,
    weather_detailed,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Toronto' AS updated_at
FROM {{ ref('stg_current_weather') }}