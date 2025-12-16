{{ config(materialized='view') }}


SELECT
    CAST("temperature_2m" AS DECIMAL(5, 2)) AS temperature_celsius,
    CAST("precipitation" AS DECIMAL(6, 2)) AS precipitation_mm,
    CASE 
        WHEN "weather_code" = 0 THEN 'Clear sky'
        WHEN "weather_code" IN (1, 2, 3) THEN 'Partly cloudy'
        WHEN "weather_code" IN (45, 48) THEN 'Fog'
        WHEN "weather_code" IN (51, 53, 55, 56, 57) THEN 'Drizzle'
        WHEN "weather_code" IN (61, 63, 65, 80, 81, 82) THEN 'Rain'
        WHEN "weather_code" IN (66, 67) THEN 'Freezing Rain'
        WHEN "weather_code" IN (71, 73, 75, 77, 85, 86) THEN 'Snow'
        WHEN "weather_code" IN (95, 96, 99) THEN 'Thunderstorm'
        ELSE 'Unknown'
    END AS weather_description,
    CASE
        WHEN "weather_code" = 0 THEN 'Clear sky'
        WHEN "weather_code" = 1 THEN 'Mainly clear'
        WHEN "weather_code" = 2 THEN 'Partly cloudy'
        WHEN "weather_code" = 3 THEN 'Overcast'
        WHEN "weather_code" = 45 THEN 'Fog'
        WHEN "weather_code" = 48 THEN 'Depositing rime fog'
        WHEN "weather_code" = 51 THEN 'Light drizzle'
        WHEN "weather_code" = 53 THEN 'Moderate drizzle'
        WHEN "weather_code" = 55 THEN 'Dense drizzle'
        WHEN "weather_code" = 56 THEN 'Light freezing drizzle'
        WHEN "weather_code" = 57 THEN 'Dense freezing drizzle'
        WHEN "weather_code" = 61 THEN 'Slight rain'
        WHEN "weather_code" = 63 THEN 'Moderate rain'
        WHEN "weather_code" = 65 THEN 'Heavy rain'
        WHEN "weather_code" = 66 THEN 'Light freezing rain'
        WHEN "weather_code" = 67 THEN 'Heavy freezing rain'
        WHEN "weather_code" = 71 THEN 'Slight snow fall'
        WHEN "weather_code" = 73 THEN 'Moderate snow fall'
        WHEN "weather_code" = 75 THEN 'Heavy snow fall'
        WHEN "weather_code" = 77 THEN 'Snow grains'
        WHEN "weather_code" = 80 THEN 'Slight rain showers'
        WHEN "weather_code" = 81 THEN 'Moderate rain showers'
        WHEN "weather_code" = 82 THEN 'Violent rain showers'
        WHEN "weather_code" = 85 THEN 'Slight snow showers'
        WHEN "weather_code" = 86 THEN 'Heavy snow showers'
        WHEN "weather_code" = 95 THEN 'Thunderstorm'
        WHEN "weather_code" = 96 THEN 'Thunderstorm with slight hail'
        WHEN "weather_code" = 99 THEN 'Thunderstorm with heavy hail'
        ELSE 'Unknown'
    END AS weather_detailed
    FROM {{source('raw', 'raw_current_weather')}}
WHERE "temperature_2m" IS NOT NULL AND "precipitation" IS NOT NULL AND "weather_code" IS NOT NULL