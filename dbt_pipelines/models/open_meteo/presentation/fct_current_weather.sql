{{ config(
    materialized='incremental',
    unique_key='snapshot_timestamp'
) }}

select
    temperature_celsius,
    precipitation_mm,
    weather_description,
    weather_detailed,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Toronto' as snapshot_timestamp
from {{ ref('stg_current_weather') }}

{% if is_incremental() %}
where CURRENT_TIMESTAMP AT TIME ZONE 'America/Toronto' > (select max(snapshot_timestamp) from {{ this }})
{% endif %}
