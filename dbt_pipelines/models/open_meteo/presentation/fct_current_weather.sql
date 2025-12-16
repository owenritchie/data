{{ config(
    materialized='incremental',
    unique_key='snapshot_timestamp'
) }}

with weather_snapshot as (

    select
        temperature_celsius,
        precipitation_mm,
        weather_description,
        weather_detailed,
        CURRENT_TIMESTAMP AT TIME ZONE 'America/Toronto' as snapshot_timestamp
    from {{ ref('stg_current_weather') }}

)

select *
from weather_snapshot

{% if is_incremental() %}
where snapshot_timestamp >
    (select max(snapshot_timestamp) from {{ this }})
{% endif %}
