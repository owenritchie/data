-- Mart: materialize the last-7-days staging view as a table.
select * from {{ ref('stg_spotify_plays_last_7_days') }}
