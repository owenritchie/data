with source as (
    select * from {{ source('spotify', 'raw_spotify_plays') }}
)

select
    played_at::timestamptz              as played_at,
    track_name,
    artist_name,
    album_name,
    album_image_url,
    round(duration_ms / 1000.0)::int    as track_duration
from source
where played_at::timestamptz >= now() - interval '7 days'
