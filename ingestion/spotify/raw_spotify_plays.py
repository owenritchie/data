"""
Pulls the 50 most recently played Spotify tracks from my account and appends them to my remote Postgres instance.

Append write disposition.

Uses the refresh token saved by spotify_auth.py to get a fresh access token
on each run.
"""

import base64
import os

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_NAME")

TOKEN_URL = "https://accounts.spotify.com/api/token"
RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"

if not REFRESH_TOKEN:
    raise ValueError("SPOTIFY_REFRESH_TOKEN is not set. Run spotify_auth.py first.")


def get_access_token():
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    response = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {auth_header}"},
        data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN},
    )

    # Spotify refresh tokens expire ~6 months after authorization (refreshing does
    # not extend them). An expired/revoked token returns HTTP 400 "invalid_grant".
    if response.status_code == 400 and response.json().get("error") == "invalid_grant":
        raise SystemExit(
            "\nSpotify refresh token is expired or revoked (they last ~6 months).\n"
            "Fix: re-run `python ingestion/spotify/spotify_auth.py`, then update\n"
            "SPOTIFY_REFRESH_TOKEN in .env and in the GitHub 'spotify' secret.\n"
        )

    response.raise_for_status()
    return response.json()["access_token"]


def get_recently_played(access_token, limit=50):
    response = requests.get(
        RECENTLY_PLAYED_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"limit": limit},
    )
    response.raise_for_status()
    return response.json()["items"]


access_token = get_access_token()
items = get_recently_played(access_token, limit=50)
print(f"Success! Retrieved {len(items)} recently played tracks from Spotify.")

rows = []
for item in items:
    track = item["track"]
    album_images = track["album"].get("images", [])
    rows.append({
        "played_at": item["played_at"],
        "track_name": track["name"],
        "artist_name": ", ".join(a["name"] for a in track["artists"]),
        "album_name": track["album"]["name"],
        # images are returned largest-first; [0] is the highest resolution (usually 640x640)
        "album_image_url": album_images[0]["url"] if album_images else None,
        "duration_ms": track["duration_ms"],
    })

df = pd.DataFrame(rows)
print(df.head())

TABLE_NAME = "raw_spotify_plays"

# played_at is unique per play (you can't play two tracks at the same instant), 
# so we insert only plays that aren't already stored. (Append)

try:
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    if inspect(engine).has_table(TABLE_NAME):
        existing = pd.read_sql(f"SELECT played_at FROM {TABLE_NAME}", engine)
        new_df = df[~df["played_at"].isin(existing["played_at"])]
    else:
        new_df = df

    if len(new_df) > 0:
        new_df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        print(f"Inserted {len(new_df)} new plays into {TABLE_NAME} "
              f"({len(df) - len(new_df)} already stored, skipped).")
    else:
        print(f"No new plays — all {len(df)} pulled tracks are already in {TABLE_NAME}.")
except Exception as e:
    print(f"Failed to write to database: {e}")
