import requests
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()

def get_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["temperature_2m", "precipitation", "weather_code"],
        "timezone": "America/Toronto",
    }
    response = requests.get(url, params=params)
    return response.json()

data = get_weather(45.4215, -75.6972)  # Ottawa

print(f"Current temp: {data['current']['temperature_2m']}°C")
print(f"Precipitation: {data['current']['precipitation']} mm")


DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')
DB_NAME = os.getenv('POSTGRES_NAME')

try:
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS raw_current_weather CASCADE"))
        conn.commit()

    df = pd.DataFrame([{
        'temperature_2m': data['current']['temperature_2m'],
        'precipitation': data['current']['precipitation'],
        'weather_code': data['current']['weather_code']
    }])

    df.to_sql('raw_current_weather', engine, if_exists='replace', index=False)
    print("Successfully updated weather data to database.")
except Exception as e:
    print(f"Failed to write to database: {e}")