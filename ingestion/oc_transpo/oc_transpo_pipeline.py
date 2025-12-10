import requests
import os
import json
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

API_KEY = os.getenv('OC_TRANSPO_API_KEY')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')
DB_NAME = os.getenv('POSTGRES_NAME')

if not API_KEY:
    raise ValueError ("API Key is not set properly.")


url = "https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-vp/beta/v1/VehiclePositions"

headers = {
    'Ocp-Apim-Subscription-Key': API_KEY,
    'Cache-Control': 'no-cache'
}

params = {'format':'json'}

response = requests.get(url, headers = headers, params = params)

print(f"Status Code: {response.status_code}") # should be 200
print(f"Content-Type: {response.headers.get('content-type')}") # should be json

if response.status_code == 200:
    data = response.json()['Entity']
    df = pd.json_normalize(data)

    # Convert all dicts/lists in DataFrame to strings
    def convert_cell(cell):
        if isinstance(cell, (dict, list)):
            return json.dumps(cell)
        return cell

    df = df.applymap(convert_cell)


    print(df.head())
    print(f"Success! Retrieved {len(df)} trip updates from the OC Transpo Endpoint.")

    # Write to Database
    try:
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)

        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS raw_active_vehicles CASCADE"))
            conn.commit()

        df.to_sql('raw_active_vehicles', engine, if_exists='replace', index=False)
        print("Successfully updated data to database.")
    except Exception as e:
        print(f"Failed to write to database: {e}")
else:
    print(f"Request failed. Error: {requests.status_codes}")
    print(response.text)
