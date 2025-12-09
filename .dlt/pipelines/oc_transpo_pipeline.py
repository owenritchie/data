import dlt
import requests

@dlt.resource(
    name="raw_active_vehicles",
    write_disposition="replace"
)
def oc_transpo_active_vehicles():
    '''Fetches active vehicle data from the OC Transpo API and populates PosgreSQL tables.'''
    response = requests.get(
        "https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-vp/beta/v1/VehiclePositions",
        headers={'Ocp-Apim-Subscription-Key': dlt.secrets['sources.oc_transpo.api_key']},
        params={'format': 'json'}
    )

    yield from response.json()['Entity']

if __name__ == "__main__":
    pipeline = dlt.pipeline("oc_transpo_pipeline", destination="postgres", dataset_name="public", dev_mode=False)
    pipeline.run(oc_transpo_active_vehicles())

    # drop dlt tables to save space
    with pipeline.sql_client() as client:
        client.execute_sql("DROP TABLE IF EXISTS public._dlt_loads CASCADE")
        client.execute_sql("DROP TABLE IF EXISTS public._dlt_pipeline_state CASCADE")
        client.execute_sql("DROP TABLE IF EXISTS public._dlt_version CASCADE")

    print("OC Transpo raw_active_vehicles data has been loaded into PostgreSQL.")