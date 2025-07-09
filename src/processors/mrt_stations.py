import requests
import json

from utils.cache import cache

url = f"https://github.com/cheeaun/sgraildata/raw/refs/heads/master/data/v1/sg-rail.geojson"

@cache
def mrt_stations():
    global url
    print(f"Fetching MRT stations")
    
    response = requests.get(url)
    geojson = response.json()

    stations = []
    for feature in geojson['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        if geometry['type'] == 'Point' and properties['stop_type'] == 'station':
            stations.append({
                'name': properties['name'].strip(),
                'location': [geometry['coordinates'][1], geometry['coordinates'][0]],  # [lat, lon]
            })

    print(f"Found {len(stations)} MRT stations")
    return stations