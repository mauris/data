import requests
from ics import Calendar, Event
from ics.grammar.parse import ContentLine
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
from typing import List, Dict, Tuple
import os
import json
import math

from processors.mrt_stations import mrt_stations
from utils.cache import cache

dataset_id = "d_bda4baa634dd1cc7a6c7cad5f19e2d68"
base_url = "https://data.gov.sg"
initial_path = f"/api/action/datastore_search?resource_id={dataset_id}&limit=1000"

def extract_closures(records):
    closures = []
    for record in records:
        name = record.get("name", "Unknown")
        for q in range(1, 5):
            start = record.get(f"q{q}_cleaningstartdate")
            end = record.get(f"q{q}_cleaningenddate")
            if start and end and start != "TBC" and end != "TBC":
                closures.append({
                    "name": name,
                    "type": f"Q{q} Cleaning",
                    "start": start,
                    "end": end,
                    "location": [float(record.get("latitude_hc")), float(record.get("longitude_hc"))],
                })
        start = record.get("other_works_startdate")
        end = record.get("other_works_enddate")
        if record.get("remarks_other_works") != "nil" and start and end and start != "TBC" and end != "TBC":
            closures.append({
                "name": name,
                "type": record.get("remarks_other_works", "Other Works"),
                "start": start,
                "end": end,
                "location": [float(record.get("latitude_hc")), float(record.get("longitude_hc"))],
            })
    return closures

@cache
def fetch_all_closures():
    all_closures = []
    next_url = initial_path

    while next_url:
        print(f"Fetching data from: {base_url + next_url}")
        response = requests.get(base_url + next_url)
        if response.status_code != 200:
            print(f"Failed to fetch data: {response.status_code}")
            break

        data = response.json()
        result = data.get("result", {})
        records = result.get("records", [])
        closures = extract_closures(records)
        all_closures.extend(closures)
        if not closures:
            break
        next_url = result.get("_links", {}).get("next")
    
    return all_closures

def haversine_distance(lat1, lon1, lat2, lon2):
    """Returns distance in kilometers between two lat/lon pairs."""
    R = 6371  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def parse_date(date_str: str):
    """Parse 'DD/MM/YYYY' into a datetime.date."""
    return datetime.strptime(date_str, "%d/%m/%Y").date()

def find_nearest_center(lat: float, lon: float, centers: Dict[str, Tuple[float, float]]) -> str:
    min_dist = float('inf')
    nearest = None
    for name, (clat, clon) in centers.items():
        dist = haversine_distance(lat, lon, clat, clon)
        if dist < min_dist:
            min_dist = dist
            nearest = name
    return nearest

def create_calendar(closures: list, calendar_name: str, ics_file: str = "hawker_closures.ics"):
    cal = Calendar()    # Set calendar name (title)

    cal.extra.append(ContentLine(name='NAME', value=calendar_name))
    cal.extra.append(ContentLine(name='X-WR-CALDESC', value=calendar_name))

    for entry in closures:
        try:
            start = parse_date(entry["start"])
            end = parse_date(entry["end"]) + timedelta(days=1)  # .ics format treats end as exclusive

            event = Event()
            event.name = f"[Closure] {entry['name']} ({entry['type']})"
            event.begin = start.isoformat()
            event.end = end.isoformat()
            event.make_all_day()
            event.description = f"{entry['name']} closed for {entry['type']}\n{entry['start']} to {entry['end']}"

            cal.events.add(event)
        except Exception as e:
            print(f"Skipping entry due to error: {entry} | Error: {e}")

    with open(ics_file, "w", encoding="utf-8") as f:
        f.writelines(cal.serialize_iter())

    print(f"Calendar saved to {ics_file}")

def cluster_by_custom_centers(
    closures: List[Dict],
    centers: Dict[str, Tuple[float, float]],
    out_dir: str
):
    os.makedirs(out_dir, exist_ok=True)
    clustered: Dict[str, List[Dict]] = {name: [] for name in centers}

    for entry in closures:
        try:
            lat = float(entry['location'][0])
            lng = float(entry['location'][1])
            region = find_nearest_center(lat, lng, centers)
            clustered[region].append(entry)
        except Exception as e:
            print(f"Skipping due to missing/invalid coordinates: {entry.get('name')} - {e}")

    for region, region_closures in clustered.items():
        filename = os.path.join(out_dir, f"{region.replace(" ", "").strip().lower()}.ics")
        if not region_closures:
            print(f"No closures found for region: {region}")
            continue
        create_calendar(region_closures, f"Hawker Centre Closures near {region}", filename)

def hawker_center_closure(out_dir: str = "output/hawker_closures"):
    closures = fetch_all_closures()
    
    stations = mrt_stations()
    centers = {station['name']: (station['location'][0], station['location'][1]) for station in stations}

    cluster_by_custom_centers(closures, centers, out_dir)
