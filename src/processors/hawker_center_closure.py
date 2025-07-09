import requests
from ics import Calendar, Event
from ics.grammar.parse import ContentLine
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import math

from processors.mrt_stations import mrt_stations
from utils.cache import cache
from utils.data_file import DataFile

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

def find_nearby_centers(lat: float, lon:float, centers: Dict[str, Tuple[float, float]], radius_km: float = 1.0) -> List[str]:
    """Find all centers within a given radius."""
    nearby_centers = []
    for name, (clat, clon) in centers.items():
        dist = haversine_distance(lat, lon, clat, clon)
        if dist <= radius_km:
            nearby_centers.append(name)
    return nearby_centers

def create_calendar(closures: list, calendar_name: str):
    cal = Calendar()

    cal.extra.append(ContentLine(name='NAME', value=calendar_name))
    cal.extra.append(ContentLine(name='X-WR-CALNAME', value=calendar_name))
    cal.extra.append(ContentLine(name='DESCRIPTION', value=calendar_name))
    cal.extra.append(ContentLine(name='X-WR-CALDESC', value=calendar_name))
    cal.extra.append(ContentLine(name='TIMEZONE-ID', value="Asia/Singapore"))
    cal.extra.append(ContentLine(name='X-WR-TIMEZONE', value="Asia/Singapore"))

    for entry in closures:
        try:
            start = parse_date(entry["start"])
            end = parse_date(entry["end"])

            event = Event()
            event.name = f"[Closed] {entry['name']} ({entry['type']})"
            event.begin = start
            event.end = end
            event.description = f"{entry['name']} closed for {entry['type']}\n{entry['start']} to {entry['end']}"
            event.make_all_day()

            cal.events.add(event)
        except Exception as e:
            print(f"Skipping entry due to error: {entry} | Error: {e}")

    return cal

def cluster_by_custom_centers(
    closures: List[Dict],
    centers: Dict[str, Tuple[float, float]],
):
    clustered: Dict[str, List[Dict]] = {name: [] for name in centers}

    for entry in closures:
        try:
            lat = float(entry['location'][0])
            lng = float(entry['location'][1])
            regions = find_nearby_centers(lat, lng, centers)
            for region in regions:
                clustered[region].append(entry)
        except Exception as e:
            print(f"Skipping due to missing/invalid coordinates: {entry.get('name')} - {e}")

    for region, region_closures in clustered.items():
        if not region_closures:
            continue
        cal = create_calendar(region_closures, f"Hawker Centre Closures near {region}")
        filename = f"{region.replace(" ", "").strip().lower()}.ics"
        yield DataFile(filename, cal.serialize())

def hawker_center_closure():
    closures = fetch_all_closures()
    
    stations = mrt_stations()
    centers = {station['name']: (station['location'][0], station['location'][1]) for station in stations}

    return cluster_by_custom_centers(closures, centers)
