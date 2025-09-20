import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
client_id = os.getenv("FROST_CLIENT_ID", "")

frost_endpoint = "https://frost.met.no/observations/v0.jsonld"
frost_sources = "https://frost.met.no/sources/v0.jsonld"

# Sanity check to test if client ID is present, and return it
def _auth():
    if not client_id:
        raise RuntimeError("Missing Frost API Client ID")
    return (client_id, "")

# Mostly followed the guide here: https://frost.met.no/python_example.html

# Main function to fetch data from station and present into a pandas dataframe
def fetch_data_observations(
    sources: str,
    elements: str,
    referencetime: str,
    qualities: str = "0,1,2,3,4",
    limit: int = 100000,
) -> pd.DataFrame:

    params = {
        "sources": sources,
        "elements": elements,
        "referencetime": referencetime,
        "qualities": qualities,
        "limit": limit,
    }

    auth = _auth()
    rows = []

    while True:
        # Issue HTTP GET request
        r = requests.get(frost_endpoint, params=params, auth=auth, timeout=60)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            print("URL:", r.url)
            print("Status:", r.status_code)
            print("Body:", r.text[:1000])  # Frost usually explains what's wrong
            raise
        
        # Extract JSON data
        j = r.json()
        data = j.get("data", [])

        for item in data:
            t = item["referenceTime"]
            src = item["sourceId"]

            for obs in item["observations"]:
                rows.append({
                    "time": t,
                    "source": src,
                    "element": obs["elementId"],
                    "value": obs.get("value"),
                    "unit": obs.get("unit"),
                })
        
        # Pagination, add new page if needed
        if "next" in j.get("page", {}):
            params["page"] += 1
        else:
            break

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"])
    return df

def pivot_table_elements(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.pivot_table(index="time", columns="element", values="value", aggfunc="mean")
    out.sort_index(inplace=True)
    return out.reset_index()

# Function to search for station ID using name
def search_station_by_name(
        text: str,
        types: str = "SensorSystem",
)-> pd.DataFrame:

    params = {
        "types": types,
        "fields": "id,name,shortName,geometry,masl,municipality,county,country",
    }

    r = requests.get(frost_sources, params = params, auth = _auth(), timeout=30)

    try:
        r.raise_for_status()
    except requests.HTTPError:
        print("URL:", r.url)
        print("Status:", r.status_code)
        print("Body:", r.text[:1000])
        raise

    data = r.json().get("data",[])
    rows = []
    for item in data:
        name = (item.get("name") or "") + " " + (item.get("shortName") or "")
        if text.lower() in name.lower():
            geom = item.get("geometry", {}).get("coordinates", [None, None])
            rows.append({
                "id": item.get("id"),
                "name": item.get("name") or item.get("shortName"),
                "lon": geom[0],
                "lat": geom[1],
                "masl": item.get("masl"),
                "municipality": item.get("municipality"),
                "county": item.get("county"),
                "country": item.get("country"),
            })
    return pd.DataFrame(rows)