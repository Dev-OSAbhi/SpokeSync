"""Save a trip batch and its matching daily weather in the raw layer."""

import argparse
import json
import os
import shutil
import tempfile
import zipfile
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
TRIP_COLUMNS = {
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name",
    "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng",
    "member_casual",
}


def session_with_retries():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temp:
        temp_path = Path(temp.name)
        try:
            with open(source, "rb") as infile:
                shutil.copyfileobj(infile, temp)
            os.replace(temp_path, destination)
        finally:
            temp_path.unlink(missing_ok=True)


def save_trips(source, destination, session=None):
    source_path = Path(source)
    parsed = urlparse(source)
    if parsed.scheme in ("http", "https"):
        session = session or session_with_retries()
        with tempfile.NamedTemporaryFile(suffix=".download", delete=False) as temp:
            download = Path(temp.name)
            try:
                with session.get(source, stream=True, timeout=(10, 120)) as response:
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        temp.write(chunk)
            except Exception:
                download.unlink(missing_ok=True)
                raise
    elif source_path.is_file():
        download = source_path
    else:
        raise FileNotFoundError(source)

    try:
        if zipfile.is_zipfile(download):
            with zipfile.ZipFile(download) as archive:
                members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if len(members) != 1:
                    raise ValueError("Trip ZIP must contain exactly one CSV file")
                destination.parent.mkdir(parents=True, exist_ok=True)
                with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temp:
                    temp_path = Path(temp.name)
                    try:
                        with archive.open(members[0]) as infile:
                            shutil.copyfileobj(infile, temp)
                        temp.flush()
                        validate_trip_columns(temp_path)
                        os.replace(temp_path, destination)
                    finally:
                        temp_path.unlink(missing_ok=True)
        else:
            validate_trip_columns(download)
            atomic_copy(download, destination)
    finally:
        if parsed.scheme in ("http", "https"):
            download.unlink(missing_ok=True)


def validate_trip_columns(path):
    columns = set(pd.read_csv(path, nrows=0).columns)
    missing = TRIP_COLUMNS - columns
    if missing:
        raise ValueError(f"Trip CSV is missing columns: {', '.join(sorted(missing))}")


def validate_weather(payload, batch_date):
    daily = payload.get("daily", {})
    fields = ("time", "temperature_2m_mean", "precipitation_sum", "weather_code")
    if any(field not in daily for field in fields):
        raise ValueError("Weather response is missing daily fields")
    count = len(daily["time"])
    if count == 0 or any(len(daily[field]) != count for field in fields):
        raise ValueError("Weather response has inconsistent daily arrays")
    if batch_date.isoformat() not in daily["time"]:
        raise ValueError(f"Weather response has no data for {batch_date}")


def save_weather(batch_date, latitude, longitude, timezone, destination, weather_file=None, session=None):
    if weather_file:
        with open(weather_file, encoding="utf-8") as infile:
            payload = json.load(infile)
    else:
        session = session or session_with_retries()
        response = session.get(
            ARCHIVE_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "start_date": batch_date.isoformat(),
                "end_date": batch_date.isoformat(),
                "daily": "temperature_2m_mean,precipitation_sum,weather_code",
                "timezone": timezone,
            },
            timeout=(10, 60),
        )
        response.raise_for_status()
        payload = response.json()
    validate_weather(payload, batch_date)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=destination.parent, delete=False) as temp:
        temp_path = Path(temp.name)
        try:
            json.dump(payload, temp, indent=2)
            temp.write("\n")
            temp.flush()
            os.replace(temp_path, destination)
        finally:
            temp_path.unlink(missing_ok=True)


def extract(batch_date, trip_source, raw_dir, latitude, longitude, timezone, weather_file=None):
    batch_dir = Path(raw_dir) / batch_date.isoformat()
    trips = batch_dir / "trips.csv"
    weather = batch_dir / "weather.json"
    save_trips(trip_source, trips)
    save_weather(batch_date, latitude, longitude, timezone, weather, weather_file)
    return trips, weather


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-date", type=date.fromisoformat, required=True)
    parser.add_argument("--trip-source", required=True, help="Citi Bike CSV/ZIP path or URL")
    parser.add_argument("--raw-dir", default=os.getenv("BIKE_RAW_DIR", "raw"))
    parser.add_argument("--latitude", type=float, default=float(os.getenv("BIKE_LATITUDE", "40.7128")))
    parser.add_argument("--longitude", type=float, default=float(os.getenv("BIKE_LONGITUDE", "-74.0060")))
    parser.add_argument("--timezone", default=os.getenv("BIKE_TIMEZONE", "America/New_York"))
    parser.add_argument("--weather-file", help="Local Open-Meteo JSON for offline runs")
    args = parser.parse_args()
    trips, weather = extract(
        args.batch_date, args.trip_source, args.raw_dir,
        args.latitude, args.longitude, args.timezone, args.weather_file,
    )
    print(f"Saved {trips} and {weather}")


if __name__ == "__main__":
    main()
