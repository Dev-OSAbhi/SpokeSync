import json
import zipfile
from datetime import date

import pytest

from extract import save_trips, save_weather, validate_weather


HEADER = (
    "ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,"
    "end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual\n"
)
ROW = "r1,classic_bike,2024-06-01 08:00:00,2024-06-01 08:12:00,A,a,B,b,40,-74,40.1,-74.1,member\n"


def weather_payload():
    return {"daily": {
        "time": ["2024-06-01"], "temperature_2m_mean": [22.1],
        "precipitation_sum": [0], "weather_code": [1],
    }}


def test_extracts_csv_from_zip(tmp_path):
    archive_path = tmp_path / "trips.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("folder/trips.csv", HEADER + ROW)
    destination = tmp_path / "raw" / "trips.csv"

    save_trips(str(archive_path), destination)

    assert destination.read_text() == HEADER + ROW


def test_rejects_missing_trip_columns(tmp_path):
    source = tmp_path / "bad.csv"
    source.write_text("ride_id,started_at\nr1,2024-06-01\n")

    with pytest.raises(ValueError, match="missing columns"):
        save_trips(str(source), tmp_path / "raw" / "trips.csv")


def test_saves_matching_weather(tmp_path):
    source = tmp_path / "weather.json"
    source.write_text(json.dumps(weather_payload()))
    destination = tmp_path / "raw" / "weather.json"

    save_weather(date(2024, 6, 1), 40, -74, "America/New_York", destination, source)

    assert json.loads(destination.read_text()) == weather_payload()


def test_rejects_weather_for_another_day():
    with pytest.raises(ValueError, match="no data"):
        validate_weather(weather_payload(), date(2024, 6, 2))
