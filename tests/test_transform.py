import json
from datetime import date

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from transform_and_load import build_tables, check_quality
from test_extract import HEADER, ROW, weather_payload


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("bike-share-tests")
               .config("spark.sql.session.timeZone", "America/New_York")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def tables_for(spark, tmp_path, rows):
    trips = tmp_path / "trips.csv"
    weather = tmp_path / "weather.json"
    trips.write_text(HEADER + "".join(rows))
    weather.write_text(json.dumps(weather_payload()))
    return build_tables(spark, trips, weather, date(2024, 6, 1))


def test_builds_star_schema_for_one_day(spark, tmp_path):
    wrong_day = ROW.replace("r1", "r2").replace("2024-06-01", "2024-06-02")
    no_station = ROW.replace("r1", "r3").replace(",A,a,B,b,", ",A,,B,b,")
    tables = tables_for(spark, tmp_path, [ROW, wrong_day, no_station])

    check_quality(tables)

    fact = tables["fact_trips"].collect()[0]
    assert fact.trip_id == "r1"
    assert fact.date_key == 20240601
    assert fact.weather_key == 20240601
    assert float(fact.duration_minutes) == 12
    assert fact.started_at.hour == 8
    assert fact.ended_at.hour == 8
    assert tables["dim_date"].collect()[0].day_of_week == 6
    assert {row.station_id for row in tables["dim_station"].collect()} == {"a", "b"}


@pytest.mark.parametrize("rows, error", [
    ([ROW, ROW], "Duplicate"),
    ([ROW.replace("08:12:00", "08:00:00")], "duration"),
    ([ROW.replace("r1,", ",", 1)], "IDs"),
])
def test_quality_rejects_bad_trips(spark, tmp_path, rows, error):
    tables = tables_for(spark, tmp_path, rows)

    with pytest.raises(ValueError, match=error):
        check_quality(tables)
