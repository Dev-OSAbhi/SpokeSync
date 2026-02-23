"""Transform one raw batch with Spark and load it into PostgreSQL."""

import argparse
import os
from datetime import date
from pathlib import Path


def build_tables(spark, trips_path, weather_path, batch_date):
    from pyspark.sql import functions as F

    trips = spark.read.option("header", True).csv(str(trips_path))
    trips = (
        trips.withColumn("started_at", F.to_timestamp("started_at"))
        .withColumn("ended_at", F.to_timestamp("ended_at"))
        .filter(F.to_date("started_at") == F.lit(batch_date.isoformat()))
        .withColumn("start_station_id", F.trim("start_station_id"))
        .withColumn("end_station_id", F.trim("end_station_id"))
        .filter(F.col("start_station_id").isNotNull() & (F.col("start_station_id") != ""))
        .filter(F.col("end_station_id").isNotNull() & (F.col("end_station_id") != ""))
    )

    weather_raw = spark.read.option("multiLine", True).json(str(weather_path))
    weather = (
        weather_raw.select(F.explode(F.arrays_zip(
            "daily.time", "daily.temperature_2m_mean", "daily.precipitation_sum", "daily.weather_code"
        )).alias("day"))
        .select(
            F.to_date("day.time").alias("calendar_date"),
            F.col("day.temperature_2m_mean").cast("decimal(6,2)").alias("temperature_c"),
            F.col("day.precipitation_sum").cast("decimal(8,2)").alias("precipitation_mm"),
            F.col("day.weather_code").cast("short").alias("weather_code"),
        )
        .filter(F.col("calendar_date") == F.lit(batch_date.isoformat()))
    )
    weather = weather.withColumn(
        "condition",
        F.when(F.col("weather_code") == 0, "Clear")
        .when(F.col("weather_code").isin(1, 2, 3), "Cloudy")
        .when(F.col("weather_code").isin(45, 48), "Fog")
        .when(F.col("weather_code").between(51, 67), "Rain")
        .when(F.col("weather_code").between(71, 86), "Snow")
        .when(F.col("weather_code") >= 95, "Thunderstorm")
        .otherwise("Other"),
    )
    weather = weather.withColumn("weather_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))

    trip_dates = F.to_date("started_at")
    facts = (
        trips.withColumn("calendar_date", trip_dates)
        .join(weather.select("calendar_date", "weather_key"), "calendar_date", "left")
        .select(
            F.trim("ride_id").alias("trip_id"),
            F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
            "weather_key", "start_station_id", "end_station_id", "started_at", "ended_at",
            F.round((F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")) / 60, 3)
            .cast("decimal(10,3)").alias("duration_minutes"),
            F.coalesce(F.nullif(F.trim("member_casual"), F.lit("")), F.lit("unknown")).alias("user_type"),
        )
    )

    starts = trips.select(
        F.col("start_station_id").alias("station_id"),
        F.col("start_station_name").alias("station_name"),
        F.col("start_lat").cast("double").alias("latitude"),
        F.col("start_lng").cast("double").alias("longitude"),
    )
    ends = trips.select(
        F.col("end_station_id").alias("station_id"),
        F.col("end_station_name").alias("station_name"),
        F.col("end_lat").cast("double").alias("latitude"),
        F.col("end_lng").cast("double").alias("longitude"),
    )
    stations = (
        starts.unionByName(ends)
        .withColumn("station_name", F.coalesce(F.nullif(F.trim("station_name"), F.lit("")), "station_id"))
        .groupBy("station_id")
        .agg(F.max("station_name").alias("station_name"),
             F.max("latitude").alias("latitude"),
             F.max("longitude").alias("longitude"))
    )

    dates = (
        trips.select(F.to_date("started_at").alias("calendar_date")).distinct()
        .select(
            F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
            "calendar_date",
            F.year("calendar_date").cast("short").alias("year"),
            F.month("calendar_date").cast("short").alias("month"),
            F.dayofmonth("calendar_date").cast("short").alias("day"),
            (((F.dayofweek("calendar_date") + 5) % 7) + 1).cast("short").alias("day_of_week"),
            (F.dayofweek("calendar_date").isin(1, 7)).alias("is_weekend"),
        )
    )
    return {"dim_date": dates, "dim_weather": weather, "dim_station": stations, "fact_trips": facts}


def check_quality(tables):
    from pyspark.sql import functions as F

    facts = tables["fact_trips"]
    if facts.limit(1).count() == 0:
        raise ValueError("No trips found for the requested batch date")
    if tables["dim_weather"].count() != 1:
        raise ValueError("Expected exactly one weather record for the batch date")
    if facts.filter(F.col("trip_id").isNull() | (F.col("trip_id") == "")).limit(1).count():
        raise ValueError("Trip IDs must be present")
    if facts.filter(F.col("weather_key").isNull()).limit(1).count():
        raise ValueError("Trips are missing matching weather")
    if facts.filter(
        F.col("ended_at").isNull() | F.col("duration_minutes").isNull()
        | (F.col("duration_minutes") <= 0) | (F.col("duration_minutes") >= 1440)
    ).limit(1).count():
        raise ValueError("Trip duration must be over zero and under 24 hours")
    if facts.groupBy("trip_id").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate trip IDs found")


TABLE_COLUMNS = {
    "dim_date": ("date_key", "calendar_date", "year", "month", "day", "day_of_week", "is_weekend"),
    "dim_weather": ("weather_key", "calendar_date", "temperature_c", "precipitation_mm", "weather_code", "condition"),
    "dim_station": ("station_id", "station_name", "latitude", "longitude"),
    "fact_trips": ("trip_id", "date_key", "weather_key", "start_station_id", "end_station_id",
                   "started_at", "ended_at", "duration_minutes", "user_type"),
}


def load_tables(tables, database_url, schema_path, batch_size=1000):
    import psycopg2
    from psycopg2.extras import execute_values

    with psycopg2.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(Path(schema_path).read_text(encoding="utf-8"))
            for table, columns in TABLE_COLUMNS.items():
                updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns[1:])
                statement = (
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
                    f"ON CONFLICT ({columns[0]}) DO UPDATE SET {updates}"
                )
                batch = []
                for row in tables[table].select(*columns).toLocalIterator():
                    batch.append(tuple(row[column] for column in columns))
                    if len(batch) == batch_size:
                        execute_values(cursor, statement, batch, page_size=batch_size)
                        batch.clear()
                if batch:
                    execute_values(cursor, statement, batch, page_size=batch_size)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-date", type=date.fromisoformat, required=True)
    parser.add_argument("--raw-dir", default=os.getenv("BIKE_RAW_DIR", "raw"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--schema", default=str(Path(__file__).with_name("data_model.sql")))
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")

    from pyspark.sql import SparkSession

    batch_dir = Path(args.raw_dir) / args.batch_date.isoformat()
    spark = (SparkSession.builder.appName("city-bike-share")
             .config("spark.sql.session.timeZone", os.getenv("BIKE_TIMEZONE", "America/New_York"))
             .getOrCreate())
    try:
        tables = build_tables(spark, batch_dir / "trips.csv", batch_dir / "weather.json", args.batch_date)
        check_quality(tables)
        load_tables(tables, args.database_url, args.schema)
        print(f"Loaded bike-share batch {args.batch_date}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
