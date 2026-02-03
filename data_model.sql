CREATE TABLE IF NOT EXISTS dim_date (
    date_key integer PRIMARY KEY,
    calendar_date date NOT NULL UNIQUE,
    year smallint NOT NULL,
    month smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
    day smallint NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week smallint NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    is_weekend boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_weather (
    weather_key integer PRIMARY KEY,
    calendar_date date NOT NULL UNIQUE,
    temperature_c numeric(6, 2),
    precipitation_mm numeric(8, 2),
    weather_code smallint,
    condition text,
    CONSTRAINT precipitation_nonnegative CHECK (precipitation_mm IS NULL OR precipitation_mm >= 0)
);

CREATE TABLE IF NOT EXISTS dim_station (
    station_id text PRIMARY KEY,
    station_name text NOT NULL,
    latitude double precision,
    longitude double precision,
    CONSTRAINT station_latitude_range CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    CONSTRAINT station_longitude_range CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180)
);

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id text PRIMARY KEY,
    date_key integer NOT NULL REFERENCES dim_date (date_key),
    weather_key integer NOT NULL REFERENCES dim_weather (weather_key),
    start_station_id text NOT NULL REFERENCES dim_station (station_id),
    end_station_id text NOT NULL REFERENCES dim_station (station_id),
    started_at timestamp NOT NULL,
    ended_at timestamp NOT NULL,
    duration_minutes numeric(10, 3) NOT NULL CHECK (duration_minutes > 0 AND duration_minutes < 1440),
    user_type text NOT NULL,
    CONSTRAINT trip_time_order CHECK (ended_at > started_at)
);

CREATE INDEX IF NOT EXISTS fact_trips_date_idx ON fact_trips (date_key);
CREATE INDEX IF NOT EXISTS fact_trips_start_station_time_idx ON fact_trips (start_station_id, started_at);
CREATE INDEX IF NOT EXISTS fact_trips_end_station_time_idx ON fact_trips (end_station_id, ended_at);
CREATE INDEX IF NOT EXISTS dim_date_calendar_date_idx ON dim_date (calendar_date);
CREATE INDEX IF NOT EXISTS dim_weather_calendar_date_idx ON dim_weather (calendar_date);
