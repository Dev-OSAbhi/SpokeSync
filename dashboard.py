"""Streamlit dashboard for the bike-share warehouse."""

import os
from contextlib import closing
from datetime import timedelta

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st


STATION_SQL = """
WITH morning_moves AS (
    SELECT start_station_id AS station_id, 1 AS departures, 0 AS arrivals
    FROM fact_trips
    WHERE started_at >= %s::date AND started_at < (%s::date + INTERVAL '1 day')
      AND started_at::time >= TIME '07:00' AND started_at::time < TIME '10:00'
    UNION ALL
    SELECT end_station_id AS station_id, 0 AS departures, 1 AS arrivals
    FROM fact_trips
    WHERE ended_at >= %s::date AND ended_at < (%s::date + INTERVAL '1 day')
      AND ended_at::time >= TIME '07:00' AND ended_at::time < TIME '10:00'
)
SELECT s.station_id, s.station_name,
       SUM(m.departures) AS departures, SUM(m.arrivals) AS arrivals,
       SUM(m.departures - m.arrivals) AS net_outflow
FROM morning_moves m
JOIN dim_station s ON s.station_id = m.station_id
GROUP BY s.station_id, s.station_name
HAVING SUM(m.departures - m.arrivals) > 0
ORDER BY net_outflow DESC, s.station_name
LIMIT 5
"""

DAILY_SQL = """
SELECT d.calendar_date, w.temperature_c, w.precipitation_mm,
       COUNT(f.trip_id) AS trips
FROM dim_date d
JOIN dim_weather w ON w.calendar_date = d.calendar_date
LEFT JOIN fact_trips f ON f.date_key = d.date_key
WHERE d.calendar_date BETWEEN %s AND %s
GROUP BY d.calendar_date, w.temperature_c, w.precipitation_mm
ORDER BY d.calendar_date
"""


def query_frame(connection, sql, params):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


@st.cache_data(ttl=300, show_spinner=False)
def available_dates():
    with closing(psycopg2.connect(os.environ["DATABASE_URL"])) as connection, connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT MIN(calendar_date), MAX(calendar_date) FROM dim_date")
            return cursor.fetchone()


@st.cache_data(ttl=300, show_spinner=False)
def load_dashboard(start_date, end_date):
    with closing(psycopg2.connect(os.environ["DATABASE_URL"])) as connection, connection:
        stations = query_frame(connection, STATION_SQL, (start_date, end_date, start_date, end_date))
        daily = query_frame(connection, DAILY_SQL, (start_date, end_date))
    return stations, daily


def correlation(frame, column):
    valid = frame[[column, "trips"]].dropna()
    if len(valid) < 2 or valid[column].nunique() < 2 or valid["trips"].nunique() < 2:
        return "n/a"
    return f"{valid[column].corr(valid['trips']):+.2f}"


def main():
    st.set_page_config(page_title="City bike-share analytics", layout="wide")
    st.title("City bike-share analytics")
    st.caption("Daily trips, weather, and morning station outflow")

    if not os.getenv("DATABASE_URL"):
        st.error("Set DATABASE_URL before starting the dashboard.")
        st.stop()

    first_date, last_date = available_dates()
    if last_date is None:
        st.info("The warehouse has no trips yet. Run an extraction and load a batch first.")
        st.stop()

    selected_dates = st.date_input(
        "Date range", value=(max(first_date, last_date - timedelta(days=29)), last_date),
        min_value=first_date, max_value=last_date,
    )
    if len(selected_dates) != 2:
        st.info("Select both a start and end date.")
        st.stop()
    start_date, end_date = selected_dates
    if start_date > end_date:
        st.warning("Choose an end date on or after the start date.")
        st.stop()

    stations, daily = load_dashboard(start_date, end_date)
    if daily.empty:
        st.info("No trips were loaded for this date range.")
        st.stop()
    daily["temperature_c"] = pd.to_numeric(daily["temperature_c"])
    daily["precipitation_mm"] = pd.to_numeric(daily["precipitation_mm"])
    daily["trips"] = pd.to_numeric(daily["trips"])

    total, rain_corr, temp_corr = st.columns(3)
    total.metric("Trips", f"{daily['trips'].sum():,}")
    rain_corr.metric("Rain / trips correlation", correlation(daily, "precipitation_mm"))
    temp_corr.metric("Temperature / trips correlation", correlation(daily, "temperature_c"))

    st.subheader("Morning station depletion")
    st.caption("Top five stations by departures minus arrivals, 07:00–10:00 local time. Positive values indicate net bike loss.")
    if stations.empty:
        st.info("No stations had positive net outflow in this period.")
    else:
        chart = px.bar(
            stations.sort_values("net_outflow"), x="net_outflow", y="station_name",
            orientation="h", hover_data=["station_id", "departures", "arrivals"],
            labels={"net_outflow": "Net bike outflow", "station_name": "Station"},
        )
        st.plotly_chart(chart, width="stretch")

    st.subheader("Trips and weather")
    st.plotly_chart(px.line(daily, x="calendar_date", y="trips",
                            labels={"calendar_date": "Date", "trips": "Trips"}), width="stretch")
    left, right = st.columns(2)
    with left:
        st.plotly_chart(px.scatter(
            daily, x="precipitation_mm", y="trips", hover_data=["calendar_date"],
            labels={"precipitation_mm": "Precipitation (mm)", "trips": "Trips"},
            title="Rain and ridership",
        ), width="stretch")
    with right:
        st.plotly_chart(px.scatter(
            daily, x="temperature_c", y="trips", hover_data=["calendar_date"],
            labels={"temperature_c": "Mean temperature (°C)", "trips": "Trips"},
            title="Temperature and ridership",
        ), width="stretch")
    st.caption("Correlations describe the selected days; they do not establish a causal effect.")


if __name__ == "__main__":
    main()
