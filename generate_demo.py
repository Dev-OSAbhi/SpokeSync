"""Create a small synthetic input set and a PDF dashboard preview."""

import argparse
import csv
import json
from datetime import date, datetime, timedelta
from pathlib import Path


STATIONS = [
    ("S1", "Riverside", 40.72, -74.02),
    ("S2", "Hillcrest", 40.73, -74.01),
    ("S3", "Parkside", 40.71, -74.03),
    ("S4", "Central", 40.71, -74.00),
    ("S5", "Market", 40.70, -74.01),
]
TEMPERATURES = [21, 24, 27, 19, 22, 26, 18]
RAIN = [0, 0, 4.2, 12.5, 1, 0, 8]
CODES = [1, 0, 61, 63, 3, 2, 61]
COUNTS = [12, 15, 9, 6, 11, 16, 7]
FIELDS = [
    "ride_id", "rideable_type", "started_at", "ended_at", "start_station_name",
    "start_station_id", "end_station_name", "end_station_id", "start_lat", "start_lng",
    "end_lat", "end_lng", "member_casual",
]


def write_demo(output_dir):
    first_day = date(2024, 6, 1)
    for day_number, count in enumerate(COUNTS):
        day = first_day + timedelta(days=day_number)
        day_dir = output_dir / day.isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        with (day_dir / "trips.csv").open("w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=FIELDS)
            writer.writeheader()
            for ride_number in range(count):
                morning = ride_number < (count * 2 // 3)
                if morning:
                    start = STATIONS[ride_number % 3]
                    end = STATIONS[3 + ride_number % 2]
                    hour = 7 + ride_number % 3
                else:
                    start = STATIONS[3 + ride_number % 2]
                    end = STATIONS[ride_number % 3]
                    hour = 16 + ride_number % 3
                started = datetime(day.year, day.month, day.day, hour, ride_number % 60)
                ended = started + timedelta(minutes=12 + ride_number % 18)
                writer.writerow({
                    "ride_id": f"demo-{day:%Y%m%d}-{ride_number:03d}",
                    "rideable_type": "classic_bike", "started_at": started.isoformat(sep=" "),
                    "ended_at": ended.isoformat(sep=" "),
                    "start_station_name": start[1], "start_station_id": start[0],
                    "end_station_name": end[1], "end_station_id": end[0],
                    "start_lat": start[2], "start_lng": start[3],
                    "end_lat": end[2], "end_lng": end[3],
                    "member_casual": "member" if ride_number % 3 else "casual",
                })
        weather = {"daily": {
            "time": [day.isoformat()], "temperature_2m_mean": [TEMPERATURES[day_number]],
            "precipitation_sum": [RAIN[day_number]], "weather_code": [CODES[day_number]],
        }}
        (day_dir / "weather.json").write_text(json.dumps(weather, indent=2) + "\n", encoding="utf-8")


def render_preview(output_dir, pdf_path):
    import matplotlib.pyplot as plt
    import pandas as pd

    rows = []
    daily = []
    for day_dir in sorted(output_dir.iterdir()):
        if not day_dir.is_dir():
            continue
        rows.extend(pd.read_csv(day_dir / "trips.csv").to_dict("records"))
        weather = json.loads((day_dir / "weather.json").read_text())
        daily.append({
            "date": day_dir.name, "trips": len(pd.read_csv(day_dir / "trips.csv")),
            "rain": weather["daily"]["precipitation_sum"][0],
            "temperature": weather["daily"]["temperature_2m_mean"][0],
        })
    trips = pd.DataFrame(rows)
    days = pd.DataFrame(daily)
    started = pd.to_datetime(trips["started_at"])
    ended = pd.to_datetime(trips["ended_at"])
    departures = trips.loc[started.dt.hour.between(7, 9), "start_station_name"].value_counts()
    arrivals = trips.loc[ended.dt.hour.between(7, 9), "end_station_name"].value_counts()
    outflow = departures.sub(arrivals, fill_value=0).sort_values(ascending=False)
    outflow = outflow[outflow > 0].head(5)

    fig = plt.figure(figsize=(11.7, 8.3), layout="constrained")
    grid = fig.add_gridspec(3, 2, height_ratios=[0.7, 2, 2])
    heading = fig.add_subplot(grid[0, :])
    heading.axis("off")
    heading.text(0, 0.85, "City bike-share analytics", fontsize=24, weight="bold")
    heading.text(0, 0.42, "Synthetic sample · 1–7 June 2024 · 07:00–10:00 morning window", fontsize=11)
    heading.text(0, 0.02, f"{int(days['trips'].sum())} trips    ·    Rain / trips: {days['rain'].corr(days['trips']):+.2f}    ·    Temperature / trips: {days['temperature'].corr(days['trips']):+.2f}", fontsize=12)

    station_axis = fig.add_subplot(grid[1, 0])
    station_axis.barh(outflow.index[::-1], outflow.values[::-1], color="#227c9d")
    station_axis.set_title("Morning station depletion")
    station_axis.set_xlabel("Net bike outflow")
    trend_axis = fig.add_subplot(grid[1, 1])
    trend_axis.plot(days["date"].str[-2:], days["trips"], marker="o", color="#227c9d")
    trend_axis.set_title("Daily trips")
    trend_axis.set_xlabel("June 2024")
    trend_axis.set_ylabel("Trips")

    rain_axis = fig.add_subplot(grid[2, 0])
    rain_axis.scatter(days["rain"], days["trips"], color="#fe6d73", s=70)
    rain_axis.set_title("Rain and ridership")
    rain_axis.set_xlabel("Precipitation (mm)")
    rain_axis.set_ylabel("Trips")
    temp_axis = fig.add_subplot(grid[2, 1])
    temp_axis.scatter(days["temperature"], days["trips"], color="#f9a825", s=70)
    temp_axis.set_title("Temperature and ridership")
    temp_axis.set_xlabel("Mean temperature (°C)")
    temp_axis.set_ylabel("Trips")
    fig.savefig(pdf_path, format="pdf")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("sample_data"))
    parser.add_argument("--pdf", type=Path, default=Path("dashboard_preview.pdf"))
    args = parser.parse_args()
    write_demo(args.output_dir)
    render_preview(args.output_dir, args.pdf)
    print(f"Wrote synthetic sample to {args.output_dir} and preview to {args.pdf}")


if __name__ == "__main__":
    main()
