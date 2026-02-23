# City bike-share analytics pipeline

An end-to-end batch pipeline for studying station imbalances and the relationship between weather and ridership. It ingests modern Citi Bike trip CSVs and daily Open-Meteo weather, transforms them with PySpark, validates the results, loads a PostgreSQL star schema, and serves a Streamlit dashboard.

```text
Citi Bike CSV/ZIP ─┐
                   ├─ extract.py ─ raw/YYYY-MM-DD ─ transform_and_load.py ─ PostgreSQL ─ dashboard.py
Open-Meteo API ────┘                              PySpark + quality checks
                           orchestration_dag.py schedules the two jobs daily
```

## Requirements

- Python 3.10 or newer and Java 17 or newer for PySpark 4.x.
- PostgreSQL 16 (the included `compose.yaml` starts one), or an existing PostgreSQL database.
- Docker is optional if PostgreSQL is already available.
- An Airflow 3 installation is optional for scheduled runs. The scripts also run directly.

Install Python dependencies in a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

Set `JAVA_HOME` if Java is not on your path. On machines with several Java versions, point it to a Java 17 or 21 installation.

## Run the included sample

The checked-in `sample_data/` files contain seven days of **synthetic** trips and weather. They require no network access. The PDF [dashboard_preview.pdf](dashboard_preview.pdf) shows the dashboard metrics for this sample.

Start PostgreSQL and set the connection string:

```bash
docker compose up -d postgres
export DATABASE_URL=postgresql://bikeshare:bikeshare@localhost:5432/bikeshare
```

For each sample day, save a raw batch, validate it, and load it:

```bash
for day_dir in sample_data/*; do
  day=${day_dir##*/}
  python extract.py --batch-date "$day" --trip-source "$day_dir/trips.csv" \
    --weather-file "$day_dir/weather.json"
  python transform_and_load.py --batch-date "$day"
done
streamlit run dashboard.py
```

The loader creates tables from `data_model.sql` on its first run. Re-running a batch upserts its facts and dimensions by key, so the counts stay stable. To regenerate the sample and preview, run `python generate_demo.py`; Matplotlib is included in `requirements-dev.txt` for this purpose.

## Run with public trip data

Download a modern Citi Bike CSV or single-CSV ZIP from the [Citi Bike system data page](https://citibikenyc.com/system-data). It must use the `ride_id`, `started_at`, `ended_at`, station, coordinate, and `member_casual` columns listed in `extract.py`. Set the batch date to a day represented in the file. A monthly source is fine: Spark selects only trips whose **start** is on the requested day.

```bash
export DATABASE_URL=postgresql://bikeshare:bikeshare@localhost:5432/bikeshare
python extract.py --batch-date 2024-06-01 --trip-source /path/to/citibike-file.csv.zip
python transform_and_load.py --batch-date 2024-06-01
streamlit run dashboard.py
```

Extraction calls [Open-Meteo's historical weather API](https://open-meteo.com/en/docs/historical-weather-api) for older days and its [forecast API](https://open-meteo.com/en/docs) for the most recent seven days. Daily values use the configured local timezone. No API key is needed for noncommercial use under Open-Meteo's terms. Set `BIKE_LATITUDE`, `BIKE_LONGITUDE`, and `BIKE_TIMEZONE` to match the trip system; the defaults are for New York City. `--weather-file` accepts an Open-Meteo JSON response for offline runs.

## Scheduling

`orchestration_dag.py` defines an Airflow 3 DAG that runs at 07:00 in `BIKE_TIMEZONE` and processes the preceding day. Install Airflow 3 and its standard provider in an Airflow environment using the [official Airflow installation constraints](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html). Install this project's `requirements.txt` in the worker environment too, and place or symlink the DAG file in Airflow's DAG folder. Workers need access to this repository, the shared `raw/` directory, and PostgreSQL.

Set `DATABASE_URL` and `BIKE_TRIP_SOURCE_TEMPLATE` for the worker. The template accepts `{date}` (`YYYY-MM-DD`), `{year}`, `{month}`, and `{yyyymm}`. For example, a source that publishes one monthly ZIP could use `https://your-source.example/{yyyymm}-trips.zip`. Keep `BIKE_TIMEZONE`, `BIKE_LATITUDE`, and `BIKE_LONGITUDE` consistent across the extract and load tasks. Airflow retries failed tasks twice; the loader uses one PostgreSQL transaction per batch.

## Warehouse and quality rules

`fact_trips` has one row per ride. `dim_date` is keyed by the ride's local start date; `dim_weather` has one daily observation for the configured city and uses the same `YYYYMMDD` key; `dim_station` is keyed by the source station ID. The fact stores both station IDs and both local timestamps so the dashboard can count morning departures and arrivals separately. Dates and timestamps are interpreted in `BIKE_TIMEZONE`. The model uses local timestamps without an offset, matching the source CSV convention.

Spark drops rows with missing start or end station IDs. It rejects a batch with no trips, missing trip IDs, duplicate trip IDs, unmatched weather, or trip durations outside `(0, 1440)` minutes. These checks run before any warehouse write. PostgreSQL enforces primary keys, foreign keys, coordinate ranges, positive durations, and time order. Dimension rows load before facts; loading is batched and transactional. The dashboard shows the five stations with the largest positive net morning outflow and daily trip relationships with precipitation and mean temperature. Correlation is descriptive and can be unstable for short date ranges.

Run tests with `python -m pytest -q`. Spark tests require PySpark and Java; without them, pytest skips those tests. The sample preview is synthetic and is not evidence of real-world demand or weather effects.

## Data engineering skills shown

The project uses Python and SQL for extraction, warehouse design, and loading; PySpark for large CSV transformation; explicit quality gates before loading; a dimensional star schema for analysis; Airflow for orchestration; and PostgreSQL indexes plus a Streamlit dashboard for reporting. These map to the Python, SQL, Spark, ETL, data modeling, quality, and orchestration requirements in the campus hire role described in the project plan.
