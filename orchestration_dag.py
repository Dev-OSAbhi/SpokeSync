"""Daily bike-share ingestion for an Airflow worker with shared raw storage."""

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


PROJECT_DIR = Path(__file__).resolve().parent
TIMEZONE = os.getenv("BIKE_TIMEZONE", "America/New_York")


def batch_date_for(context):
    return context["data_interval_end"].in_timezone(TIMEZONE).date() - timedelta(days=1)


def extract_batch(**context):
    batch_date = batch_date_for(context)
    source_template = os.environ["BIKE_TRIP_SOURCE_TEMPLATE"]
    source = source_template.format(
        date=batch_date.isoformat(), year=batch_date.strftime("%Y"),
        month=batch_date.strftime("%m"), yyyymm=batch_date.strftime("%Y%m"),
    )
    subprocess.run(
        [sys.executable, str(PROJECT_DIR / "extract.py"), "--batch-date", batch_date.isoformat(),
         "--trip-source", source],
        cwd=PROJECT_DIR,
        check=True,
    )


def transform_batch(**context):
    batch_date = batch_date_for(context)
    subprocess.run(
        [sys.executable, str(PROJECT_DIR / "transform_and_load.py"),
         "--batch-date", batch_date.isoformat()],
        cwd=PROJECT_DIR,
        check=True,
    )


with DAG(
    dag_id="city_bike_share_daily",
    start_date=datetime(2024, 1, 1, tzinfo=ZoneInfo(TIMEZONE)),
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=15)},
    tags=["bike-share", "analytics"],
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract_batch)
    transform_task = PythonOperator(task_id="transform_and_load", python_callable=transform_batch)
    extract_task >> transform_task
