"""
DAG: portfolio_schedule_snapshot
------------------------------
Triggers hourly batch snapshot for all eligible portfolios via the NestJS API.
Runs every hour (24/7, UTC scheduler cadence).

This DAG calls the NestJS API's batch snapshot endpoint which:
1. Fetches all portfolios from the database
2. Captures a snapshot for each portfolio (fire-and-forget)
3. Returns immediately with job status

Architecture Notes:
- The scheduling responsibility is with Airflow (see docs in this repository)
- The business logic (iteration, snapshot capture) stays in NestJS
- The API returns 202 Accepted immediately to avoid Airflow timeouts
- Market-session filtering is NOT applied at DAG level in this phase
"""

import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.sdk import task

from dags.etl_modules.notifications import (
    send_failure_notification,
    send_success_notification,
)

# Configuration
API_BASE_URL = os.getenv("NESTJS_API_URL", "http://localhost:3001")
DATA_PIPELINE_API_KEY = os.getenv("DATA_PIPELINE_API_KEY", "")

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="portfolio_schedule_snapshot",
    default_args=default_args,
    description=(
        "Capture hourly portfolio snapshots for historical performance tracking"
    ),
    schedule="@hourly",  # Runs every hour (UTC)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["portfolio", "snapshot", "batch", "hourly"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task
    def trigger_batch_snapshot():
        """
        Call the NestJS API to trigger batch snapshot for all portfolios.
        The API returns 202 Accepted immediately (fire-and-forget pattern).
        """
        if not DATA_PIPELINE_API_KEY:
            raise ValueError("DATA_PIPELINE_API_KEY environment variable is not set")

        url = f"{API_BASE_URL}/api/v1/portfolios/snapshots/batch"
        headers = {
            "X-Api-Key": DATA_PIPELINE_API_KEY,
            "Content-Type": "application/json",
        }

        print(f"Triggering batch snapshot at {url}...")

        try:
            response = requests.post(url, headers=headers, timeout=30)
            response.raise_for_status()

            result = response.json()
            data = result.get("data", {})

            print("Batch snapshot triggered successfully:")
            print(f"  - Started: {data.get('started')}")
            print(f"  - Portfolio Count: {data.get('portfolioCount')}")
            print(f"  - Message: {data.get('message')}")

            if not data.get("started"):
                raise Exception(
                    f"Batch snapshot failed to start: {data.get('message')}"
                )

            return data

        except requests.exceptions.RequestException as e:
            print(f"Failed to trigger batch snapshot: {str(e)}")
            raise

    # Execute the task
    trigger_batch_snapshot()
