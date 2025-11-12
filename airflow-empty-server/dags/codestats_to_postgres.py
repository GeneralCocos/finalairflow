from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict, Optional

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

API_BASE_URL = "https://api.spacexdata.com/v4"
LATEST_LAUNCH_ENDPOINT = "launches/latest"
POSTGRES_CONN_ID = os.getenv("SPACEX_POSTGRES_CONN_ID", "spacex_postgres")
TARGET_TABLE = "spacex_launch_snapshots"


def _build_headers() -> Dict[str, str]:
    """Return headers for SpaceX API requests."""
    return {
        "Accept": "application/json",
        "User-Agent": "airflow-spacex-ingest/1.0",
    }


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp into an aware ``datetime`` instance."""

    if not value:
        return None

    try:
        # The SpaceX API returns timestamps in UTC with a trailing ``Z``.
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


@task
def fetch_latest_launch() -> Dict[str, Any]:
    """Fetch metadata about the most recent SpaceX launch."""
    response = requests.get(
        f"{API_BASE_URL}/{LATEST_LAUNCH_ENDPOINT}",
        headers=_build_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


@task
def load_launch_into_postgres(launch: Dict[str, Any]) -> None:
    """Store the retrieved launch metadata in PostgreSQL as a JSON document."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            launch_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            date_utc TIMESTAMPTZ,
            payload JSONB NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL
        )
        """
    )

    launch_id = launch.get("id")
    launch_name = launch.get("name") or "Unknown launch"
    launch_date = _parse_datetime(launch.get("date_utc"))
    fetched_at = datetime.now(timezone.utc)

    hook.run(
        f"""
        INSERT INTO {TARGET_TABLE} (launch_id, name, date_utc, payload, fetched_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (launch_id) DO UPDATE SET
            name = EXCLUDED.name,
            date_utc = EXCLUDED.date_utc,
            payload = EXCLUDED.payload,
            fetched_at = EXCLUDED.fetched_at
        """,
        parameters=(
            launch_id,
            launch_name,
            launch_date,
            Json(launch),
            fetched_at,
        ),
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
        dag_id="spacex_launch_to_postgres",
        description="Load the latest SpaceX launch metadata into PostgreSQL every minute",
        schedule_interval="* * * * *",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["spacex", "postgres"],
) as dag:
    latest_launch = fetch_latest_launch()
    load_launch_into_postgres(latest_launch)
