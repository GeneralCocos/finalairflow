"""Airflow DAG for ingesting OpenAQ measurements for Moscow."""
from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from typing import Any, Dict, List

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extras import execute_values

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

OPENAQ_API_BASE_URL = os.environ.get("OPENAQ_API_BASE_URL", "https://api.openaq.org")
OPENAQ_API_KEY = os.environ.get("OPENAQ_API_KEY")
OPENAQ_CITY = os.environ.get("OPENAQ_CITY", "Moscow")
OPENAQ_COUNTRY = os.environ.get("OPENAQ_COUNTRY", "RU")
OPENAQ_LATEST_ENDPOINT = f"{OPENAQ_API_BASE_URL.rstrip('/')}/v2/latest"

POSTGRES_CONN_ID = os.environ.get("OPENAQ_POSTGRES_CONN_ID", "spacex_postgres")
OPENAQ_TABLE_NAME = os.environ.get("OPENAQ_TABLE_NAME", "openaq_moscow_measurements")


def _build_request_headers() -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if OPENAQ_API_KEY:
        headers["X-API-Key"] = OPENAQ_API_KEY
    return headers


logger = logging.getLogger(__name__)


def _flatten_measurements(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    measurements: List[Dict[str, Any]] = []
    fetched_at = pendulum.now("UTC")

    for location in results:
        location_name = location.get("location")
        coordinates = location.get("coordinates") or {}
        latitude = coordinates.get("latitude")
        longitude = coordinates.get("longitude")

        for measurement in location.get("measurements") or []:
            measured_at_raw = (
                measurement.get("lastUpdated")
                or measurement.get("date", {}).get("utc")
                or measurement.get("date", {}).get("local")
            )
            if not measured_at_raw:
                continue

            measured_at = pendulum.parse(measured_at_raw).in_timezone("UTC")

            parameter = measurement.get("parameter")
            value = measurement.get("value")
            unit = measurement.get("unit")
            if not location_name or parameter is None or value is None or unit is None:
                logger.debug(
                    "Skipping measurement with missing required fields: location=%s, parameter=%s",
                    location_name,
                    parameter,
                )
                continue

            measurements.append(
                {
                    "location": location_name,
                    "parameter": parameter,
                    "value": value,
                    "unit": unit,
                    "latitude": latitude,
                    "longitude": longitude,
                    "measured_at": measured_at,
                    "fetched_at": fetched_at,
                    "raw_payload": json.dumps(
                        {
                            "city": location.get("city"),
                            "country": location.get("country"),
                            "measurement": measurement,
                        }
                    ),
                }
            )

    return measurements


def fetch_and_store_latest_measurements() -> None:
    params = {
        "city": OPENAQ_CITY,
        "country": OPENAQ_COUNTRY,
        "limit": 100,
        "page": 1,
        "sort": "desc",
        "order_by": "datetime",
    }

    response = requests.get(
        OPENAQ_LATEST_ENDPOINT,
        params=params,
        headers=_build_request_headers(),
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    results = payload.get("results", [])
    measurements = _flatten_measurements(results)

    if not measurements:
        raise ValueError(
            "OpenAQ API did not return any measurements for city=%s, country=%s"
            % (OPENAQ_CITY, OPENAQ_COUNTRY)
        )

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    insert_sql = f"""
        INSERT INTO {OPENAQ_TABLE_NAME} (
            location,
            parameter,
            value,
            unit,
            latitude,
            longitude,
            measured_at,
            fetched_at,
            raw_payload
        ) VALUES %s
        ON CONFLICT (location, parameter, measured_at)
        DO UPDATE SET
            value = EXCLUDED.value,
            unit = EXCLUDED.unit,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            fetched_at = EXCLUDED.fetched_at,
            raw_payload = EXCLUDED.raw_payload;
    """

    rows = [
        (
            measurement["location"],
            measurement["parameter"],
            measurement["value"],
            measurement["unit"],
            measurement["latitude"],
            measurement["longitude"],
            measurement["measured_at"],
            measurement["fetched_at"],
            measurement["raw_payload"],
        )
        for measurement in measurements
    ]

    conn = hook.get_conn()
    with conn:
        with conn.cursor() as cursor:
            execute_values(cursor, insert_sql, rows)
    logger.info("Stored %d OpenAQ measurements for %s, %s", len(rows), OPENAQ_CITY, OPENAQ_COUNTRY)


with DAG(
    dag_id="openaq_moscow_to_postgres",
    default_args=DEFAULT_ARGS,
    description="Fetch the latest OpenAQ data for Moscow and store it in Postgres.",
    schedule="* * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["openaq", "moscow", "postgres"],
) as dag:
    create_table = PostgresOperator(
        task_id="create_openaq_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPENAQ_TABLE_NAME} (
            location TEXT NOT NULL,
            parameter TEXT NOT NULL,
            value DOUBLE PRECISION NOT NULL,
            unit TEXT NOT NULL,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            measured_at TIMESTAMPTZ NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL,
            raw_payload JSONB NOT NULL,
            CONSTRAINT openaq_measurements_pk PRIMARY KEY (location, parameter, measured_at)
        );
        """,
    )

    fetch_latest = PythonOperator(
        task_id="fetch_latest_measurements",
        python_callable=fetch_and_store_latest_measurements,
    )

    create_table >> fetch_latest

