from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values

from utils.coincap_postgres import ensure_coincap_postgres_connection

COINCAP_POSTGRES_CONN_ID = "coincap_postgres"
COINCAP_API_KEY_ENV = "COINCAP_API_KEY"
COINCAP_ASSET_IDS: List[str] = ["bitcoin", "ethereum", "tether"]
COINCAP_API_BASE_URL = os.environ.get("COINCAP_API_BASE_URL", "https://api.coincap.io")
COINCAP_API_HOST_HEADER = os.environ.get("COINCAP_API_HOST_HEADER")
COINCAP_ASSETS_ENDPOINT = f"{COINCAP_API_BASE_URL.rstrip('/')}/v2/assets"

ensure_coincap_postgres_connection()


def _get_api_key() -> str:
    try:
        return os.environ[COINCAP_API_KEY_ENV]
    except KeyError as exc:  # pragma: no cover - defensive guard for runtime envs
        raise RuntimeError(
            f"Environment variable {COINCAP_API_KEY_ENV} must be defined for CoinCap access"
        ) from exc


def _to_decimal(value: Optional[str]) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError):
        return None


def _to_int(value: Optional[str]) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_records(assets: Iterable[Dict[str, Any]], fetched_at: datetime) -> List[tuple]:
    rows: List[tuple] = []
    for asset in assets:
        rows.append(
            (
                asset.get("id"),
                asset.get("symbol"),
                asset.get("name"),
                _to_int(asset.get("rank")),
                _to_decimal(asset.get("priceUsd")),
                _to_decimal(asset.get("marketCapUsd")),
                _to_decimal(asset.get("changePercent24Hr")),
                _to_decimal(asset.get("supply")),
                _to_decimal(asset.get("volumeUsd24Hr")),
                fetched_at,
                Json(asset),
            )
        )
    return rows


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="coincap_assets_to_postgres",
    description="Fetch CoinCap metrics for the top assets and store raw snapshots in Postgres",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    default_args=default_args,
    tags=["coincap", "postgres"],
) as dag:

    @task
    def ensure_target_table() -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS coincap_asset_snapshots (
            asset_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            rank INTEGER,
            price_usd NUMERIC,
            market_cap_usd NUMERIC,
            change_percent_24h NUMERIC,
            supply NUMERIC,
            volume_usd_24h NUMERIC,
            fetched_at TIMESTAMPTZ NOT NULL,
            raw_payload JSONB NOT NULL,
            CONSTRAINT coincap_asset_snapshots_pk PRIMARY KEY (asset_id, fetched_at)
        );
        CREATE INDEX IF NOT EXISTS coincap_asset_snapshots_fetched_at_idx
            ON coincap_asset_snapshots (fetched_at DESC);
        """
        PostgresHook(postgres_conn_id=COINCAP_POSTGRES_CONN_ID).run(ddl)

    @task
    def fetch_top_assets() -> List[Dict[str, Any]]:
        headers = {
            "Authorization": f"Bearer {_get_api_key()}",
            "Accept": "application/json",
        }
        if COINCAP_API_HOST_HEADER:
            headers["Host"] = COINCAP_API_HOST_HEADER
        params = {"ids": ",".join(COINCAP_ASSET_IDS)}
        response = requests.get(
            COINCAP_ASSETS_ENDPOINT,
            headers=headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        assets = payload.get("data", []) if isinstance(payload, dict) else []
        if not assets:
            raise ValueError("CoinCap API returned an empty payload for assets")
        return assets

    @task
    def load_assets(assets: List[Dict[str, Any]]) -> int:
        if not assets:
            return 0

        fetched_at = datetime.now(timezone.utc)
        rows = _build_records(assets, fetched_at)

        insert_sql = """
        INSERT INTO coincap_asset_snapshots (
            asset_id,
            symbol,
            name,
            rank,
            price_usd,
            market_cap_usd,
            change_percent_24h,
            supply,
            volume_usd_24h,
            fetched_at,
            raw_payload
        ) VALUES %s
        ON CONFLICT (asset_id, fetched_at) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            rank = EXCLUDED.rank,
            price_usd = EXCLUDED.price_usd,
            market_cap_usd = EXCLUDED.market_cap_usd,
            change_percent_24h = EXCLUDED.change_percent_24h,
            supply = EXCLUDED.supply,
            volume_usd_24h = EXCLUDED.volume_usd_24h,
            raw_payload = EXCLUDED.raw_payload;
        """

        hook = PostgresHook(postgres_conn_id=COINCAP_POSTGRES_CONN_ID)
        conn = hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows)
        return len(rows)

    ensure_target_table() >> load_assets(fetch_top_assets())
