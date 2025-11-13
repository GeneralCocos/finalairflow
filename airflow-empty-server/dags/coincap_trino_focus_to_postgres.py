from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.trino.hooks.trino import TrinoHook

TRINO_CONN_ID = "trino_default"
COINCAP_POSTGRES_CONN_ID = "coincap_postgres"
FOCUS_ASSET_ID = "bitcoin"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="coincap_trino_focus_to_postgres",
    description="Read CoinCap snapshots through Trino, filter a single asset, and load it into Postgres",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    default_args=default_args,
    tags=["coincap", "trino", "postgres"],
) as dag:

    @task
    def ensure_target_table() -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS coincap_focus_asset (
            asset_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL,
            price_usd NUMERIC,
            market_cap_usd NUMERIC,
            change_percent_24h NUMERIC,
            CONSTRAINT coincap_focus_asset_pk PRIMARY KEY (asset_id, fetched_at)
        );
        CREATE INDEX IF NOT EXISTS coincap_focus_asset_fetched_at_idx
            ON coincap_focus_asset (fetched_at DESC);
        """
        PostgresHook(postgres_conn_id=COINCAP_POSTGRES_CONN_ID).run(ddl)

    @task
    def extract_focus_asset() -> List[Dict[str, Any]]:
        sql = f"""
        SELECT
            asset_id,
            symbol,
            name,
            price_usd,
            market_cap_usd,
            change_percent_24h,
            fetched_at
        FROM postgresql.public.coincap_asset_snapshots
        WHERE asset_id = '{FOCUS_ASSET_ID}'
        ORDER BY fetched_at DESC
        LIMIT 1
        """
        th = TrinoHook(trino_conn_id=TRINO_CONN_ID)
        df = th.get_pandas_df(sql)
        if df.empty:
            return []

        df = df.where(pd.notnull(df), None)
        df["fetched_at"] = df["fetched_at"].apply(
            lambda value: value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
        )
        return df.to_dict("records")

    @task
    def load_focus_asset(rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        insert_sql = """
        INSERT INTO coincap_focus_asset (
            asset_id,
            symbol,
            name,
            fetched_at,
            price_usd,
            market_cap_usd,
            change_percent_24h
        ) VALUES (
            %(asset_id)s,
            %(symbol)s,
            %(name)s,
            %(fetched_at)s,
            %(price_usd)s,
            %(market_cap_usd)s,
            %(change_percent_24h)s
        )
        ON CONFLICT (asset_id, fetched_at) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            price_usd = EXCLUDED.price_usd,
            market_cap_usd = EXCLUDED.market_cap_usd,
            change_percent_24h = EXCLUDED.change_percent_24h;
        """

        hook = PostgresHook(postgres_conn_id=COINCAP_POSTGRES_CONN_ID)
        conn = hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, rows)
        return len(rows)

    ensure_target_table() >> load_focus_asset(extract_focus_asset())
