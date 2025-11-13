from __future__ import annotations
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

TRINO_CONN_ID = "trino_default"
POSTGRES_CONN_ID = "spacex_postgres"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
        dag_id="trino_to_postgres_launches_enriched",
        description="Read SpaceX snapshots via Trino, transform, and load to Postgres",
        start_date=datetime(2025, 1, 1),
        schedule="*/2 * * * *",  # каждые 2 минуты
        catchup=False,
        tags=["trino", "transform", "postgres"],
        default_args=default_args,
) as dag:

    @task
    def ensure_target_table():
        sql = """
        CREATE TABLE IF NOT EXISTS launches_enriched (
            id TEXT PRIMARY KEY,
            date_utc TIMESTAMPTZ,
            year INT,
            success BOOLEAN,
            success_label TEXT,
            rocket_id TEXT,
            rocket_name TEXT,
            launchpad_id TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_launches_enriched_year ON launches_enriched(year);
        """
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql)

    @task
    def extract_from_trino() -> str:
        """
        Берём данные из spacex_launch_snapshots через Trino.
        Пока используем только id и дату — без успеха и ракет.
        """
        th = TrinoHook(trino_conn_id=TRINO_CONN_ID)
        sql = """
        SELECT
          launch_id AS id,
          date_utc
        FROM postgresql.public.spacex_launch_snapshots
        """
        df = th.get_pandas_df(sql)

        tmp_path = "/tmp/launches_trino.csv"
        df.to_csv(tmp_path, index=False)
        return tmp_path

    @task
    def transform(tmp_path: str) -> str:
        df = pd.read_csv(tmp_path)

        # дата в datetime (на всякий)
        df["date_utc"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")

        # год
        df["year"] = df["date_utc"].dt.year

        # success — строго NULL (None), чтобы не было NaN/float
        df["success"] = None

        # метка успеха (у нас пока нет инфы — ставим UNKNOWN)
        df["success_label"] = "UNKNOWN"

        # ракету/площадку пока не заполняем
        df["rocket_id"] = None
        df["rocket_name"] = None
        df["launchpad_id"] = None

        # оставляем только нужные колонки и убираем дубли по id
        out = df[
            [
                "id",
                "date_utc",
                "year",
                "success",
                "success_label",
                "rocket_id",
                "rocket_name",
                "launchpad_id",
            ]
        ].drop_duplicates(subset=["id"])

        out_path = "/tmp/launches_enriched.csv"
        out.to_csv(out_path, index=False)
        return out_path

    @task
    def load_to_postgres(out_path: str):
        df = pd.read_csv(out_path)

        # ещё раз на всякий: success -> None, чтобы не было float/NaN
        df["success"] = None

        upsert_sql = """
        INSERT INTO launches_enriched
        (id, date_utc, year, success, success_label, rocket_id, rocket_name, launchpad_id)
        VALUES (%(id)s, %(date_utc)s, %(year)s, %(success)s, %(success_label)s, %(rocket_id)s, %(rocket_name)s, %(launchpad_id)s)
        ON CONFLICT (id) DO UPDATE SET
          date_utc = EXCLUDED.date_utc,
          year = EXCLUDED.year,
          success = EXCLUDED.success,
          success_label = EXCLUDED.success_label,
          rocket_id = EXCLUDED.rocket_id,
          rocket_name = EXCLUDED.rocket_name,
          launchpad_id = EXCLUDED.launchpad_id
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        records = df.to_dict("records")
        for rec in records:
            # date_utc из CSV уже строка ISO, Postgres её нормально съест
            # success у нас уже None
            cur.execute(upsert_sql, rec)

        conn.commit()
        cur.close()
        conn.close()

    ensure_target_table() >> load_to_postgres(transform(extract_from_trino()))
