from __future__ import annotations
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Извлекаем из Postgres через Trino (каталог postgresql, схема public)
TRINO_CONN_ID = "trino_default"         # создай это подключение в Airflow
POSTGRES_CONN_ID = "spacex_postgres"    # у тебя уже есть в airflow.env

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
        dag_id="trino_to_postgres_launches_enriched",
        description="Read from Postgres via Trino, transform, and load back to Postgres",
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["trino", "transform", "postgres"],
        default_args=default_args,
) as dag:

    @task
    def ensure_target_table():
        sql = """
        CREATE TABLE IF NOT EXISTS launches_enriched (
            id TEXT PRIMARY KEY,
            date_utc TIMESTAMP,
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
        Читаем исходные таблицы через Trino (каталог postgresql, схема public).
        Если у тебя таблицы называются иначе — поправь имена.
        """
        th = TrinoHook(trino_conn_id=TRINO_CONN_ID)
        # Тянем join, чтобы сразу получить имя ракеты
        sql = """
        SELECT
          l.id,
          l.date_utc,
          l.success,
          l.rocket_id,
          r.name AS rocket_name,
          l.launchpad_id
        FROM postgresql.public.launches l
        LEFT JOIN postgresql.public.rockets r
               ON r.id = l.rocket_id
        """
        df = th.get_pandas_df(sql)
        # кладём во временный parquet (надёжнее для межтаскового обмена)
        tmp_path = "/tmp/launches_trino.parquet"
        df.to_parquet(tmp_path, index=False)
        return tmp_path

    @task
    def transform(tmp_path: str) -> str:
        df = pd.read_parquet(tmp_path)

        # Преобразования:
        # 1) Год запуска
        df["year"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce").dt.year
        # 2) Читабельная метка успеха
        df["success_label"] = df["success"].map({True: "SUCCESS", False: "FAIL"}).fillna("UNKNOWN")

        # Сохраняем только нужные поля в том же порядке, что в целевой таблице
        out = df[[
            "id", "date_utc", "year", "success",
            "success_label", "rocket_id", "rocket_name", "launchpad_id"
        ]].drop_duplicates(subset=["id"])

        out_path = "/tmp/launches_enriched.parquet"
        out.to_parquet(out_path, index=False)
        return out_path

    @task
    def load_to_postgres(out_path: str):
        df = pd.read_parquet(out_path)

        # upsert по id
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
        hook.insert_rows(
            table="launches_enriched",
            rows=df.to_dict("records"),
            target_fields=[
                "id", "date_utc", "year", "success", "success_label",
                "rocket_id", "rocket_name", "launchpad_id"
            ],
            commit_every=1000,
            replace=False,  # используем отдельный upsert ниже
        )
        # insert_rows не умеет ON CONFLICT; выполним батч-upsert вручную:
        conn = hook.get_conn()
        cur = conn.cursor()
        for rec in df.to_dict("records"):
            cur.execute(upsert_sql, rec)
        conn.commit()
        cur.close()
        conn.close()

    ensure_target_table() >> load_to_postgres(transform(extract_from_trino()))
