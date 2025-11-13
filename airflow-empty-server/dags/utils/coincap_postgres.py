from __future__ import annotations

import os
from typing import Dict, Optional

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.session import create_session
from sqlalchemy.engine.url import make_url

COINCAP_POSTGRES_CONN_ID = "coincap_postgres"

_DEFAULT_SETTINGS: Dict[str, Optional[str]] = {
    "login": "coincap",
    "password": "coincap",
    "host": "coincap-postgres",
    "schema": "coincap",
    "port": "5432",
}


def _extract_settings_from_uri(uri: str) -> Dict[str, Optional[str]]:
    url = make_url(uri)
    return {
        "login": url.username or _DEFAULT_SETTINGS["login"],
        "password": url.password or _DEFAULT_SETTINGS["password"],
        "host": url.host or _DEFAULT_SETTINGS["host"],
        "schema": url.database or _DEFAULT_SETTINGS["schema"],
        "port": str(url.port or int(_DEFAULT_SETTINGS["port"])),
    }


def _collect_connection_settings() -> Dict[str, Optional[str]]:
    uri = os.environ.get("AIRFLOW_CONN_COINCAP_POSTGRES")
    if uri:
        return _extract_settings_from_uri(uri)

    settings_from_env = {
        "login": os.environ.get("COINCAP_POSTGRES_USER"),
        "password": os.environ.get("COINCAP_POSTGRES_PASSWORD"),
        "host": os.environ.get("COINCAP_POSTGRES_HOST"),
        "schema": os.environ.get("COINCAP_POSTGRES_DB"),
        "port": os.environ.get("COINCAP_POSTGRES_PORT"),
    }

    merged: Dict[str, Optional[str]] = {}
    for key, default_value in _DEFAULT_SETTINGS.items():
        merged[key] = settings_from_env.get(key) or default_value
    return merged


def ensure_coincap_postgres_connection() -> None:
    conn_settings = _collect_connection_settings()

    with create_session() as session:
        conn = (
            session.query(Connection)
            .filter(Connection.conn_id == COINCAP_POSTGRES_CONN_ID)
            .one_or_none()
        )
        if conn is None:
            conn = Connection(conn_id=COINCAP_POSTGRES_CONN_ID, conn_type="postgres")
        conn.conn_type = "postgres"
        conn.host = conn_settings["host"]
        conn.login = conn_settings["login"]
        conn.password = conn_settings["password"]
        conn.schema = conn_settings["schema"]
        port_value = conn_settings.get("port")
        conn.port = int(port_value) if port_value is not None else None
        session.add(conn)
        session.flush()

    try:
        Connection.get_connection_from_secrets(COINCAP_POSTGRES_CONN_ID)
    except AirflowNotFoundException as exc:  # pragma: no cover - runtime safeguard
        raise RuntimeError(
            "Failed to ensure CoinCap Postgres connection. "
            "Verify the Airflow metadata database is reachable and environment"
            " variables for CoinCap Postgres are set."
        ) from exc


def get_coincap_postgres_hook() -> PostgresHook:
    ensure_coincap_postgres_connection()
    return PostgresHook(postgres_conn_id=COINCAP_POSTGRES_CONN_ID)
