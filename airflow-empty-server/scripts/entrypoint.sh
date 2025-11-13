#!/usr/bin/env bash
set -euo pipefail

migrate_database() {
  airflow db migrate
}

create_admin_user() {
  local username="${_AIRFLOW_WWW_USER_USERNAME:-admin}"
  local firstname="${_AIRFLOW_WWW_USER_FIRSTNAME:-Airflow}"
  local lastname="${_AIRFLOW_WWW_USER_LASTNAME:-Admin}"
  local email="${_AIRFLOW_WWW_USER_EMAIL:-airflowadmin@example.com}"
  local role="${_AIRFLOW_WWW_USER_ROLE:-Admin}"
  local password="${_AIRFLOW_WWW_USER_PASSWORD:-admin}"

  airflow users create \
    --username "${username}" \
    --firstname "${firstname}" \
    --lastname "${lastname}" \
    --email "${email}" \
    --role "${role}" \
    --password "${password}" || true
}

ensure_connection() {
  local conn_id="$1"
  local env_var="$2"
  local conn_uri="${!env_var:-}"

  if [[ -n "${conn_uri}" ]]; then
    airflow connections add "${conn_id}" --conn-uri "${conn_uri}" --overwrite >/dev/null 2>&1 || true
  fi
}

bootstrap_connections() {
  ensure_connection "trino_default" "AIRFLOW_CONN_TRINO_DEFAULT"
  ensure_connection "spacex_postgres" "AIRFLOW_CONN_SPACEX_POSTGRES"
}

case "${1:-}" in
  init)
    migrate_database
    create_admin_user
    bootstrap_connections
    ;;
  webserver)
    migrate_database
    create_admin_user
    bootstrap_connections
    exec airflow webserver
    ;;
  scheduler)
    migrate_database
    bootstrap_connections
    exec airflow scheduler
    ;;
  *)
    # по умолчанию ведём себя как init
    migrate_database
    create_admin_user
    bootstrap_connections
    ;;
esac
