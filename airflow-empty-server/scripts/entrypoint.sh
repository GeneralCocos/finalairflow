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

case "${1:-}" in
  init)
    migrate_database
    create_admin_user
    ;;
  webserver)
    migrate_database
    create_admin_user
    exec airflow webserver
    ;;
  scheduler)
    migrate_database
    exec airflow scheduler
    ;;
  *)
    # по умолчанию ведём себя как init
    migrate_database
    create_admin_user
    ;;
esac
