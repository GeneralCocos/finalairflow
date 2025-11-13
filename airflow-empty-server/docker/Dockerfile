# docker/Dockerfile
FROM python:3.12-slim

ENV AIRFLOW_HOME=/opt/airflow \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1

# Базовые системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc build-essential libpq-dev curl tini bash ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Устанавливаем Airflow по constraints (совместимо с Python 3.12)
ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt
RUN pip install --no-cache-dir "apache-airflow[postgres,celery]==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}"

# Ставим зависимости проекта по тем же constraints
# (файл берём из build-context корня: docker/requirements.txt)
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINT_URL}"

# Готовим каталоги Airflow
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins /opt/airflow/scripts
WORKDIR ${AIRFLOW_HOME}

# Создаём пользователя airflow (UID должен совпадать с тем, что в compose) и отдаём права
ARG AIRFLOW_UID=50000
RUN useradd -ms /bin/bash -u ${AIRFLOW_UID} airflow \
 && chown -R ${AIRFLOW_UID}:0 /opt/airflow \
 && chmod -R g+w /opt/airflow

# Кладём entrypoint, чиним переносы строк (CRLF->LF) и права
COPY scripts/entrypoint.sh /opt/airflow/scripts/entrypoint.sh
RUN sed -i 's/\r$//' /opt/airflow/scripts/entrypoint.sh && chmod +x /opt/airflow/scripts/entrypoint.sh

# Запускаем под пользователем airflow; tini — init-процесс
USER ${AIRFLOW_UID}
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/airflow/scripts/entrypoint.sh"]
