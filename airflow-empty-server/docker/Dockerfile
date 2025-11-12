ARG AIRFLOW_VERSION=2.9.1

FROM apache/airflow:${AIRFLOW_VERSION}

WORKDIR /opt/airflow

COPY requirements.txt ./requirements.txt
RUN if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

USER root
RUN pip install --no-cache-dir \
    apache-airflow-providers-trino \
    apache-airflow-providers-postgres \
    pandas \
    pyarrow
USER airflow