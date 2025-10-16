# Dockerfile (para Airflow)

FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/data && chown -R airflow /opt/airflow/data

USER airflow
RUN pip install --no-cache-dir \
    pandas \
    requests \
    duckdb \
    dbt-duckdb