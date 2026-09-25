# Airflow image with the pipeline and dbt in their own virtualenvs (ADR 0001).
FROM apache/airflow:3.3.2-python3.12

USER root
# Java for Spark 4 (needs 17+).
# The JDK directory name depends on the CPU architecture, so link it to a fixed path.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

COPY pyproject.toml /opt/pipeline/src/
COPY pipeline /opt/pipeline/src/pipeline
COPY dbt /opt/pipeline/dbt
RUN python -m venv /opt/pipeline/venv \
 && /opt/pipeline/venv/bin/pip install --no-cache-dir "/opt/pipeline/src[spark]" "pyspark>=4,<5" \
 && chown -R airflow: /opt/pipeline
USER airflow

ENV PIPELINE_BIN=/opt/pipeline/venv/bin/pipeline \
    DBT_BIN=/opt/pipeline/venv/bin/dbt \
    DBT_PROJECT_DIR=/opt/pipeline/dbt \
    LAKE_ROOT=/data/lake \
    WAREHOUSE_PATH=/data/warehouse/telemetry.duckdb \
    REPORTS_ROOT=/data/reports
