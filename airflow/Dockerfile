FROM apache/airflow:2.9.2-python3.11

USER airflow

# dbt + Trino client + data quality libs, pinned to compatible versions
RUN pip install --no-cache-dir \
      "dbt-core==1.8.3" \
      "dbt-trino==1.8.1" \
      "trino==0.328.0"
