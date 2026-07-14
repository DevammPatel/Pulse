FROM bitnami/spark:3.5.1

USER root

# Python deps for the streaming job + quality checks
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-fetch the Kafka / Delta / hadoop-aws jars into the local ivy cache so the
# spark-submit --packages step resolves instantly (and works offline after the
# first build). Versions must match the spark-submit command in docker-compose.
RUN /opt/bitnami/spark/bin/spark-shell \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      -e ":quit" || true

WORKDIR /opt/jobs
