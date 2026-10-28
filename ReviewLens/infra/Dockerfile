FROM eclipse-temurin:11-jre-jammy AS java
FROM apache/hadoop:3.3.6 AS hadoop
FROM apache/spark:3.5.7-scala2.12-java11-python3-ubuntu AS spark
FROM python:3.11-slim-bookworm AS runtime
COPY --from=java /opt/java/openjdk /opt/java/openjdk
RUN apt-get update && apt-get install -y --no-install-recommends procps libgomp1 tini libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=hadoop /opt/hadoop /opt/hadoop
COPY --from=spark /opt/spark /opt/spark
COPY requirements-runtime.txt /tmp/requirements-runtime.txt
RUN pip install --no-cache-dir -r /tmp/requirements-runtime.txt
ENV JAVA_HOME=/opt/java/openjdk \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    PYTHONPATH=/workspace/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PATH=/opt/hadoop/bin:/opt/spark/bin:/opt/java/openjdk/bin:$PATH
COPY infra/hadoop/ /opt/hadoop/etc/hadoop/
COPY infra/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /exports /workspace /hadoop/name /hadoop/data
WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
CMD ["sleep", "infinity"]

