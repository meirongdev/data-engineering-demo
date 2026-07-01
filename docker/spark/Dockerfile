# syntax=docker/dockerfile:1
#
# Spark + Iceberg + Jupyter Lab image for the lakehouse demo.
#
# Modelled on tabulario/spark-iceberg, but:
#   * built natively for the host arch (works on Apple Silicon / arm64),
#   * trimmed down — no multi-GB NYC taxi datasets baked in,
#   * wired to talk to the in-cluster Iceberg REST catalog + SeaweedFS S3.
#
# Versions are the battle-tested combo used by the upstream image.
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.8
ARG SPARK_MAJOR_VERSION=3.5
ARG ICEBERG_VERSION=1.10.1
ARG SCALA_VERSION=2.12
# JDBC driver for the Postgres source. hadoop-aws + the AWS SDK v1 bundle let
# Spark read raw JSON over s3a:// (Iceberg's own S3FileIO is separate, SDK v2).
# hadoop-aws MUST match the Hadoop version bundled in spark-*-bin-hadoop3 (3.3.4).
ARG POSTGRES_JDBC_VERSION=42.7.6
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

ENV SPARK_HOME=/opt/spark
ENV SPARK_VERSION=${SPARK_VERSION}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
      procps \
      tini \
      openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Resolve JAVA_HOME independently of CPU arch (java -> .../bin/java).
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home
ENV JAVA_HOME=/opt/java-home

# Python deps. Split into separate layers so a flaky PyPI download only forces
# a re-pull of the failing group on rebuild (each successful layer is cached).
ENV PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10 PIP_NO_CACHE_DIR=1
RUN pip3 install jupyterlab==4.6.1
RUN pip3 install pandas==2.3.3 prettytable==3.18.0
RUN pip3 install matplotlib==3.11.0
RUN pip3 install "pyiceberg[pyarrow,duckdb,pandas]==0.11.1"

# Download and install Spark (dlcdn first, fall back to the Apache archive).
RUN mkdir -p ${SPARK_HOME} && \
    ( curl -fSL "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz || \
      curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz ) && \
    tar xzf /tmp/spark.tgz --directory ${SPARK_HOME} --strip-components 1 && \
    rm /tmp/spark.tgz

# Iceberg Spark runtime + AWS bundle (S3FileIO -> SeaweedFS).
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
      -o ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      -o ${SPARK_HOME}/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Source connectors for the medallion pipeline:
#   * Postgres JDBC driver         -> read the "oneshop" OLTP tables
#   * hadoop-aws + aws-java-sdk    -> read raw pageview JSON over s3a://
RUN curl -fSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      -o ${SPARK_HOME}/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}

# Spark + PyIceberg config.
COPY docker/spark/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY docker/spark/pyiceberg.yaml /root/.pyiceberg.yaml

# IPython startup: registers the %%sql magic + pretty DataFrame rendering.
RUN mkdir -p /root/.ipython/profile_default/startup
COPY docker/spark/ipython/startup/ /root/.ipython/profile_default/startup/

# Seed notebooks. An init step copies these into the notebooks PVC on first run.
RUN mkdir -p /opt/seed-notebooks /home/iceberg/notebooks /home/iceberg/spark-events
COPY notebooks/ /opt/seed-notebooks/

# Medallion pipeline scripts, run non-interactively via spark-submit
# (`make pipeline`). Same logic the guided notebooks walk through.
COPY docker/spark/pipeline/ /opt/pipeline/

COPY docker/spark/entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

WORKDIR /home/iceberg/notebooks
EXPOSE 8888 4040
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
