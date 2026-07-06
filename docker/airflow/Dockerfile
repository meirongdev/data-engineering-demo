# syntax=docker/dockerfile:1
#
# Airflow image for the lakehouse demo — orchestrates the medallion pipeline.
#
# Airflow itself stays tiny: LocalExecutor, no Spark inside the image. The only
# extra it needs is the Kubernetes provider, so KubernetesPodOperator can launch
# ephemeral spark-iceberg pods (Airflow orchestrates, Spark computes). The DAGs
# are baked in so the scheduler sees them the moment the pod starts.
#
# The Airflow version is pinned; the provider is resolved through Airflow's
# official constraints file for this exact Airflow + Python combo, so the build
# is reproducible without hand-pinning every transitive dependency.
FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow-providers-cncf-kubernetes" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# DAGs baked into the image (default DAGS_FOLDER). Rebuild + reload after edits:
# `make airflow`.
COPY --chown=airflow:root docker/airflow/dags/ /opt/airflow/dags/
