"""medallion_pipeline — the medallion ETL as an Airflow DAG.

This DAG is the "after" to the CronJob's "before". The CronJob
(k8s/90-pipeline-cron.yaml) runs the five medallion stages as a serial shell
loop inside a single pod:

    for s in 00_create 10_pg 20_s3 30_silver 40_gold 50_analytics; do
        spark-submit /opt/pipeline/$s.py
    done

That works until it doesn't: one stage fails and the whole run dies, there is no
retry on a transient JDBC/S3 blip, the two independent bronze ingests run one
after the other for no reason, and the only record of what happened is whatever
`kubectl logs` still has.

Airflow turns those same five spark-submit calls into a task graph and adds:

  * dependency graph  — the two bronze ingests run IN PARALLEL; silver waits for
                        both; gold waits for silver.
  * per-task retries  — a transient error retries that one task instead of
                        killing the run.
  * visibility        — the web UI shows per-task logs, duration, run history,
                        and lets you re-run a single failed task.

The Spark logic is untouched: each task launches an ephemeral pod from the
existing spark-iceberg:local image and runs the same `/opt/pipeline/*.py` the
CronJob ran. Airflow orchestrates; Spark still does the compute.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Image + pipeline scripts already exist in the cluster (baked in by `make build`
# / `make up`). We only point Airflow at them — no Spark inside Airflow.
SPARK_IMAGE = "spark-iceberg:local"
NAMESPACE = "lakehouse"

# The pipeline scripts write to Iceberg via S3FileIO (AWS SDK v2), which needs a
# region even against SeaweedFS. The interactive spark-iceberg Deployment injects
# these as env (see k8s/40-spark-iceberg.yaml); a fresh pod from the image does
# not, so Airflow supplies the same runtime env here. (S3 keys are also in the
# baked spark-defaults.conf, but we mirror the Deployment for parity.)
SPARK_ENV = {
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "admin",
    "AWS_SECRET_ACCESS_KEY": "password",
}

# Each stage is a small Spark job. Cap memory so the two parallel bronze pods can
# coexist with the base stack on a kind cluster.
SPARK_RESOURCES = k8s.V1ResourceRequirements(
    requests={"memory": "1Gi", "cpu": "250m"},
    limits={"memory": "1536Mi"},
)

default_args = {
    # The whole point vs. the CronJob: a transient failure retries instead of
    # taking down the run.
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def spark_stage(task_id: str, script: str) -> KubernetesPodOperator:
    """One medallion stage = one ephemeral Spark pod running spark-submit."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"spark-{task_id}",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        image_pull_policy="IfNotPresent",  # kind-loaded, not from a registry
        cmds=["spark-submit"],
        arguments=[f"/opt/pipeline/{script}"],
        env_vars=SPARK_ENV,
        container_resources=SPARK_RESOURCES,
        in_cluster=True,  # use the mounted service account (see RBAC in 85-airflow.yaml)
        get_logs=True,  # stream the pod's logs into the Airflow task log
        on_finish_action="delete_succeeded_pod",  # tidy up, but keep failed pods to debug
    )


with DAG(
    dag_id="medallion_pipeline",
    description="Bronze -> Silver -> Gold ETL, one ephemeral Spark pod per stage",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # daily 06:00 UTC — the same slot the CronJob used
    catchup=False,
    default_args=default_args,
    tags=["medallion", "spark", "demo"],
) as dag:
    create_tables = spark_stage("create_tables", "00_create_tables.py")
    postgres_to_bronze = spark_stage("postgres_to_bronze", "10_postgres_to_bronze.py")
    s3_to_bronze = spark_stage("s3_to_bronze", "20_s3_to_bronze.py")
    bronze_to_silver = spark_stage("bronze_to_silver", "30_bronze_to_silver.py")
    silver_to_gold = spark_stage("silver_to_gold", "40_silver_to_gold.py")
    gold_analytics = spark_stage("gold_analytics", "50_gold_analytics.py")

    # create -> (parallel bronze ingest) -> silver -> gold -> extended gold.
    # The list makes both bronze tasks depend on create_tables AND both become
    # upstream of bronze_to_silver — that fan-out/fan-in is what the shell loop
    # could never express.
    create_tables >> [postgres_to_bronze, s3_to_bronze] >> bronze_to_silver
    bronze_to_silver >> silver_to_gold >> gold_analytics
