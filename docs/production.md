# From lab to production

This repo is a **local learning lab**. To stay simple and self-contained on
kind, it deliberately collapses concerns a production platform keeps separate,
and makes choices that are fine on a laptop but wrong for production:

- the Iceberg **catalog keeps its table registry in memory**
  (`apache/iceberg-rest-fixture`, `k8s/30-iceberg-rest.yaml`) — a pod restart
  loses the list of tables;
- **object storage is a single SeaweedFS replica** (`k8s/10-seaweedfs.yaml`)
  with a static `admin` / `password` identity;
- the **pipeline is a shell script** (`scripts/pipeline.sh`) running
  `spark-submit` stages in sequence — no scheduling, retries, backfills, or
  alerting;
- **buckets are created by an in-cluster `minio/mc` Job**
  (`k8s/20-bucket-init.yaml`) and **credentials are hardcoded** in manifests;
- **ingestion is synthetic and full-overwrite batch** (the `loadgen` Job plus
  `10_postgres_to_bronze.py` / `20_s3_to_bronze.py`).

None of that should ship. This page maps each lab component to the kind of
production technology you would replace it with, and why. It is a **decision
reference**, not a step-by-step migration guide, and the specific products are
examples — pick per your cloud, scale, and team.

> **Do not point production traffic at this lab as-is.**

## Three layers of automation

The lab merges three things production keeps separate. Getting them straight is
half the answer:

1. **Infrastructure provisioning** — cloud/cluster resources, buckets, IAM →
   Infrastructure as Code.
2. **Deployment** — how app + config reach the cluster → GitOps.
3. **Data-pipeline orchestration** — when/how the ETL runs, in what order, with
   retries → a real orchestrator.

The third is the biggest gap between this lab and production.

## The core gap: pipeline orchestration

`scripts/pipeline.sh` runs five `spark-submit` stages back to back — no
scheduling, no retry, no backfill, no dependency graph. Production wants an
orchestrator:

| Orchestrator | Best when | Notes |
|---|---|---|
| **Dagster** | Medallion / data-asset modelling | Asset-centric, maps cleanly onto bronze/silver/gold, built-in lineage + typing |
| **Apache Airflow** | General batch DAGs | Largest ecosystem, easiest to hire for; the safe default |
| **Argo Workflows** | Already all-in on Kubernetes | K8s-native, container-step DAGs |

**How Spark runs** — replace `kubectl exec … spark-submit` with either:

- the **Spark Operator** (`SparkApplication` CRD) — the natural fit for this
  repo's kind → real-Kubernetes path, or
- a **managed Spark** service (EMR / Dataproc / Databricks / Spark Connect).

The orchestrator *triggers and sequences*; the operator or managed service
*runs* the job with isolation, autoscaling, and retries.

**Transformations** — the SQL-shaped parts of silver/gold are a good fit for
**dbt** (tests, docs, lineage out of the box). Keep PySpark for logic that is
genuinely DataFrame-shaped.

## Component-by-component replacements

| Concern | This lab | Production replacement | Why it has to change |
|---|---|---|---|
| Buckets / IAM | `minio/mc` Job (`20-bucket-init.yaml`) | **Terraform / OpenTofu** | Infra shouldn't be created by an in-cluster job; declarative + auditable |
| Object storage | SeaweedFS, 1 replica | Cloud **S3 / GCS / ADLS**, or self-managed **Ceph / MinIO (HA)** | Durability, replication, lifecycle policies |
| Iceberg catalog | REST fixture, **in-memory** | **Nessie** (git-like branching) / **Polaris** / **Glue** / **Unity**, or JDBC catalog on Postgres | Registry must survive restarts; needs RBAC + HA |
| DB ingestion | Full-overwrite JDBC (`10_*`) | **CDC** (Debezium + Kafka) or Airbyte/Fivetran; bronze via **MERGE/upsert** | Incremental, no full reloads, no load on the source |
| Event ingestion | loadgen JSON + batch read (`20_*`) | **Kafka → Spark Structured Streaming / Flink** | Real-time, delivery guarantees, schema handling |
| Transformation | Hand-written PySpark scripts | **dbt** (SQL) + packaged, tested PySpark | Tests, lineage, maintainability |
| Orchestration | `scripts/pipeline.sh` | **Dagster / Airflow** (see above) | Scheduling, retries, backfills, SLAs, alerting |
| Compute | `kubectl exec spark-submit` | **Spark Operator** / managed Spark | Isolation, autoscaling, retry, history |
| Deployment | `make deploy` (imperative apply) | **Argo CD / Flux** + Helm/Kustomize | Declarative, git-audited, rollbacks |
| Secrets | Hardcoded `admin` / `password` | **Vault / External Secrets Operator / cloud KMS / IRSA** | No secrets in git or manifests |
| Data quality | Single `valid_email` flag | **dbt tests / Great Expectations / Soda** as gates | Block bad data before it propagates |
| Lineage / observability | Spark UI only | **OpenLineage + Marquez/DataHub**, Prometheus/Grafana | Debuggability, audits, alerting |
| Table maintenance | None | Scheduled **compaction, `expire_snapshots`, `remove_orphan_files`** | Small files + snapshot growth degrade Iceberg over time |
| CI/CD | None | Test → build image → staging → promote | Safe, repeatable releases |
| Notebooks | `01`–`04`, in-cluster | Dev/exploration only | Never in the production data path |
| Data | Synthetic (`loadgen`) | Real upstream sources | — |

## A default production stack

For a typical cloud team, a sane starting point:

> **Terraform** (infra + buckets + IAM) → **Argo CD** (GitOps deploy) →
> **Dagster** *or* **Airflow** (orchestration + scheduling) → **Spark Operator
> on K8s** *or* managed Spark (compute) → **dbt** (silver/gold SQL + tests) →
> **Lakekeeper / Polaris** (catalog, persisted) → **Trino** (interactive SQL /
> BI serving) → **Debezium + Kafka** (CDC ingestion) → **OpenLineage +
> Prometheus/Grafana** (lineage + monitoring) → a scheduled **Iceberg
> maintenance** job.

**Lakekeeper** is a lightweight, Rust-based Iceberg REST catalog that fits this
repo's *lightweight, K8s-native* ethos far better than the JVM-heavy Polaris or
Nessie. Single binary, Postgres-backed, no JVM overhead. See
[lakekeeper.dev](https://lakekeeper.dev) — it is the natural catalog-upgrade
path for this lab.

On a managed cloud (EMR / Dataproc / Databricks + Glue/Unity) much of the
compute and catalog operations are handled for you; self-hosting means wiring
Nessie + Spark Operator + storage yourself.

## What changes the recommendation

- **Cloud vs self-hosted** — managed services (Glue/EMR/Dataproc/Databricks)
  remove most catalog + compute ops; self-hosting trades cost for effort.
- **Batch vs real-time** — pure batch → Airflow/Dagster + Spark batch; low
  latency → Kafka + Flink / Structured Streaming.
- **Scale & team maturity** — don't adopt the whole stack at once (see below).
- **SQL-first vs Python-first** — SQL-heavy team → dbt + Trino/Spark;
  complex logic → Dagster + PySpark.

## If you only change three things

In priority order, the highest-leverage upgrades:

1. **A real orchestrator** (Dagster/Airflow) to replace `scripts/pipeline.sh`.
2. **A persistent, HA catalog** (Lakekeeper / Polaris / Glue) to replace the
   in-memory REST fixture.
3. **Managed secrets** to replace the hardcoded `admin` / `password`.

The serving layer (Trino + Metabase) is already the right shape for production
— Trino is the standard interactive/BI engine for Iceberg, and Metabase's
embedded H2 should be replaced with Postgres for HA. See [serving.md](serving.md).

Everything else can follow incrementally.

## See also

- [overview.md](overview.md) — what the lab is, from business + architecture angles
- [architecture.md](architecture.md) — the lab's component-level architecture
- [pipeline.md](pipeline.md) — the medallion pipeline this would productionise
