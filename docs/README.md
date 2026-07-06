# Data Engineering Demo — Iceberg Lakehouse on Kind

An end-to-end data lakehouse running locally on `kind` with Apache Iceberg,
Spark, Trino, Airflow, and Metabase — all inside a single Kubernetes cluster.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Kind Cluster (data-eng)                          │
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │  Ingestion   │    │  Storage     │    │  Compute & Engine    │  │
│  │  ──────────  │    │  ──────────  │    │  ─────────────────  │  │
│  │  Loadgen     │───►│  SeaweedFS   │◄───│  Spark (PySpark)    │  │
│  │  (Postgres   │    │  (S3 API)    │    │  Jupyter Notebooks  │  │
│  │   + Events)  │    │              │    │  Pipeline Scripts   │  │
│  └──────────────┘    └──────┬───────┘    └─────────┬────────────┘  │
│                             │                      │               │
│  ┌──────────────┐          │                      │               │
│  │  PostgreSQL  │          │    ┌──────────────────┴──────────┐   │
│  │  ──────────  │          │    │  Iceberg REST Catalog      │   │
│  │  oneshop DB  │──────────┼───►│  (table metadata + schema) │   │
│  └──────────────┘          │    └─────────────────────────────┘   │
│                             │                                      │
│  ┌──────────────────────────────────────────────────────────┐     │
│  │  Serving Layer                                           │     │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────────────┐   │     │
│  │  │  Trino   │───►│  Iceberg │───►│  Metabase (BI)   │   │     │
│  │  │  (SQL)   │    │  Tables  │    │  :3000           │   │     │
│  │  └──────────┘    └──────────┘    └──────────────────┘   │     │
│  └──────────────────────────────────────────────────────────┘     │
│                                                                      │
│  ┌──────────── Orchestration (opt-in: make airflow) ─────────────┐  │
│  │  Airflow — medallion_pipeline DAG                              │  │
│  │  ┌────────────────────────────────────────────────────────┐  │  │
│  │  │ create → [postgres_to_bronze ∥ s3_to_bronze] → silver   │  │  │
│  │  │        → gold → gold_analytics                          │  │  │
│  │  │  (KubernetesPodOperator per stage, retries, web UI)     │  │  │
│  │  └────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Stack Components

| Layer | Component | Host Port | Description |
|---|---|---|---|
| **Object Store** | SeaweedFS | `:8333` S3, `:9333` UI | S3-compatible storage for Iceberg data files |
| **Source DB** | PostgreSQL | `:30432` | `oneshop` OLTP database (users, items, purchases) |
| **Catalog** | Iceberg REST | `:8181` | Table metadata and schema registry |
| **Compute** | Spark + Jupyter | `:8888` | PySpark ETL, interactive notebooks, pipeline scripts |
| **Query Engine** | Trino | `:8080` | SQL engine for Iceberg tables (opt-in) |
| **BI** | Metabase | `:3000` | Dashboards and ad-hoc queries (opt-in) |
| **Orchestration** | Airflow | `:8880` | Workflow orchestration — DAG, retries, monitoring (opt-in) |

## Medallion Pipeline

```
Bronze                    Silver                    Gold
──────                    ──────                    ────
users ───────────────────► users (validated) ──────► item_performance
items ───────────────────► items (enriched)         top_selling_items
purchases ───────────────► purchases_enriched       sales_performance_24h
pageviews (S3 JSON) ────► pageviews_by_items       pageviews_by_channel
                                                     user_engagement_segments
```

## Project Structure

```
├── cluster/                  # kind cluster config
├── docker/
│   ├── spark/                # Spark + Jupyter image + pipeline scripts
│   ├── iceberg-rest/         # Iceberg REST catalog with PG JDBC
│   ├── loadgen/              # Postgres/pageview data generator
│   ├── metabase/             # Metabase BI image
│   └── airflow/              # Airflow image + DAGs ← NEW (opt-in)
│       └── dags/             # medallion_pipeline.py (baked into image)
├── k8s/                      # Kubernetes manifests
│   ├── 00-namespace.yaml
│   ├── 10-seaweedfs.yaml
│   ├── 20-bucket-init.yaml
│   ├── 30-iceberg-rest.yaml
│   ├── 40-spark-iceberg.yaml
│   ├── 50-postgres.yaml
│   ├── 60-loadgen.yaml
│   ├── 70-trino.yaml         # serving (opt-in: make serving)
│   ├── 80-metabase.yaml      # serving (opt-in: make serving)
│   ├── 85-airflow.yaml       # orchestration (opt-in: make airflow) ← NEW
│   └── 90-pipeline-cron.yaml # the "before" — superseded by the Airflow DAG
├── notebooks/                # Jupyter seed notebooks
├── scripts/                  # Operational scripts
├── docs/                     # Architecture and design docs
│   ├── README.md             # This file
│   └── airflow-design.md     # Airflow integration design
└── Makefile
```

## Quick Start

```bash
make up          # Create cluster, build base images, deploy the base stack
make status      # Show pods and local URLs
make jupyter     # Open Jupyter Lab
make smoke       # Run end-to-end Iceberg smoke test
make pipeline    # Run the full medallion pipeline (loadgen -> bronze -> silver -> gold)

# Opt-in layers (on top of the base stack):
make serving     # Deploy Trino + Metabase (BI / SQL)
make airflow     # Deploy Airflow (orchestration) — then trigger the pipeline DAG
make airflow-ui  # Open the Airflow web UI (http://localhost:8880, admin/admin)

make down        # Tear everything down
```

> Airflow is **opt-in** and not part of `make up`. It needs the base stack's
> `spark-iceberg:local` image (`make build` / `make up`), but not the serving layer.

## Airflow (orchestration)

One DAG that turns the serial CronJob into a monitored, retry-able task graph.
See [`docs/airflow-design.md`](airflow-design.md) for the **when & how** write-up.

| DAG | Schedule | Purpose |
|---|---|---|
| `medallion_pipeline` | Daily 06:00 UTC | Bronze → Silver → Gold ETL — one ephemeral Spark pod per stage, parallel bronze ingest, per-task retries |

Data-quality and reporting DAGs are intentionally left as future extensions
(see the design doc) to keep the demo focused on *why* you introduce Airflow.
