# data-engineering-demo

A hands-on learning lab for the **Apache Iceberg lakehouse** stack, running end
to end on a local [kind](https://kind.sigs.k8s.io/) Kubernetes cluster. It takes
a classic Docker Compose–based lakehouse tutorial stack and moves it off Docker
Compose onto Kubernetes, modernised.

| Layer | Component | Why |
|---|---|---|
| Source data | **Postgres 16** (OLTP) + **pageview JSON** on S3 | the "oneshop" e-commerce sources the pipeline ingests |
| Object storage | **SeaweedFS** (S3 API) | Apache 2.0, lightweight, S3-compatible |
| Table catalog | **Apache Iceberg REST catalog** | the reference `iceberg-rest-fixture` |
| Compute / notebooks | **Spark 3.5 + Iceberg 1.10 + Jupyter Lab** | write PySpark / SQL against Iceberg tables |
| Platform | **kind** (k8s in Docker) | learn Kubernetes and data infra together |

On top of the storage/catalog/compute stack, a **medallion (bronze → silver →
gold) data pipeline** ingests from Postgres (JDBC) and raw pageview JSON (S3),
validates and enriches it, and builds an item-performance analytics table. See
[docs/pipeline.md](docs/pipeline.md) and run it with `make pipeline`.

## Architecture

```
                          kind cluster (namespace: lakehouse)
   ┌────────────────────────────────────────────────────────────────────────┐
   │                                                                          │
   │   ┌────────────┐   seed    ┌──────────────────────┐  REST  ┌──────────┐ │
   │   │  loadgen   │──────────▶│  postgres (:5432)     │        │ iceberg- │ │
   │   │  (Job)     │           │  oneshop OLTP tables  │◀──JDBC─┤ rest     │ │
   │   └─────┬──────┘           └──────────────────────┘        │ (:8181)  │ │
   │         │ pageview JSON              ▲ spark-iceberg reads  └────┬─────┘ │
   │         ▼                            │                           │       │
   │   ┌──────────────────────┐   S3 read ┌──────────────────────┐   │ meta  │
   │   │  seaweedfs (:8333)    │◀─────────│  spark-iceberg        │───┘ +data │
   │   │  bucket: pageviews    │          │  Spark + Iceberg      │           │
   │   │  bucket: warehouse    │◀─────────│  Jupyter Lab (:8888)  │  S3 write │
   │   │  (Iceberg data files) │  S3 write │  Spark UI (:4040)     │           │
   │   └──────────────────────┘          └──────────────────────┘           │
   └────────────────────────────────────────────────────────────────────────┘
        host ports: 8888 (Jupyter) · 4040 (Spark UI) · 8181 (REST)
                     8333 (S3) · 9333 (SeaweedFS UI) · 5432 (Postgres)
```

Spark asks the REST catalog for table metadata; the catalog hands back the table
location, and Spark reads/writes the actual Parquet + Iceberg metadata files
directly on SeaweedFS via `S3FileIO`. The **pipeline** adds two ingestion paths:
Spark reads the `oneshop` tables from Postgres over JDBC and the raw pageview
JSON from the `pageviews` bucket over `s3a://`, landing both in Iceberg. See
[docs/architecture.md](docs/architecture.md) and
[docs/pipeline.md](docs/pipeline.md) for the full walkthrough.

## Quickstart

Requires **Docker (running)**, **kind**, **kubectl**, and ~4 GB free RAM — all
preflight-checked by `scripts/up.sh`.

```bash
make up        # kind cluster -> build & load images -> deploy everything
make smoke     # end-to-end test: create an Iceberg table, read it back
make pipeline  # run the medallion pipeline: loadgen -> bronze -> silver -> gold
make status    # pods/services + the URLs below
```

`make up` is idempotent and takes a few minutes the first time (it downloads
Spark and the Iceberg jars while building the image). When it finishes, open
**http://localhost:8888** and run
[`notebooks/00-getting-started.ipynb`](notebooks/00-getting-started.ipynb).

`make pipeline` then seeds the sources and runs the full bronze → silver → gold
flow non-interactively; the same steps are walked through interactively in
notebooks `01`–`04`. See [docs/pipeline.md](docs/pipeline.md).

| URL | What |
|---|---|
| http://localhost:8888 | Jupyter Lab |
| http://localhost:4040 | Spark driver UI (live only while a notebook Spark session runs) |
| http://localhost:8181/v1/config | Iceberg REST catalog |
| http://localhost:8333 | SeaweedFS S3 API (`admin` / `password`) |
| http://localhost:9333 | SeaweedFS master UI |
| localhost:5432 | Postgres `oneshop` source (`etluser` / `etlpassword`) |

Tear everything down (deletes the cluster and all its data):

```bash
make down
```

Run `make help` for the full command list.

## Repository layout

```
cluster/kind-config.yaml     kind cluster: 1 control-plane + 2 workers, host port maps
docker/spark/                Spark + Iceberg + Jupyter image (Dockerfile, configs, startup)
  pipeline/                  medallion pipeline scripts (00 create → 40 gold), run by make pipeline
docker/loadgen/              one-shot seeder image (Postgres + pageview JSON)
k8s/                         manifests, applied in order by deploy.sh
  00-namespace.yaml
  10-seaweedfs.yaml          storage: ConfigMap (S3 creds) + PVC + Deployment + NodePort Svc
  20-bucket-init.yaml        Job: create the `warehouse` + `pageviews` buckets
  50-postgres.yaml           Postgres source: bootstrap ConfigMap + PVC + Deployment + Svc
  30-iceberg-rest.yaml       Iceberg REST catalog
  40-spark-iceberg.yaml      Spark + Jupyter (uses the locally built image)
  60-loadgen.yaml            Job: seed Postgres + pageviews (run on demand by make pipeline)
notebooks/                   seeded into Jupyter on first start (00 intro, 01-04 pipeline)
scripts/                     up / down / build-image / deploy / status / smoke-test / pipeline (+ lib.sh)
Makefile                     thin wrapper over scripts/
```

## Documentation

Detailed docs live under [`docs/`](docs/):

- [Overview](docs/overview.md) — the project from two angles: the **business** scenario (what "oneshop" is and the questions the pipeline answers) and the **technical architecture** (components, ingestion paths, design trade-offs). Start here.
- [Getting started](docs/getting-started.md) — prerequisites, first run, the smoke test, and querying Iceberg from Spark, `%%sql`, and PyIceberg.
- [Medallion architecture](docs/medallion.md) — the bronze → silver → gold pattern itself: what each layer is for, the principles behind it, and how this lab maps onto it. Read before the pipeline.
- [Pipeline](docs/pipeline.md) — the medallion (bronze → silver → gold) ETL: sources, each stage, how to run it, and how to explore the results.
- [Architecture](docs/architecture.md) — each component, how metadata and data flow, in-cluster DNS, ports, and persistence.
- [Operations](docs/operations.md) — `make` targets, the scripts behind them, config env vars, and the rebuild/iterate loop.
- [Configuration](docs/configuration.md) — versions, Spark/PyIceberg settings, credentials, storage sizes, and version pinning.
- [Troubleshooting](docs/troubleshooting.md) — common failures and how to recover.
- [From lab to production](docs/production.md) — this is a lab; what you'd swap each component for (orchestration, catalog, ingestion, storage, secrets, …) to run it for real, and why.

## Security note

This lab uses a single static demo identity (`admin` / `password`) for SeaweedFS
S3, and Jupyter runs with **authentication disabled**. It is intended for local
development only — do not expose it to a network or reuse these credentials
anywhere else. See [docs/configuration.md](docs/configuration.md#credentials).

## Attribution

Modelled on [`tabulario/spark-iceberg`](https://github.com/tabular-io/docker-spark-iceberg)
(Apache 2.0) — trimmed down and rebuilt to run natively on arm64/amd64 and to
talk to the in-cluster catalog and storage. The `%%sql` notebook magic is
borrowed from that image.
