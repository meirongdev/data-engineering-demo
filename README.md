# data-engineering-demo

A hands-on learning lab for the **Apache Iceberg lakehouse** stack, running end
to end on a local [kind](https://kind.sigs.k8s.io/) Kubernetes cluster. Inspired
by chapter 02 of *Practical Data Engineering with Apache Projects*, but moved off
Docker Compose onto Kubernetes and modernised.

| Layer | Component | Why |
|---|---|---|
| Object storage | **SeaweedFS** (S3 API) | Apache 2.0, lightweight, S3-compatible |
| Table catalog | **Apache Iceberg REST catalog** | the reference `iceberg-rest-fixture` |
| Compute / notebooks | **Spark 3.5 + Iceberg 1.10 + Jupyter Lab** | write PySpark / SQL against Iceberg tables |
| Platform | **kind** (k8s in Docker) | learn Kubernetes and data infra together |

## Architecture

```
                          kind cluster (namespace: lakehouse)
   ┌───────────────────────────────────────────────────────────────────┐
   │                                                                     │
   │   ┌──────────────────────┐        ┌──────────────────────┐         │
   │   │  spark-iceberg        │  REST  │  iceberg-rest         │         │
   │   │  Spark + Iceberg      │───────▶│  catalog (:8181)      │         │
   │   │  Jupyter Lab (:8888)  │        └──────────┬───────────┘         │
   │   │  Spark UI (:4040)     │                   │ table metadata      │
   │   └──────────┬───────────┘                    │ + data files (S3)   │
   │              │  S3 (data files)                ▼                     │
   │              │                     ┌──────────────────────┐         │
   │              └────────────────────▶│  seaweedfs            │         │
   │                                    │  S3 storage (:8333)   │         │
   │                                    │  bucket: warehouse    │         │
   │                                    └──────────────────────┘         │
   └───────────────────────────────────────────────────────────────────┘
        host ports: 8888 (Jupyter) · 4040 (Spark UI) · 8181 (REST)
                     8333 (S3) · 9333 (SeaweedFS UI)
```

Spark asks the REST catalog for table metadata; the catalog hands back the table
location, and Spark reads/writes the actual Parquet + Iceberg metadata files
directly on SeaweedFS via `S3FileIO`. See
[docs/architecture.md](docs/architecture.md) for the full walkthrough.

## Quickstart

Requires **Docker (running)**, **kind**, **kubectl**, and ~4 GB free RAM — all
preflight-checked by `scripts/up.sh`.

```bash
make up        # kind cluster -> build & load image -> deploy everything
make smoke     # end-to-end test: create an Iceberg table, read it back
make status    # pods/services + the URLs below
```

`make up` is idempotent and takes a few minutes the first time (it downloads
Spark and the Iceberg jars while building the image). When it finishes, open
**http://localhost:8888** and run
[`notebooks/00-getting-started.ipynb`](notebooks/00-getting-started.ipynb).

| URL | What |
|---|---|
| http://localhost:8888 | Jupyter Lab |
| http://localhost:4040 | Spark driver UI (live only while a notebook Spark session runs) |
| http://localhost:8181/v1/config | Iceberg REST catalog |
| http://localhost:8333 | SeaweedFS S3 API (`admin` / `password`) |
| http://localhost:9333 | SeaweedFS master UI |

Tear everything down (deletes the cluster and all its data):

```bash
make down
```

Run `make help` for the full command list.

## Repository layout

```
cluster/kind-config.yaml     kind cluster: 1 control-plane + 2 workers, host port maps
docker/spark/                Spark + Iceberg + Jupyter image (Dockerfile, configs, startup)
k8s/                         manifests, applied in order by deploy.sh
  00-namespace.yaml
  10-seaweedfs.yaml          storage: ConfigMap (S3 creds) + PVC + Deployment + NodePort Svc
  20-bucket-init.yaml        Job: create the `warehouse` bucket
  30-iceberg-rest.yaml       Iceberg REST catalog
  40-spark-iceberg.yaml      Spark + Jupyter (uses the locally built image)
notebooks/                   seeded into Jupyter on first start
scripts/                     up / down / build-image / deploy / status / smoke-test (+ lib.sh)
Makefile                     thin wrapper over scripts/
```

## Documentation

Detailed docs live under [`docs/`](docs/):

- [Getting started](docs/getting-started.md) — prerequisites, first run, the smoke test, and querying Iceberg from Spark, `%%sql`, and PyIceberg.
- [Architecture](docs/architecture.md) — each component, how metadata and data flow, in-cluster DNS, ports, and persistence.
- [Operations](docs/operations.md) — `make` targets, the scripts behind them, config env vars, and the rebuild/iterate loop.
- [Configuration](docs/configuration.md) — versions, Spark/PyIceberg settings, credentials, storage sizes, and version pinning.
- [Troubleshooting](docs/troubleshooting.md) — common failures and how to recover.

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
