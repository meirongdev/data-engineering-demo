# Architecture

This lab runs a complete Iceberg lakehouse inside a single [kind](https://kind.sigs.k8s.io/)
cluster (namespace `lakehouse`). Three core workloads make up the lakehouse
stack (object storage, catalog, compute), plus a Postgres **source** database, a
one-shot bucket bootstrap Job, and an on-demand load generator Job. For the
high-level diagram, see the [README](../README.md#architecture); for the ETL
that runs on top, see [pipeline.md](pipeline.md).

## The three layers

### 1. Object storage — SeaweedFS (`k8s/10-seaweedfs.yaml`)

- Single-pod, all-in-one `chrislusf/seaweedfs` server with the S3 gateway
  enabled on port **8333** (master UI on **9333**).
- One static S3 identity — `admin` / `password` — with full access, defined in
  the `seaweedfs-s3-config` ConfigMap (`s3.json`). Both the catalog and Spark
  authenticate with it.
- Data persists on a **2 Gi** PVC (`seaweedfs-data`) backed by kind's default
  `standard` StorageClass. The Deployment uses the `Recreate` strategy so the
  single writer never double-mounts the volume.
- Probes: readiness on the S3 port (8333), liveness on the master port (9333).

### 2. Table catalog — Iceberg REST (`k8s/30-iceberg-rest.yaml`)

- The locally built `iceberg-rest:local` image, extending
  `apache/iceberg-rest:1.10.1` with the Postgres JDBC driver for persistent
  table metadata via JdbcCatalog. Serves the Iceberg REST catalog protocol on
  port **8181**.
- Table **metadata is stored in Postgres** (`iceberg_catalog` database) — it
  survives pod restarts. The data and metadata *files* live durably in SeaweedFS.
- Configured through env vars: `CATALOG_CATALOG__IMPL=…JdbcCatalog`,
  `CATALOG_URI=jdbc:postgresql://postgres:5432/iceberg_catalog`,
  `CATALOG_WAREHOUSE=s3://warehouse/`, `CATALOG_IO__IMPL=…S3FileIO`,
  `CATALOG_S3_ENDPOINT=http://seaweedfs:8333`, and
  `CATALOG_S3_PATH__STYLE__ACCESS=true`.
- Two init containers block startup until both SeaweedFS (8333) and Postgres
  (5432) are reachable.

### 3. Compute / notebooks — Spark + Jupyter (`k8s/40-spark-iceberg.yaml`)

- The locally built `spark-iceberg:local` image: Spark 3.5.8 + Iceberg 1.10.1 +
  Jupyter Lab, plus PyIceberg. See [configuration.md](configuration.md) for the
  full version matrix.
- Jupyter Lab on port **8888**, Spark driver UI on **4040** (live only while a
  notebook holds an active Spark session).
- Notebooks persist on a **1 Gi** PVC (`notebooks`); seed notebooks are copied
  in from the image on first start (existing edits are never clobbered).
- An init container waits for **both** SeaweedFS (8333) and the REST catalog
  (8181) before Spark starts.

### 4. Serving layer (opt-in) — Trino + Metabase

Added by `make serving` on top of the base stack. Full detail in
[serving.md](serving.md).

**Trino** (`k8s/70-trino.yaml`) — interactive SQL engine. Talks to the same
Iceberg REST catalog and SeaweedFS data files as Spark. Namespaces appear as
`iceberg.<namespace>.<table>` (e.g. `iceberg.gold.item_performance`). Uses the
native S3 filesystem (`fs.s3.enabled=true`) for direct data access.

**Metabase** (`docker/metabase/Dockerfile` + `k8s/80-metabase.yaml`) — BI
dashboards on the gold-layer tables. Connects to Trino via the Starburst
community driver (baked into the `metabase:local` image). Embedded H2 metadata
on a 1 Gi PVC.

### Bootstrap — bucket-init (`k8s/20-bucket-init.yaml`)

A one-shot `minio/mc` Job that waits for SeaweedFS, then creates the `warehouse`
and `pageviews` buckets. `S3FileIO` never creates buckets, so the warehouse must
exist before Iceberg writes anything; `pageviews` holds the raw clickstream JSON
the loadgen drops. The Job is idempotent (`mc mb --ignore-existing`) and
self-cleans 600 s after completion.

## The source & pipeline

### Source database — Postgres (`k8s/50-postgres.yaml`)

- `postgres:16` holding the "oneshop" OLTP tables (`users`, `items`,
  `purchases`). Schema and the read-only `etluser` login are bootstrapped on
  first start from the `postgres-bootstrap` ConfigMap (mounted into
  `/docker-entrypoint-initdb.d`); the actual rows come from the loadgen.
- Data persists on a **1 Gi** PVC (`postgres-data`); the Deployment uses the
  `Recreate` strategy. Reachable in-cluster at `postgres:5432` and from the host
  at `localhost:5432`.
- Deployed as part of `make deploy` (it's a standing component, not a Job).

### Load generator — loadgen (`k8s/60-loadgen.yaml`)

- The locally built `loadgen:local` image (Python). A one-shot Job that
  TRUNCATEs and seeds the Postgres tables and writes a bounded batch of pageview
  events as newline-delimited JSON into the `pageviews` bucket, then exits.
- Idempotent and reproducible — re-running gives a clean dataset. Not part of
  `make deploy`; it's (re)applied on demand by `make pipeline` / `make loadgen`.
- An init container waits for both Postgres (5432) and SeaweedFS (8333).

### Pipeline — Spark medallion ETL (`docker/spark/pipeline/`)

Five `spark-submit` stages, baked into the Spark image at `/opt/pipeline/` and
run in order by `scripts/pipeline.sh`: create tables → Postgres-to-bronze →
pageviews-to-bronze → bronze-to-silver → silver-to-gold. The same logic is
walked through interactively in notebooks `01`–`04`. Full detail in
[pipeline.md](pipeline.md).

## How a query flows

1. In a notebook, `SparkSession.builder.getOrCreate()` picks up
   `spark-defaults.conf` and registers the `demo` catalog (type `rest`).
2. A `CREATE TABLE` / `INSERT` / `SELECT` against `demo.<ns>.<table>` makes Spark
   call the **REST catalog** at `http://iceberg-rest:8181` for the table's
   metadata pointer.
3. The catalog returns the table location under `s3://warehouse/`.
4. Spark reads/writes the actual **Parquet data files and Iceberg metadata**
   directly on **SeaweedFS** via `S3FileIO` — the catalog is never in the data
   path.

The end-to-end smoke test (`scripts/smoke-test.sh`) exercises exactly this path
and asserts the resulting data files land under `s3://warehouse/`.

## Networking

Host ports map onto fixed NodePorts on the control-plane node
(`cluster/kind-config.yaml`), so services are reachable at `localhost:<port>`
with no `kubectl port-forward`.

| Host port | NodePort | Service | Purpose |
|---|---|---|---|
| 8888 | 30888 | `spark-iceberg` | Jupyter Lab |
| 4040 | 30404 | `spark-iceberg` | Spark driver UI |
| 8181 | 30181 | `iceberg-rest` | Iceberg REST catalog |
| 8333 | 30333 | `seaweedfs` | SeaweedFS S3 API |
| 9333 | 30933 | `seaweedfs` | SeaweedFS master UI |
| 5432 | 30432 | `postgres` | Postgres `oneshop` source |
| 8080 | 30080 | `trino` | Trino SQL engine (opt-in) |
| 3000 | 30300 | `metabase` | Metabase BI dashboards (opt-in) |

Inside the cluster, workloads reach each other by Service DNS:
`seaweedfs:8333` (S3), `iceberg-rest:8181` (catalog), `postgres:5432`
(source), `trino:8080` (Trino), and `metabase:3000` (Metabase).

## Persistence model

| Data | Where | Survives pod restart? | Survives `make down`? |
|---|---|---|---|
| S3 objects (table data + metadata files) | `seaweedfs-data` PVC (2 Gi) | ✅ | ❌ |
| Your notebooks | `notebooks` PVC (1 Gi) | ✅ | ❌ |
| Postgres source rows | `postgres-data` PVC (1 Gi) | ✅ | ❌ |
| Catalog table registry | `iceberg_catalog` database in Postgres | ✅ | ❌ |
| Metabase metadata | `metabase-data` PVC (1 Gi) | ✅ | ❌ |

`make down` deletes the whole kind cluster, including all PVCs. The catalog
registry is now backed by Postgres, so restarting the `iceberg-rest` pod no
longer loses the list of tables — only a full `make down` destroys everything.
