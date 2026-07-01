# Architecture

This lab runs a complete Iceberg lakehouse inside a single [kind](https://kind.sigs.k8s.io/)
cluster (namespace `lakehouse`). Three workloads make up the stack, plus a
one-shot bucket bootstrap Job. For the high-level diagram, see the
[README](../README.md#architecture).

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

- The reference `apache/iceberg-rest-fixture:1.10.1` image, serving the Iceberg
  REST catalog protocol on port **8181**.
- Table **metadata is stored in-memory** — it does *not* survive a pod restart.
  The data and metadata *files* it points at, however, live durably in
  SeaweedFS. (This is the fixture's design; it is for demos, not production.)
- Configured entirely through env vars to write into SeaweedFS:
  `CATALOG_WAREHOUSE=s3://warehouse/`, `CATALOG_IO__IMPL=…S3FileIO`,
  `CATALOG_S3_ENDPOINT=http://seaweedfs:8333`, and
  `CATALOG_S3_PATH__STYLE__ACCESS=true`.
- An init container blocks startup until SeaweedFS is reachable on 8333.

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

### Bootstrap — bucket-init (`k8s/20-bucket-init.yaml`)

A one-shot `minio/mc` Job that waits for SeaweedFS, then creates the `warehouse`
bucket. `S3FileIO` never creates buckets, so the warehouse must exist before
Iceberg writes anything. The Job is idempotent (`mc mb --ignore-existing`) and
self-cleans 600 s after completion.

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

Inside the cluster, workloads reach each other by Service DNS:
`seaweedfs:8333` (S3) and `iceberg-rest:8181` (catalog).

## Persistence model

| Data | Where | Survives pod restart? | Survives `make down`? |
|---|---|---|---|
| S3 objects (table data + metadata files) | `seaweedfs-data` PVC (2 Gi) | ✅ | ❌ |
| Your notebooks | `notebooks` PVC (1 Gi) | ✅ | ❌ |
| Catalog table registry | in-memory in `iceberg-rest` | ❌ | ❌ |

`make down` deletes the whole kind cluster, including both PVCs. Because the
catalog registry is in-memory, restarting the `iceberg-rest` pod loses the list
of tables even though the underlying files remain in SeaweedFS.
