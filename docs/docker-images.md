# Docker images

This lab builds and runs inside four custom container images (plus several
official upstream images). This page explains what each one does, how they
relate, and the design decisions behind them.

## Quick reference

| Image | Dockerfile | Build target | Used by | Purpose |
|---|---|---|---|---|
| `spark-iceberg:local` | `docker/spark/Dockerfile` | `make build` | `40-spark-iceberg.yaml` | Compute + notebooks |
| `loadgen:local` | `docker/loadgen/Dockerfile` | `make build` | `60-loadgen.yaml` | Synthetic data seeding |
| `iceberg-rest:local` | `docker/iceberg-rest/Dockerfile` | `make build` | `30-iceberg-rest.yaml` | Iceberg REST catalog (persistent) |
| `metabase:local` | `docker/metabase/Dockerfile` | `make serving` | `80-metabase.yaml` | BI dashboards (opt-in) |

## Why custom images at all?

All the components in this stack have perfectly good upstream images on Docker
Hub. Four of them are custom because each solves a problem the upstream image
can't:

1. **Jars must be injected.** The official `apache/iceberg-rest` image doesn't
   ship the Postgres JDBC driver. Metabase doesn't ship the Starburst Trino
   driver. Spark's official Python image doesn't bundle Iceberg runtime jars.
   Each custom Dockerfile adds exactly the runtime dependency that's missing.

2. **Config must be baked in.** `spark-defaults.conf`, `pyiceberg.yaml`, IPython
   startup scripts, seed notebooks, and the pipeline ETL scripts are all part
   of the spark-iceberg image. Mounting them separately via ConfigMaps would
   work, but baking them in means the image is self-contained — no external
   mount dependencies at deploy time.

3. **Multi-arch matters.** Many upstream JVM images are amd64-only and run
   under Rosetta emulation on Apple Silicon. Building locally with `docker
   buildx` produces native arm64 binaries, which is faster and uses less memory.

## `spark-iceberg:local` — the heart of the stack

This is by far the most complex image. It extends `python:3.11-slim-bookworm`
and layers on:

```
python:3.11-slim-bookworm   ← 153 MB (Debian Bookworm, Python 3.11)
  └─ openjdk-17-jdk-headless    ← 290 MB (JVM for Spark)
  └─ JupyterLab 4.6.1           ← 30 MB (notebook front-end)
  └─ pandas 2.3.3 + matplotlib  ← 50 MB (DataFrame + charting)
  └─ PyIceberg 0.11.1           ← 10 MB (programmatic catalog access)
  └─ Spark 3.5.8 (hadoop3)     ← 330 MB (the big one)
  └─ Iceberg runtime jars       ← 40 MB (Spark + AWS bundle)
  └─ Connector jars             ← 20 MB (Postgres JDBC, hadoop-aws, SDK)
  └─ Config files + scripts     ← 1 MB (defaults, pipeline, notebooks)
```

### Layer structure (and why it matters)

Each `RUN` is a separate layer. Docker caches layers, so a flaky download only
invalidates the single failing layer on rebuild — the rest stays cached. The
order is deliberate: things that change less often come first.

1. **System deps** (`apt-get`): OpenJDK 17, `tini`, `curl`, `procps`. These
   almost never change.
2. **Python packages** (three separate `pip3 install` lines): JupyterLab,
   pandas/matplotlib, PyIceberg. Each is a separate layer so a transient PyPI
   error only re-downloads that one group.
3. **Spark tarball**: downloaded from `dlcdn.apache.org` with an archive.apache.org
   fallback. This is ~330 MB compressed and takes the longest.
4. **Iceberg runtime jars**: `iceberg-spark-runtime` (the Spark SQL
   extensions) and `iceberg-aws-bundle` (AWS SDK v2 for S3FileIO).
5. **Connector jars**: Postgres JDBC driver, `hadoop-aws` + AWS SDK v1 bundle
   (for `s3a://` reads — see [s3-access-paths.md](s3-access-paths.md)).
6. **Config files**: `spark-defaults.conf`, `pyiceberg.yaml`, IPython startup
   scripts.
7. **Seed notebooks**: copied into the image, then copied to the PVC on first
   start by the entrypoint (so edits survive pod restarts).
8. **Pipeline scripts**: the medallion ETL at `/opt/pipeline/`.

### Entrypoint

```
/usr/bin/tini -- /opt/entrypoint.sh
```

[`tini`](https://github.com/krallin/tini) is a lightweight init system (PID 1)
that properly forwards signals and reaps zombie processes. Spark spawns child
JVM processes; without tini they'd accumulate as defunct children.

`entrypoint.sh` does:
1. Copy seed notebooks to PVC if the PVC is empty (first start only).
2. Start Jupyter Lab in the background.
3. Block on `sleep infinity` (or, if arguments are passed, runs `spark-submit
   "$@"` for the pipeline — `make pipeline` uses this).

### Key design decisions

- **Python 3.11, not 3.12+**: Spark 3.5's PySpark hasn't been tested against
  Python 3.12. Using 3.11 avoids hard-to-diagnose crashes.
- **pip3, not `uv` or `poetry`**: `pip3` is simpler for a single-user image,
  and the dependency tree is small enough that resolution speed doesn't matter.
- **`openjdk-17-jdk-headless`**, not the full JDK: the `-headless` variant drops
  GUI and sound libraries (X11, ALSA) that aren't needed in a container.
- **`JAVA_HOME` via `readlink`**: `/opt/java-home` is resolved from `$(command
  -v java)` so the same Dockerfile works on amd64 and arm64 without hard-coding
  paths like `/usr/lib/jvm/java-17-openjdk-arm64/`.

## `iceberg-rest:local` — the catalog with a database backend

```
FROM alpine:3.20 AS downloader            ← multi-stage: downloader stage
  ├─ curl the Postgres JDBC jar           ←   saves the jar
FROM apache/iceberg-rest:1.10.1           ← final stage: the upstream image
  └─ COPY --from=downloader postgresql.jar  →  /opt/iceberg-rest/libs/
```

The upstream `apache/iceberg-rest:1.10.1` image ships the REST server fat jar
but **does not** include the Postgres JDBC driver — it's a runtime-scope
dependency in the Iceberg build, not a compile-time one. Without it,
`JdbcCatalog` throws `ClassNotFoundException: org.postgresql.Driver` and the
catalog falls back to the in-memory registry (tables disappear on restart).

The downloader stage uses `alpine:3.20` (lightweight, no JVM needed) to curl
the jar. The final stage copies it into `/opt/iceberg-rest/libs/`, which the
base image's entrypoint already has in its classpath via `libs/*`.

### Why not just `apache/iceberg-rest` as-is?

Without the Postgres driver, the catalog's table registry lives in memory. A
pod restart loses the list of tables — the Parquet and Iceberg metadata files
remain in SeaweedFS, but Spark has no way to discover them. With the Postgres
driver, `JdbcCatalog` stores table metadata in the `iceberg_catalog` database
on Postgres, surviving pod restarts.

## `loadgen:local` — one-shot data seeder

```
FROM python:3.11-slim-bookworm
  ├─ pip install psycopg2-binary boto3 Faker    ← Postgres + S3 + fake data
  └─ COPY generate_load.py                       ← the entrypoint script
```

A deliberately minimal image. It runs to completion, then exits. The
`60-loadgen.yaml` Job manifest sets `restartPolicy: Never` and
`backoffLimit: 3`, so Kubernetes restarts it only if it crashes, not when
it exits successfully.

`generate_load.py` does:
1. Connect to Postgres as `etluser` at `postgres:5432/oneshop`.
2. `TRUNCATE` the `users`, `items`, and `purchases` tables.
3. Seed them with synthetic rows using the `Faker` library (counts tunable via
   `USER_COUNT`, `ITEM_COUNT`, `PURCHASE_COUNT` env vars on the Job).
4. Connect to SeaweedFS S3 via `boto3` (`admin` / `password`, endpoint
   `http://seaweedfs:8333`).
5. Delete any existing files in the `pageviews` bucket, then write
   newline-delimited JSON pageview events.

Every run is idempotent: `TRUNCATE` + delete gives a clean slate, then
reseed. This is what makes `make loadgen` safe to re-run at any time.

### Why `boto3` not `s3a`?

The loadgen is a Python script, not a Spark job. It uses `boto3` (the AWS SDK
for Python, v1) which speaks native S3 REST API — the same protocol SeaweedFS
implements. There's no Hadoop or Iceberg dependency at all.

## `metabase:local` — BI dashboarding (opt-in)

```
FROM metabase/metabase:v0.53.9
  └─ ADD starburst-trino-driver.jar  →  /plugins/
```

The official Metabase image ships drivers for Postgres, MySQL, BigQuery, etc.,
but **not** Trino/Starburst — that's a community driver. Without it, the
"Trino" database type never appears in Metabase's "New Database" dropdown.

The `ADD` instruction downloads the Starburst Trino driver JAR directly from
GitHub Releases at build time:

```
https://github.com/starburstdata/metabase-driver/releases/download/
    6.1.0/starburst-6.1.0.metabase-driver.jar
```

Metabase automatically picks up any JAR in `/plugins/` at startup.

### Why not use the Metabase + Trino driver without a custom image?

Because Metabase needs the Trino driver JAR physically present when the JVM
starts. You could mount it from a ConfigMap or a shared volume, but a
ConfigMap can't hold a 10+ MB binary, and sharing the host filesystem adds
a dependency on the host node. Baking it into the image is the simplest
self-contained approach for a local lab.

## Upstream images (not custom)

These are pulled directly from Docker Hub (or your local cache):

| Image | Tag | Used by | Notes |
|---|---|---|---|
| `chrislusf/seaweedfs` | `4.37` | `10-seaweedfs.yaml` | Single-binary, no JVM |
| `minio/mc` | `latest` | `20-bucket-init.yaml` | S3-compatible CLI for bucket creation |
| `postgres` | `16` | `50-postgres.yaml` | Both the OLTP source and the catalog backend |
| `trinodb/trino` | `482` | `70-trino.yaml` | Interactive SQL engine (opt-in) |
| `busybox` | `1.38` | Init containers | Lightweight networking check (`nc -z`) |

### What about `minio/mc latest`?

This is the one `latest` tag in the repo — noted in
[configuration.md](configuration.md) as something to pin for reproducibility.
`minio/mc` uses date-stamped `RELEASE.*` tags instead of semver, so there's no
obvious semver pin. The `scripts/check.sh` linter explicitly allows
`minio/mc:latest` as the one exception.

## Build pipeline

When you run `make build` (or `make up` which calls it):

1. Each custom Dockerfile is built natively for the host CPU architecture via
   `docker buildx build --load`.
2. `kind load docker-image` loads the resulting images into the kind cluster's
   Docker-in-Docker engine (no registry needed).
3. `make build` then `rollout restart`s the `spark-iceberg` deployment so the
   new image takes effect (the image tag `:local` is unchanged, so a plain
   `kubectl apply` wouldn't restart the pod).

The images stay local to your machine — they are never pushed to a registry.

## See also

- [configuration.md](configuration.md) — version pins, credentials, resource
  requests
- [s3-access-paths.md](s3-access-paths.md) — why two S3 SDKs coexist in the
  spark-iceberg image
- [kubernetes-resources.md](kubernetes-resources.md) — how these images are
  deployed as Kubernetes workloads
