# Troubleshooting

First stop for any issue: `make status` (what's running) and `make logs` (tail
Spark/Jupyter). For a specific component:

```bash
kubectl -n lakehouse logs deploy/<seaweedfs|iceberg-rest|spark-iceberg|postgres>
kubectl -n lakehouse logs job/<bucket-init|loadgen>     # the one-shot Jobs
kubectl -n lakehouse describe pod -l app=<name>         # events, probe failures, pull errors
```

## `make up` says Docker isn't running

Start Docker Desktop (or your Docker daemon) and retry. The preflight in
`scripts/up.sh` checks `docker info` and fails fast if the daemon is down.

## `spark-iceberg` pod: `ErrImageNeverPull` / image not found

The image `spark-iceberg:local` wasn't loaded into the cluster. It uses
`imagePullPolicy: IfNotPresent` and only exists locally, so it must be
`kind load`ed. Run `make build` (which builds **and** loads both local images —
`spark-iceberg:local` and `loadgen:local` — and restarts `spark-iceberg`), then
`make deploy`. The same applies to the `loadgen` Job if it shows
`ErrImageNeverPull`.

## Pod stuck `ImagePullBackOff` on `apache/iceberg-rest-fixture`

On Apple Silicon some upstream images are amd64-only and run under emulation
(slower, but they work). If a specific tag is unavailable, change it to `:latest`
in `k8s/30-iceberg-rest.yaml` and re-`make deploy`.

## A pod is stuck `Init:0/1`

Init containers gate startup on dependencies:

- `iceberg-rest` waits for SeaweedFS on `:8333`.
- `spark-iceberg` waits for **both** SeaweedFS (`:8333`) and `iceberg-rest`
  (`:8181`).
- `loadgen` waits for **both** Postgres (`:5432`) and SeaweedFS (`:8333`).

If it hangs, the dependency isn't ready. Check that SeaweedFS rolled out
(`kubectl -n lakehouse get pods`) and that the `bucket-init` Job completed.

## `bucket-init` Job never completes

It loops until SeaweedFS S3 answers, then creates the `warehouse` and
`pageviews` buckets (`mc mb --ignore-existing`). Check its logs:

```bash
kubectl -n lakehouse logs job/bucket-init
```

If SeaweedFS isn't reachable, the Job retries (`backoffLimit: 10`). Confirm the
`seaweedfs` Deployment is ready first.

## Writes fail with S3 / addressing errors

SeaweedFS **requires path-style S3 addressing** (no bucket-as-subdomain). This is
already set in three places — if you changed one, make them consistent:

- `spark-defaults.conf`: `spark.sql.catalog.demo.s3.path-style-access true`
- `pyiceberg.yaml`: `s3.path-style-access: "true"`
- `30-iceberg-rest.yaml`: `CATALOG_S3_PATH__STYLE__ACCESS: "true"`

Also confirm the `warehouse` bucket exists (the `bucket-init` Job) — `S3FileIO`
never creates buckets.

## Tables disappeared after a restart, but the data is still in S3

The REST fixture keeps its **table registry in-memory**. Restarting the
`iceberg-rest` pod loses the catalog's list of tables even though the Parquet and
metadata files remain in SeaweedFS. This is expected for this demo catalog —
recreate the tables (or re-run the notebook) to re-register them.

## Spark UI at :4040 shows nothing

The driver UI only exists **while a Spark session is active**. Start a session in
a notebook (any `spark.sql(...)`), then refresh http://localhost:4040.

## Port already in use on the host

The lab maps host ports 8888, 4040, 8181, 8333, 9333 and 5432 to cluster
NodePorts. If one is taken (e.g. another kind cluster, a local Jupyter, or a
local Postgres on 5432), free it or edit the mappings in
`cluster/kind-config.yaml` and recreate the cluster (`make down && make up`).

## `make pipeline`: loadgen Job fails

`scripts/pipeline.sh` prints the Job's logs on failure. Common causes:

- **Postgres not ready** — the init container waits for `:5432`, but if Postgres
  is crash-looping the seed fails. Check `kubectl -n lakehouse logs deploy/postgres`
  and that `make deploy` rolled it out.
- **Bucket missing** — the loadgen creates the `pageviews` bucket if absent, but
  if S3 auth is off it can't. Confirm `bucket-init` completed and the
  `admin`/`password` identity is intact in `k8s/10-seaweedfs.yaml`.

Re-run just the seeding with `make loadgen`. It's idempotent (TRUNCATEs Postgres
and clears the bucket first).

## Pipeline stage fails reading Postgres (JDBC)

`10_postgres_to_bronze` connects as `etluser` to
`jdbc:postgresql://postgres:5432/oneshop`. If it errors:

- `No suitable driver` → the `postgresql-*.jar` isn't in the image. Rebuild:
  `make build`.
- Auth/`relation does not exist` → the Postgres bootstrap didn't run (it only
  runs on an empty data dir). If you changed `50-postgres.yaml`'s bootstrap SQL,
  the existing `postgres-data` PVC keeps the old schema — `make down && make up`,
  or drop the PVC, to re-bootstrap.

## Pipeline stage fails reading pageviews (s3a)

`20_s3_to_bronze` reads `s3a://pageviews/`. If it errors:

- `No FileSystem for scheme "s3a"` or `ClassNotFoundException` → the
  `hadoop-aws` / `aws-java-sdk-bundle` jars are missing or mismatched. They must
  match the Spark build's Hadoop version (3.3.4 for Spark 3.5.x). Rebuild:
  `make build`.
- `No pageview data found` (the stage exits non-zero) → the bucket is empty. Run
  the loadgen first (`make loadgen`, or the full `make pipeline`).

## `make build` didn't seem to change anything in the pod

The image tag (`spark-iceberg:local`) is unchanged across rebuilds, so a plain
`kubectl apply`/`make deploy` will **not** restart the pod. `make build` handles
this — it `kind load`s the new image and then `rollout restart`s
`spark-iceberg`. If you loaded an image by hand, restart the pod yourself:
`kubectl -n lakehouse rollout restart deploy/spark-iceberg`.

## Start clean

When in doubt, tear it all down and rebuild:

```bash
make down
make up
make smoke
```
