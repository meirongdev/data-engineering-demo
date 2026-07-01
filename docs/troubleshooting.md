# Troubleshooting

First stop for any issue: `make status` (what's running) and `make logs` (tail
Spark/Jupyter). For a specific component:

```bash
kubectl -n lakehouse logs deploy/<seaweedfs|iceberg-rest|spark-iceberg|job/bucket-init>
kubectl -n lakehouse describe pod -l app=<name>     # events, probe failures, pull errors
```

## `make up` says Docker isn't running

Start Docker Desktop (or your Docker daemon) and retry. The preflight in
`scripts/up.sh` checks `docker info` and fails fast if the daemon is down.

## `spark-iceberg` pod: `ErrImageNeverPull` / image not found

The image `spark-iceberg:local` wasn't loaded into the cluster. It uses
`imagePullPolicy: IfNotPresent` and only exists locally, so it must be
`kind load`ed. Run `make build` (which builds **and** loads it), then
`make deploy`.

## Pod stuck `ImagePullBackOff` on `apache/iceberg-rest-fixture`

On Apple Silicon some upstream images are amd64-only and run under emulation
(slower, but they work). If a specific tag is unavailable, change it to `:latest`
in `k8s/30-iceberg-rest.yaml` and re-`make deploy`.

## A pod is stuck `Init:0/1`

Init containers gate startup on dependencies:

- `iceberg-rest` waits for SeaweedFS on `:8333`.
- `spark-iceberg` waits for **both** SeaweedFS (`:8333`) and `iceberg-rest`
  (`:8181`).

If it hangs, the dependency isn't ready. Check that SeaweedFS rolled out
(`kubectl -n lakehouse get pods`) and that the `bucket-init` Job completed.

## `bucket-init` Job never completes

It loops until SeaweedFS S3 answers, then runs `mc mb --ignore-existing
sw/warehouse`. Check its logs:

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

The lab maps host ports 8888, 4040, 8181, 8333, 9333 to cluster NodePorts. If one
is taken (e.g. another kind cluster, or a local Jupyter), free it or edit the
mappings in `cluster/kind-config.yaml` and recreate the cluster (`make down &&
make up`).

## Start clean

When in doubt, tear it all down and rebuild:

```bash
make down
make up
make smoke
```
