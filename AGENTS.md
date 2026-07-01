# Repository Guidelines

## Project Structure & Module Organization

This repository is a local Iceberg lakehouse lab running on kind. The kind
cluster definition (1 control-plane + 2 workers, host port maps) lives in
`cluster/kind-config.yaml`. Kubernetes manifests live in `k8s/` and are applied
in numeric order: namespace, SeaweedFS, bucket init, Iceberg REST, then
Spark/Jupyter. The Spark image is in `docker/spark/`, including the Dockerfile,
`entrypoint.sh`, Spark defaults, PyIceberg config, and IPython startup scripts.
`notebooks/` contains seed Jupyter notebooks copied into the notebook PVC on
first start. `scripts/` holds the operational shell commands, with shared
configuration in `scripts/lib.sh`. The `Makefile` is a thin wrapper around those
scripts.

## Prerequisites

Docker (running), `kind`, and `kubectl`, plus ~4 GB free RAM for Docker. All are
preflight-checked by `scripts/up.sh`.

## Build, Test, and Development Commands

- `make help`: list available repo commands.
- `make up`: create the kind cluster, build/load `spark-iceberg:local`, and
  deploy the full stack.
- `make build`: rebuild the Spark/Iceberg/Jupyter image and load it into kind.
- `make deploy`: reapply Kubernetes manifests and wait for readiness.
- `make smoke`: run the end-to-end Iceberg write/read test inside the cluster.
- `make status`: show pods, services, and local URLs.
- `make logs`: tail Spark/Jupyter deployment logs.
- `make jupyter`: open Jupyter Lab (`http://localhost:8888`) in the browser.
- `make shell`: open a shell inside the Spark/Jupyter pod.
- `make down`: delete the kind cluster and all demo data.

Config is overridable via env (or `make VAR=value`), defaulting to
`CLUSTER_NAME=data-eng`, `NAMESPACE=lakehouse`, and `IMAGE=spark-iceberg:local`
(see `scripts/lib.sh`). Local endpoints once the stack is up: Jupyter `:8888`,
Iceberg REST `:8181`, SeaweedFS S3 `:8333`, SeaweedFS master UI `:9333`, and the
Spark driver UI `:4040` (live only while a notebook Spark session runs).

## Coding Style & Naming Conventions

Shell scripts use Bash with `set -euo pipefail`; keep shared helpers in
`scripts/lib.sh` and call `kc` for namespace/context-aware `kubectl` commands.
Keep Kubernetes manifest filenames numerically prefixed to preserve deployment
order. Prefer explicit, descriptive resource names matching the existing
components, such as `spark-iceberg`, `iceberg-rest`, and `seaweedfs`.

## Testing Guidelines

Use `make smoke` as the primary regression test after changes to manifests,
scripts, Spark configuration, or storage/catalog wiring. For Dockerfile changes,
run `make build` before `make smoke`. Notebook changes should be manually run in
Jupyter at `http://localhost:8888` when they affect user-facing examples.

## Commit & Pull Request Guidelines

There is no existing commit history, so no repository-specific commit convention
is established yet. Use short imperative subjects, for example
`Add Spark smoke test notes`. Pull requests should summarize the changed layer
(`k8s`, `docker/spark`, `scripts`, or `notebooks`), include verification output
such as `make smoke`, and mention any required environment variables like
`CLUSTER_NAME`, `NAMESPACE`, or `IMAGE`.

## Security & Configuration Tips

This lab uses static demo credentials (`admin` / `password`) for SeaweedFS S3.
Do not reuse them outside local development. Avoid committing generated notebook
outputs, local secrets, or cluster-specific artifacts.
