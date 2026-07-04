# Operations

The `Makefile` is a thin wrapper over the scripts in `scripts/`. Run `make help`
for the list. All state-changing scripts share `scripts/lib.sh` (config +
logging helpers + the `kc` kubectl wrapper).

## Make targets

| Command | What it does | Script |
|---|---|---|
| `make up` | Create the cluster, build/load the images, deploy the full stack | `scripts/up.sh` |
| `make build` | Rebuild `spark-iceberg:local` + `loadgen:local` and `kind load` them | `scripts/build-image.sh` |
| `make deploy` | (Re)apply the k8s manifests and wait for readiness | `scripts/deploy.sh` |
| `make smoke` | End-to-end Iceberg write/read test inside the cluster | `scripts/smoke-test.sh` |
| `make pipeline` | Run the medallion pipeline: loadgen → bronze → silver → gold | `scripts/pipeline.sh` |
| `make loadgen` | (Re)run just the loadgen Job (seed Postgres + pageviews) | (kubectl) |
| `make serving` | (Opt-in) deploy the Trino + Metabase serving layer | `scripts/deploy-serving.sh` |
| `make status` | Show pods, services, and host URLs | `scripts/status.sh` |
| `make logs` | Tail the Spark/Jupyter deployment logs | (kubectl) |
| `make jupyter` | Open Jupyter Lab in your browser | (open) |
| `make shell` | Open a shell inside the Spark/Jupyter pod | (kubectl) |
| `make down` | Delete the kind cluster and all its data | `scripts/down.sh` |

## Deploy order

`scripts/deploy.sh` applies the manifests in strict dependency order, waiting for
each to be ready before the next:

1. `00-namespace.yaml` — namespace `lakehouse`
2. `10-seaweedfs.yaml` — object storage → `rollout status deploy/seaweedfs`
3. `20-bucket-init.yaml` — create `warehouse` + `pageviews` buckets → `wait job/bucket-init complete`
4. `50-postgres.yaml` — Postgres source → `rollout status deploy/postgres`
5. `30-iceberg-rest.yaml` — REST catalog → `rollout status deploy/iceberg-rest`
6. `40-spark-iceberg.yaml` — Spark + Jupyter → `rollout status deploy/spark-iceberg`

`60-loadgen.yaml` is **not** applied by `make deploy` — it's an on-demand Job run
by `make pipeline` / `make loadgen`. The manifests' own init containers also gate
startup (the catalog waits for SeaweedFS; Spark and loadgen wait for their deps),
so a re-apply out of order still converges.

## Running the pipeline

`make pipeline` (`scripts/pipeline.sh`) runs the whole medallion flow: it
(re)applies the loadgen Job and waits for it, then `spark-submit`s each stage in
`/opt/pipeline/` inside the Spark pod, in order. The pipeline scripts are baked
into the image, so **after editing anything in `docker/spark/pipeline/` run
`make build`** (which reloads the image and restarts the pod) before
`make pipeline`. See [pipeline.md](pipeline.md).

## The rebuild / iterate loop

- **Changed a k8s manifest, Spark/PyIceberg config baked into the image, or
  script** → re-run the relevant step, then re-verify:
  - Manifest only: `make deploy && make smoke`
  - Dockerfile / image config (`spark-defaults.conf`, `pyiceberg.yaml`, IPython
    startup, seed notebooks, `docker/spark/pipeline/` scripts):
    `make build && make smoke` (or `make pipeline`). The image tag is unchanged,
    so `make build` rolls `spark-iceberg` itself after loading the new image —
    a plain `kubectl apply`/`make deploy` would *not* restart the pod.
- **Notebook content** → edit live in Jupyter at http://localhost:8888; changes
  persist on the notebooks PVC.
- **Inspect a running pod** → `make logs` (tail Spark/Jupyter) or `make shell`
  (drop into the pod). For any component:
  `kubectl -n lakehouse logs deploy/<seaweedfs|iceberg-rest|spark-iceberg>`.

## Configuration via environment

The scripts and the Makefile read three overridable variables (defaults from
`scripts/lib.sh`):

| Variable | Default | Meaning |
|---|---|---|
| `CLUSTER_NAME` | `data-eng` | kind cluster name (context becomes `kind-<name>`) |
| `NAMESPACE` | `lakehouse` | Kubernetes namespace |
| `IMAGE` | `spark-iceberg:local` | Spark/Iceberg/Jupyter image tag |
| `LOADGEN_IMAGE` | `loadgen:local` | load generator image tag |

Override per-invocation, e.g.:

```bash
CLUSTER_NAME=lakehouse-2 make up
make CLUSTER_NAME=lakehouse-2 logs
```

The `kc` helper in `lib.sh` wraps `kubectl --context kind-${CLUSTER_NAME} -n
${NAMESPACE}`; the Makefile's `logs`/`shell` targets use the same variables, so
overrides apply consistently across scripts and `make`.

> Note: `cluster/kind-config.yaml` hard-codes the cluster name `data-eng` and the
> NodePort→host port mappings. If you override `CLUSTER_NAME`, the scripts create
> a cluster with your name but the config file's `name:` field is ignored by
> `kind create --name`; the host port maps still apply. Running two clusters at
> once will collide on the same host ports.

For what's inside the image and the manifests, see
[configuration.md](configuration.md).
