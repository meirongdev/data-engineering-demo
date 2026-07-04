# Kubernetes resources

This lab runs everything in a single [kind](https://kind.sigs.k8s.io/)
Kubernetes cluster. The k8s manifests in `k8s/` define all the resources the
lakehouse needs. This page explains the Kubernetes resource types used, with
concrete examples from this repo.

> If you are new to Kubernetes, think of it as an operating system for
> containers — it schedules them onto machines (nodes), keeps them running
> (controllers), and provides networking and storage (Services, PVCs). The
> manifests are declarative YAML files that describe "what" you want; the
> cluster figures out "how" to make it happen.

## Namespace (`00-namespace.yaml`)

**What it is:** A virtual cluster inside the physical cluster — groups related
resources together and provides scope for names.

**In this lab:** Everything lives in the `lakehouse` namespace. All `kubectl`
commands use `-n lakehouse` (or the `kc` helper from `lib.sh`) so they don't
accidentally touch resources in other namespaces.

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: lakehouse
```

Without a namespace, resources would land in the `default` namespace and
collide with anything else running on the cluster.

## Deployment (most workloads)

**What it is:** A controller that ensures a specified number of identical pods
are always running. If a pod crashes, the Deployment replaces it. Rolling
updates let you change the image/configuration without downtime.

**Key fields:**

| Field | Meaning | Example from `10-seaweedfs.yaml` |
|---|---|---|
| `replicas` | Desired pod count | `1` |
| `strategy.type` | How to replace pods on update | `Recreate` — stop the old pod before starting the new one (needed when sharing a PVC) |
| `template.spec.containers` | The container image, ports, env, probes, resource requests | `chrislusf/seaweedfs:4.37` |
| `template.spec.volumes` | Volumes attached to the pod | PVC, ConfigMap |

### Probes

Kubernetes uses probes to decide whether a container is alive or ready:

```yaml
readinessProbe:
  tcpSocket:
    port: 8333        # S3 gateway port
livenessProbe:
  tcpSocket:
    port: 9333        # master port
```

- **Readiness probe** — is the service ready to accept traffic? If it fails,
  the pod is removed from the Service's endpoints.
- **Liveness probe** — is the process healthy? If it fails, Kubernetes kills
  and restarts the pod.

### Resource requests and limits

```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "100m"
  limits:
    memory: "1Gi"
```

- **Requests** — guaranteed minimum resources (the scheduler uses this to pick
  a node)
- **Limits** — maximum the pod is allowed to use (exceeding = OOMKilled)

`100m` CPU = 0.1 core. See [configuration.md](configuration.md) for per-component values.

## Service + NodePort

**What it is:** A stable network endpoint backed by one or more pods. Services
abstract away pod IPs (which change on restart).

```yaml
apiVersion: v1
kind: Service
metadata:
  name: seaweedfs
spec:
  type: NodePort
  selector:
    app: seaweedfs
  ports:
    - name: s3
      port: 8333
      nodePort: 30333
```

- **`type: NodePort`** — exposes the service on a fixed port on every worker
  node. Combined with the kind host port mapping in `cluster/kind-config.yaml`,
  this makes the service reachable at `localhost:8333`.
- **`selector: {app: seaweedfs}`** — the Service routes traffic to pods with
  label `app=seaweedfs`.
- **Internal DNS** — inside the cluster, other pods reach it as
  `seaweedfs:8333` (Services get a DNS name matching their name).

See [architecture.md](architecture.md) for the full port table.

## PersistentVolumeClaim (PVC)

**What it is:** A request for storage. Kubernetes binds it to a
PersistentVolume (PV), which in kind's case is backed by the KinD node's
filesystem via the `standard` StorageClass (local-path provisioner).

| PVC | Size | Used by | Data |
|---|---|---|---|
| `seaweedfs-data` | 2 Gi | SeaweedFS | All Parquet + Iceberg metadata files |
| `notebooks` | 1 Gi | Spark pod | Your notebook edits |
| `postgres-data` | 1 Gi | Postgres | The `oneshop` source database |
| `metabase-data` | 1 Gi | Metabase | Saved questions/dashboards |

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: seaweedfs-data
spec:
  accessModes:
    - ReadWriteOnce        # single pod read-write
  resources:
    requests:
      storage: 2Gi
```

**Lifetime:** PVCs survive pod restarts but are destroyed when the cluster is
deleted (`make down`). This is a trade-off: if the SeaweedFS PVC is lost, all
Iceberg table data is gone (see [production.md](production.md) for production
storage options).

## ConfigMap

**What it is:** In-cluster configuration key-value store. ConfigMaps are
mounted as files or injected as environment variables, keeping configuration
separate from the container image.

Examples from this lab:

`10-seaweedfs.yaml` mounts a ConfigMap as a file:

```yaml
volumes:
  - name: config
    configMap:
      name: seaweedfs-s3-config
```

This creates `/etc/seaweedfs/s3.json` inside the container with the S3
identity (`admin` / `password`).

`30-iceberg-rest.yaml` uses ConfigMap values as environment variables:

```yaml
envFrom:
  - configMapRef:
      name: iceberg-rest-config
```

This injects all key-value pairs from the `iceberg-rest-config` ConfigMap
(`CATALOG_WAREHOUSE=s3://warehouse/`, etc.) as env vars — cleaner than
hardcoding them in the manifest.

## Init containers

**What it is:** Containers that run to completion **before** the main container
starts. If an init container fails, the pod restarts it.

In this lab, init containers gate startup on dependencies:

```yaml
initContainers:
  - name: wait-for-seaweedfs
    image: busybox:1.38
    command:
      - sh
      - -c
      - until nc -z seaweedfs 8333; do sleep 2; done
```

The init container loops until TCP port 8333 (SeaweedFS S3) responds. The main
container never starts until the dependency is ready. This means manifests can
be applied in any order — if SeaweedFS isn't up yet, the catalog just waits.

Init container dependencies by component:

| Component | Waits for |
|---|---|
| `iceberg-rest` | SeaweedFS (`:8333`) |
| `spark-iceberg` | SeaweedFS (`:8333`) + `iceberg-rest` (`:8181`) |
| `loadgen` | SeaweedFS (`:8333`) + Postgres (`:5432`) |
| `trino` | SeaweedFS (`:8333`) + `iceberg-rest` (`:8181`) |
| `metabase` | Trino (`:8080`) |

## Job

**What it is:** Like a Deployment but runs to completion and stops, rather than
keeping a pod running forever.

Two Jobs in this lab:

| Job | Purpose | Runs when |
|---|---|---|
| `bucket-init` | Create `warehouse`, `pageviews`, and `customer-segments` buckets on SeaweedFS | `make deploy` |
| `loadgen` | Seed Postgres tables and write pageview JSON | `make pipeline` / `make loadgen` |

Jobs have a `backoffLimit` (retry count) and `ttlSecondsAfterFinished` (how
long to keep the completed pod around for log inspection). The `bucket-init`
Job also deletes itself after 600 seconds via `ttlSecondsAfterFinished: 600`.

## CronJob (`90-pipeline-cron.yaml`, optional)

**What it is:** A Job on a schedule. A CronJob holds a `jobTemplate` and a cron
`schedule`; at each firing the controller stamps out a fresh Job (which runs a
pod to completion, exactly like the Jobs above) and then stops. It is the
Kubernetes-native answer to "re-run this batch work every night" — no Airflow or
external scheduler needed.

The `pipeline-refresh` CronJob re-runs the whole medallion sequence
(bronze → silver → gold) so the gold tables reflect current source state:

| Field | Value | Why |
|---|---|---|
| `schedule` | `"0 6 * * *"` | Daily at 06:00 UTC (standard 5-field cron). |
| `concurrencyPolicy` | `Forbid` | Skip a firing if the previous run is still going, rather than stacking overlapping pipeline runs on the same tables. |
| `restartPolicy` | `Never` | A failed stage surfaces as a failed Job, not an infinite in-pod retry loop. |

Unlike the two Jobs above, this CronJob is **not** applied by `make deploy` — it
is opt-in:

```bash
kubectl apply -f k8s/90-pipeline-cron.yaml     # install the schedule
kubectl get cronjobs,pods -n lakehouse -l app=pipeline-refresh
kubectl delete cronjob pipeline-refresh -n lakehouse   # remove it
```

It runs the full `10`→`50` stage sequence rather than gold alone: a gold-only
refresh would recompute from *stale* silver, and `sales_performance_24h`'s
rolling-24h filter would age purchases out until the table trends empty. Note it
does **not** re-run the loadgen, so no *new* source rows arrive between firings —
keeping source data fresh would mean also scheduling the loadgen Job, which is
out of scope for this lab. See [pipeline.md](pipeline.md#automated-daily-refresh-optional)
for the operational detail.

## The `Recreate` strategy — why it matters

Most deployments in this lab use `strategy.type: Recreate` instead of the
default `RollingUpdate`. This is because they mount a `ReadWriteOnce` PVC,
which can only be attached to one pod at a time. A rolling update would start
a new pod before stopping the old one, and the new pod would hang waiting for
the PVC to become free.

The trade-off: there is a brief downtime during updates. For a development
lab this is fine; for production you would use a replicated storage backend or
`ReadWriteMany` PVCs.

## How resources are applied

`scripts/deploy.sh` applies manifests in dependency order using `kubectl apply
-f <file>`, waiting for each group to be ready before proceeding:

1. Namespace
2. SeaweedFS (storage) + wait for readiness
3. Bucket init Job + wait for completion
4. Postgres + wait for readiness
5. Iceberg REST catalog + wait for readiness
6. Spark + Jupyter + wait for readiness

The init containers inside each pod provide a second layer of dependency
gating, so the system converges even if manifests were applied out of order.

## See also

- [architecture.md](architecture.md) — component-level view of the same resources
- [operations.md](operations.md) — deploy order and rebuild loop
- [production.md](production.md) — what to change for production Kubernetes
- [troubleshooting.md](troubleshooting.md) — pod stuck `Init:0/1`, `ImagePullBackOff`, etc.
