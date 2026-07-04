# Serving layer: interactive SQL + BI

The serving layer adds **interactive query (Trino)** and **BI dashboards
(Metabase)** on top of the Iceberg lakehouse — the final piece of the
*storage → catalog → ETL compute → interactive query → BI* story.

It is **opt-in** (`make serving`) because Trino and Metabase together add ~2.5 GB
JVM heap, pushing the base lab past ~4 GB. Run it *after* the pipeline has
populated the gold layer (`make pipeline`).

## The shared-catalog concept

Trino talks to the **same** `iceberg-rest` catalog and the **same** SeaweedFS
data files that Spark writes. When Spark's medallion pipeline writes a gold
table, Trino sees it immediately — there is no copy, no export, no ETL to
prepare data for querying. That decoupled-compute property is the core lakehouse
value proposition:

```
Spark (batch ETL) ──→ Iceberg REST catalog ←── Trino (interactive SQL)
                          │
                    SeaweedFS (data files)
```

Both engines authenticate with the same S3 credentials (`admin` / `password`)
and use the same REST catalog endpoint (`iceberg-rest:8181` in-cluster).

## Components

### Trino (`k8s/70-trino.yaml`)

- **Image:** `trinodb/trino:482`
- **Port:** `8080` (host) → `30080` (NodePort) — mapped to `localhost:8080`
- **Heap:** 1.5 GB (Xmx1536M in `jvm.config`)
- **Connector:** Iceberg (REST catalog) — namespace `iceberg` in Trino, tables
  appear as `iceberg.<namespace>.<table>` (e.g. `iceberg.gold.item_performance`)
- **Config:** mounted from the `trino-config` and `trino-catalog` ConfigMaps

The Iceberg connector uses the **native S3 filesystem**
(`fs.s3.enabled=true`), which was renamed from `fs.native-s3.enabled` in the
Trino 4xx line. This is the modern, recommended S3 access path.

### Metabase (`docker/metabase/Dockerfile` + `k8s/80-metabase.yaml`)

- **Image:** locally built `metabase:local` — wraps `metabase/metabase:v0.53.9`
  with the Starburst Trino driver at `/plugins/`
- **Port:** `3000` (host) → `30300` (NodePort) — mapped to `localhost:3000`
- **Heap:** 1 GB (`-Xmx1G` in `JAVA_OPTS`)
- **Metadata store:** Embedded H2 on a 1 Gi PVC so saved questions/dashboards
  survive pod restarts
- **Dependency:** init container waits for Trino before starting

Metabase does **not** bundle a Trino driver — the Starburst community driver
JAR must be baked into the image. This is the main gotcha vs Superset's
`pip install trino[sqlalchemy]`.

## Usage

### Prerequisites

```bash
# Full stack + pipeline first
make up
make pipeline

# Then deploy the serving layer
make serving
```

**Note:** If you created the cluster *before* adding the serving port mappings
to `kind-config.yaml`, the NodePorts won't be reachable from the host. Either:

```bash
make down && make up && make pipeline && make serving    # recreate cluster
```

or use `kubectl port-forward` (no cluster recreation needed):

```bash
kubectl port-forward -n lakehouse svc/trino 8080:8080 &
kubectl port-forward -n lakehouse svc/metabase 3000:3000 &
```

### Query with Trino

Trino's web UI: http://localhost:8080

From the Trino CLI (inside the cluster or if you have the Trino CLI installed):

```sql
SHOW CATALOGS;                           -- should show "iceberg"
SHOW SCHEMAS FROM iceberg;               -- shows bronze, silver, gold
SHOW TABLES FROM iceberg.gold;           -- shows item_performance
SELECT * FROM iceberg.gold.item_performance LIMIT 10;
```

From a notebook (`05-query-with-trino.ipynb`), connect via the Trino JDBC driver
or the Trino Python client:

```python
from trino.dbapi import Connection
conn = Connection(host="trino", port=8080, catalog="iceberg")
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM gold.item_performance")
print(cur.fetchone())
```

### BI with Metabase

1. Open http://localhost:3000
2. Create a first admin account on the setup screen
3. Add a **new database connection**:

   | Setting | Value |
   |---|---|
   | Database type | Trino |
   | Host | `trino` |
   | Port | `8080` |
   | Database name | `iceberg` |
   | Username | (leave blank — Trino has no auth in this lab) |
   | Password | (leave blank) |

4. Metabase will scan the `iceberg` catalog and discover the gold-layer tables.
5. Build a question or dashboard on `item_performance`.

The Trino driver is already installed in the `metabase:local` image — the
"Trino" database type should appear in the dropdown automatically.

## Resource budget

The serving layer adds ~2.5 GB on top of the base lab:

| Component | Memory request | Memory limit |
|---|---|---|
| Trino | 2 Gi | 3 Gi |
| Metabase | 1 Gi | 2 Gi |

Base lab is ~4 GB; total with serving is ~6 GB recommended free RAM.

## Port reference

| Host port | NodePort | Service | Purpose |
|---|---|---|---|
| 8080 | 30080 | Trino | Interactive SQL, Trino web UI |
| 3000 | 30300 | Metabase | BI dashboards |

## See also

- [`k8s/70-trino.yaml`](../k8s/70-trino.yaml) — Trino manifest + config
- [`k8s/80-metabase.yaml`](../k8s/80-metabase.yaml) — Metabase manifest
- [`docker/metabase/Dockerfile`](../docker/metabase/Dockerfile) — Metabase image with Trino driver
- [architecture.md](architecture.md) — how the serving layer fits the overall stack
