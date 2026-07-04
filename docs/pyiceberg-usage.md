# PyIceberg usage

[PyIceberg](https://py.iceberg.apache.org/) is the Python client for the
Iceberg table format. It lets you read and manage Iceberg tables **without
Spark** — useful for lightweight catalog inspection, data quality checks, or
integrating Iceberg tables into Python-native workflows.

This page covers how PyIceberg is configured in this lab and what you can do
with it from a notebook or the command line.

## Configuration

The image includes `/root/.pyiceberg.yaml`, defining a catalog named `default`:

```yaml
catalog:
  default:
    uri: http://iceberg-rest:8181
    s3.endpoint: http://seaweedfs:8333
    s3.access-key-id: admin
    s3.secret-access-key: password
    s3.path-style-access: "true"
    s3.region: us-east-1
```

This mirrors the `demo` catalog from `spark-defaults.conf` — same REST
endpoint, same S3 credentials — so you can point PyIceberg at the same tables
that Spark writes.

## Basic usage

```python
from pyiceberg.catalog import load_catalog

cat = load_catalog("default")
```

### List namespaces and tables

```python
# List all namespaces (bronze, silver, gold)
cat.list_namespaces()          # → [('bronze',), ('silver',), ('gold',)]

# List tables in a namespace
cat.list_tables("bronze")      # → [('bronze', 'pageviews'), ('bronze', 'users'), ...]
```

### Read a table

```python
tbl = cat.load_table("bronze.pageviews")

# Scan all rows
df = tbl.scan().to_pandas()
df.head()
```

Scanning uses PyArrow under the hood — it reads the Parquet files directly
from SeaweedFS via the S3FileIO (same as Spark). No JVM involved.

### Read with filters

```python
from pyiceberg.expressions import EqualTo, GreaterThanOrEqual

# Push down filters to the scan — Iceberg uses manifest stats to skip files
tbl.scan(
    row_filter=EqualTo("channel", "web")
).to_pandas()

# Time range filter
tbl.scan(
    row_filter=GreaterThanOrEqual("received_at", "2025-06-01")
).to_pandas()
```

Filters are pushed down to Iceberg's metadata layer — the manifest statistics
are checked first, and only data files that could satisfy the filter are
opened. This is the same optimisation Spark uses, without Spark.

### Query with DuckDB (optional)

If PyIceberg was installed with the `duckdb` extra (as it is in this image),
you can query Iceberg tables through DuckDB:

```python
tbl = cat.load_table("gold.item_performance")
con = tbl.scan().to_duckdb("perf")
con.sql("SELECT item_name, conversion_rate FROM perf ORDER BY conversion_rate DESC LIMIT 5")
```

DuckDB runs in-process (no separate server), so this is fast for the small
datasets in this lab.

### Inspect table metadata

```python
# Current snapshot ID
tbl.current_snapshot().snapshot_id

# Schema
tbl.schema()                   # lists all columns and types

# Partition spec
tbl.spec()                     # shows how the table is partitioned

# All snapshots
list(tbl.snapshots())          # iterates over every snapshot (warning: many on an active table)
```

### Time travel with PyIceberg

```python
# Load the table as of a specific snapshot
snapshots = list(tbl.snapshots())
old_snapshot = snapshots[0].snapshot_id

tbl.scan(snapshot_id=old_snapshot).to_pandas()   # data at the first snapshot
```

## When to use PyIceberg vs. Spark

| Task | Use | Why |
|---|---|---|
| Inspect table schemas, snapshots, partitions | PyIceberg | Lightweight, no Spark session overhead |
| Quick `SELECT` from a notebook cell | PyIceberg or `%%sql` | PyIceberg needs no JVM warm-up |
| Filter + read a gold table into pandas | PyIceberg | Direct to Arrow/pandas, no serialisation overhead |
| Join multiple tables | Spark | PyIceberg has no join engine (delegate to DuckDB or pandas) |
| Write/transform data | Spark | PyIceberg is read-optimised; writes require manual `TableAppender` |
| Large-scale scans (> 100 MB of Parquet) | Spark | Spark's optimiser and codegen outperform PyIceberg on big data |

## CLI quick start

From `make shell` inside the Spark pod:

```python
from pyiceberg.catalog import load_catalog
cat = load_catalog("default")

# Check the gold table is there
tbl = cat.load_table("gold.item_performance")
print(f"Rows: {tbl.scan().to_pandas().shape[0]}")

# Top products by revenue
df = tbl.scan().to_pandas()
print(df.nlargest(5, "revenue")[["item_name", "revenue", "conversion_rate"]])
```

## See also

- [PyIceberg docs](https://py.iceberg.apache.org/) — official API reference
- [getting-started.md](getting-started.md) — PyIceberg example in `00-getting-started.ipynb`
- [iceberg-concepts.md](iceberg-concepts.md) — snapshots, manifests, metadata tree
- [configuration.md](configuration.md) — `pyiceberg.yaml` settings
