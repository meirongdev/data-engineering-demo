# Apache Iceberg concepts

[Apache Iceberg](https://iceberg.apache.org/) is an open **table format** — a
specification for how a dataset's files and metadata are organised on storage.
It sits between the compute engine (Spark, Trino, Flink) and the data files
(Parquet on S3), giving tables the reliability and performance of a database
without being one.

This page explains the core Iceberg concepts you encounter in this lab. If you
already know Iceberg, you can skip to [pipeline.md](pipeline.md) for the
concrete tables.

## The problem Iceberg solves

Traditional data lakes store files in Hive-style directory trees:

```
s3://warehouse/orders/dt=2025-01-01/part-00001.parquet
s3://warehouse/orders/dt=2025-01-02/part-00002.parquet
```

This works until someone writes a file mid-directory, a `SELECT *` scans more
files than needed, or a rename fails halfway and leaves a corrupted table.
Hive tables have no ACID, no consistent snapshots, and no schema evolution.

Iceberg fixes this by adding a **metadata layer** between the table name and
the data files, inspired by database internals.

## The Iceberg metadata tree

An Iceberg table's metadata has three levels:

```
Table name (e.g. demo.bronze.pageviews)
    │
    ▼
current metadata pointer ─── stored in the catalog (REST → Postgres)
    │
    ▼
metadata.json  ─── a JSON file on object storage,
    │               one per table version (snapshot)
    ├── schema, partition spec, sort order
    ├── snapshot list
    │
    ▼
manifest list  ─── lists which manifest files are part of each snapshot
    │
    ▼
manifest files ─── index of data files + column stats (min/max values,
    │               row counts, null counts per column)
    │
    ▼
data files  ─── actual Parquet rows
```

When Spark reads `SELECT * FROM demo.bronze.pageviews`:

1. It asks the REST catalog for the **current metadata pointer**.
2. It reads the `metadata.json` from SeaweedFS to find the latest snapshot.
3. It reads the **manifest list** for that snapshot — this tells it which
   manifest files it needs.
4. Each manifest contains **column statistics** (min/max of each column per
   data file), so Iceberg can skip manifest files that contain no relevant
   rows — called **partition pruning** and **min/max filtering**.
5. It reads only the **data files** that might contain matching rows.

A direct Hive table scan reads *all* files; an Iceberg scan reads *metadata
first, then only the data files that satisfy the query's filters*.

## Snapshots and time travel

Every write to an Iceberg table produces a new **snapshot** — an immutable
point-in-time view of the table's data. The previous snapshot is preserved
(until `expire_snapshots` is run).

```sql
-- What's the current state?
SELECT * FROM demo.bronze.pageviews;

-- What did it look like at a specific time?
SELECT * FROM demo.bronze.pageviews
  FOR SYSTEM_TIME AS OF TIMESTAMP '2025-06-01 10:00:00';

-- What did it look like at snapshot #12345?
SELECT * FROM demo.bronze.pageviews
  FOR SYSTEM_VERSION AS OF 12345;
```

This is **time travel** — you can query the table as it existed at any past
snapshot, debug a pipeline by comparing snapshots, or revert to a known-good
version without restoring from backup.

**In this lab:** The pipeline uses `mode("overwrite")` for simplicity (full
table replacement). Each `make pipeline` run creates at least 5 new snapshots
per table (one per stage). Querying `FOR SYSTEM_TIME AS OF ...` would show you
the table state after bronze ingestion but before silver transforms.

## Manifest files and statistics

Each manifest file is an index of a set of data files, with rich statistics:

```
Manifest file (Parquet or Avro)
├── data file path  →  s3://warehouse/bronze/pageviews/.../00001.parquet
│   ├── row count        → 1500
│   ├── min(user_id)     → 1
│   ├── max(user_id)     → 500
│   └── null count(url)  → 0
├── data file path  →  s3://warehouse/bronze/pageviews/.../00002.parquet
│   ├── row count        → 2000
│   ├── min(user_id)     → 501
│   ├── max(user_id)     → 1000
│   └── null count(url)  → 5
└── ...
```

When you query `WHERE user_id = 42`, Iceberg reads the manifest statistics,
determines that only `00001.parquet` could contain `user_id=42`, and skips
`00002.parquet` entirely — without opening it. This is called **metadata
filtering** and is the main reason Iceberg outperforms Hive-style tables on
selective queries.

## Partitioning (conceptual)

Iceberg partitions are different from Hive partitions:

- **Hive**: directory-based (`dt=2025-01-01/`) — renaming a directory breaks
  the table; adding a partition column requires rewriting all files.
- **Iceberg**: the partition value is computed from a column and stored in the
  metadata — the files live wherever they live; you can evolve the partition
  spec without rewriting data.

Iceberg supports **hidden partitioning**: transform functions like
`days(ts)`, `months(ts)`, `bucket(id, 16)`, or `truncate(name, 10)` compute
the partition from the data. The user never manages partition directories.

> This lab's tables do not use custom partitioning — they are small enough
> that full scans are fast. In production, you would add partitioning on
> frequently-filtered columns (e.g. `days(received_at)` for pageviews).

## Schema evolution

Iceberg tables support schema changes without rewriting data files:

```sql
ALTER TABLE demo.bronze.pageviews ADD COLUMN user_agent string;
ALTER TABLE demo.bronze.pageviews RENAME COLUMN user_agent TO ua;
ALTER TABLE demo.bronze.pageviews DROP COLUMN ua;
```

Each change creates a new schema version in the metadata. Old data files (with
the old schema) are read correctly — Iceberg fills missing columns with
`null` and ignores dropped columns at read time. No `ALTER TABLE ... REBUILD`
needed.

## ACID semantics

Iceberg provides **serializable isolation** for concurrent reads and writes:

- **Atomic commits**: the catalog's `commit` operation atomically swaps the
  table's current metadata pointer from snapshot N to snapshot N+1. If two
  writers commit at the same time, one succeeds and the other retries.
- **Consistent reads**: a reader holding snapshot N sees that snapshot's data
  even if another writer commits snapshot N+1 mid-query — the reader never
  sees a partial write.
- **Rollback**: a failed write (e.g. Spark stage crashes mid-commit) leaves
  the table at the previous snapshot. Uncommitted data files become orphans
  and are cleaned up by `remove_orphan_files`.

> **In this lab:** the pipeline uses `mode("overwrite")` which replaces the
> whole table atomically — if the job crashes before committing, the old data
> is preserved.

## Table maintenance

Iceberg tables require occasional maintenance (unlike a database's autovacuum):

| Operation | What it does | Why |
|---|---|---|
| `expire_snapshots` | Deletes old metadata + data files no longer referenced by any retained snapshot | Prevents storage from growing unboundedly |
| `rewrite_data_files` | Compacts many small files into fewer larger ones | Improves read performance; `.parquet` files are most efficient at 128 MB–1 GB |
| `rewrite_manifests` | Rewrites manifest files for faster metadata scans | Degraded manifests slow down planning |
| `remove_orphan_files` | Cleans up files not referenced by any snapshot | Recovers leaked storage after crashed writes |

> **In this lab** none of these are automated — the dataset is tiny (hundreds
> of rows, a handful of files). For production, schedule these as recurring
> Spark jobs (see [production.md](production.md)).

## Where Iceberg concepts appear in this lab

| Concept | Where you encounter it |
|---|---|
| Snapshot | Every `mode("overwrite")` in `10_postgres_to_bronze.py` creates one; `FOR SYSTEM_TIME AS OF` in notebooks |
| Metadata tree | The catalog registers the metadata pointer; metadata/Parquet files live under `s3://warehouse/` |
| Manifest | Browsable on SeaweedFS at `s3://warehouse/bronze/pageviews/metadata/` (if you open the S3 UI) |
| Schema evolution | Adding columns in silver/gold without rewriting bronze |
| ACID commit | Concurrent `make smoke` + `make pipeline` would serialise at the catalog |

## See also

- [Iceberg table spec](https://iceberg.apache.org/spec/) — the canonical reference
- [pipeline.md](pipeline.md) — how this lab creates and uses Iceberg tables
- [architecture.md](architecture.md) — how the catalog, compute, and storage interact
- [production.md](production.md) — table maintenance in production
