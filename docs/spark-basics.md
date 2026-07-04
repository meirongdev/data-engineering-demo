# Apache Spark basics

Apache Spark is a distributed computing engine for large-scale data processing.
This lab uses Spark as the ETL compute engine for the medallion pipeline,
running both from Jupyter notebooks (interactive) and via `spark-submit`
(automated).

This page explains the Spark concepts you need to understand how the pipeline
works. It is not a full Spark tutorial — just the parts this lab touches.

## How Spark runs in this lab

In a production cluster, Spark would run across many machines. In this lab, it
runs in a single Kubernetes pod with a single JVM process:

```
┌─────────────────────────────────────┐
│  spark-iceberg pod                    │
│  ┌─────────────────────────────────┐ │
│  │  Spark driver                    │ │
│  │  - runs the code (main())        │ │
│  │  - plans and schedules tasks     │ │
│  │  - 1 GB heap (spark.driver.memory)│ │
│  │  - Jupyter Lab runs in-process    │ │
│  └─────────────────────────────────┘ │
│  No separate executors — driver      │
│  runs all tasks locally (master=local)│
└─────────────────────────────────────┘
```

The `master=local[*]` mode means Spark uses all CPU cores in the pod for
parallelism. There is no separate executor fleet — the driver does everything
itself. This is sufficient for this lab's dataset (hundreds to thousands of
rows) and avoids the resource overhead of Spark on Kubernetes in the small.

## SparkSession

Everything in Spark starts with a `SparkSession` — it is the unified entry
point for reading data, running SQL, and configuring the session:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

In this lab, `getOrCreate()` picks up `spark-defaults.conf`, which configures:

- The `demo` Iceberg REST catalog (default catalog)
- `S3FileIO` for Iceberg table reads/writes
- `s3a://` filesystem for raw JSON reads
- Memory limit (`spark.driver.memory = 1g`)
- Small shuffle partitions (`spark.sql.shuffle.partitions = 4` — tuned for
  this tiny dataset; production clusters use 200+)

See [configuration.md](configuration.md) for the full settings list.

## DataFrame (the core API)

DataFrames are Spark's primary data abstraction — a distributed collection of
rows with named columns, similar to a table in SQL or a pandas DataFrame but
backed by Spark's execution engine.

```python
# From a SQL query
df = spark.sql("SELECT * FROM demo.bronze.pageviews LIMIT 10")

# From a file read
df = spark.read.json("s3a://pageviews/")

# Transform it
filtered = df.filter(col("channel") == "mobile")
```

Operations on DataFrames build up a **logical plan** (a DAG of steps) that
Spark optimises and executes only when an **action** triggers computation:

| Type | Examples | When it runs |
|---|---|---|
| **Transformations** (lazy) | `.select()`, `.filter()`, `.join()`, `.withColumn()` | Nothing happens yet — just builds the plan |
| **Actions** (eager) | `.show()`, `.count()`, `.write()`, `.collect()` | Triggers actual execution on all partitions |

This lazy evaluation lets Spark optimise the entire pipeline before touching
any data — for example, pushing filters down into the file read so it never
loads irrelevant rows.

## How the pipeline uses Spark

Each pipeline script follows the same pattern:

```python
# 1. Create a session
spark = SparkSession.builder.appName("postgres-to-bronze").getOrCreate()

# 2. Read source data (lazy — just defines a DataFrame)
raw = spark.read.format("jdbc").options(**JDBC_OPTS).option("dbtable", "users").load()

# 3. Transform (lazy — builds the plan)
cleaned = raw.select(col("id").cast("long"), col("email").cast("string"))

# 4. Write (action — triggers execution)
cleaned.write.format("iceberg").mode("overwrite").save("demo.bronze.users")

# 5. Clean up
spark.stop()
```

The pipeline scripts are submitted to the running Spark pod via `kubectl exec`:

```bash
kubectl -n lakehouse exec deploy/spark-iceberg -- \
  spark-submit /opt/pipeline/10_postgres_to_bronze.py
```

`spark-submit` launches the Python script inside the JVM, which runs the
spark-iceberg pod's existing Python environment.

## SQL + DataFrames interchangeably

Spark lets you switch between SQL and DataFrame APIs freely:

```python
# Create a view from a DataFrame
df.createOrReplaceTempView("pageviews")

# Query it with SQL
spark.sql("""
  SELECT channel, COUNT(*) AS cnt
  FROM pageviews
  GROUP BY channel
  ORDER BY cnt DESC
""").show()
```

The `%%sql` magic in notebooks does exactly this — it calls
`spark.sql(...)` and renders the result as an HTML table:

```
%%sql
SELECT channel, COUNT(*) AS cnt
FROM demo.bronze.pageviews
GROUP BY channel
ORDER BY cnt DESC
```

Both paths go through the same query planner and Iceberg integration.

## Catalog resolution

When you write `demo.bronze.pageviews`, Spark resolves it by asking the `demo`
catalog (the Iceberg REST catalog). The three-level name maps to:

```
demo      → catalog name (iceberg-rest)
bronze    → namespace / database
pageviews → table name
```

Since `demo` is the default catalog (`spark.sql.defaultCatalog = demo`), you
can write just `bronze.pageviews` in SQL or `demo.bronze.pageviews`.

## Joins and shuffles

When Spark joins two DataFrames (e.g. `purchases` + `items`), it must
co-locate matching rows. If both tables are not already partitioned on the
join key, Spark performs a **shuffle** — it repartitions data across workers,
sending rows with the same join key to the same partition.

In a single-pod `local[*]` mode, the shuffle still happens but stays within
the JVM — no network traffic. The setting `spark.sql.shuffle.partitions = 4`
controls how many partitions the shuffle creates. This is small to keep the
pipeline fast on tiny data; production clusters would set 200+.

## Spark UI

While a Spark session is active, Spark's web UI is available at
http://localhost:4040. It shows:

- **Jobs and stages** — each action becomes a job; each job is split into
  stages by shuffle boundaries
- **SQL queries** — the physical plan Spark generated for each SQL statement
- **Storage** — cached DataFrames
- **Environment** — all `spark.*` configuration values

The UI is ephemeral — it disappears when the Spark session ends.

## Key takeaways for this lab

| Concept | How you see it |
|---|---|
| `local[*]` mode | Single-pod execution, no separate executors |
| Lazy evaluation | `.show()`, `.count()`, `.write()` are the only actions that run |
| Shuffle | Happens on joins (`bronze_to_silver` joins users+items+purchases) |
| Catalog resolution | `demo.bronze.pageviews` → REST catalog → Iceberg table |
| `spark-submit` | Launches pipeline scripts inside the pod |

## See also

- [pipeline.md](pipeline.md) — the five stages and how they use Spark
- [getting-started.md](getting-started.md) — PySpark and `%%sql` in notebooks
- [configuration.md](configuration.md) — `spark-defaults.conf` settings
- [troubleshooting.md](troubleshooting.md) — Spark UI not loading, JDBC driver not found
