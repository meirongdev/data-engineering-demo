# Getting started

## Prerequisites

- [Docker](https://www.docker.com/) — installed and **running**
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- ~4 GB free RAM for Docker

All three tools and the Docker daemon are checked by the preflight in
`scripts/up.sh`; it fails fast with a clear message if something is missing.

## First run

```bash
make up
```

This single command is idempotent and does three things:

1. Creates the kind cluster (`data-eng`) from `cluster/kind-config.yaml` if it
   doesn't already exist.
2. Builds the `spark-iceberg:local` and `loadgen:local` images natively for your
   CPU arch and `kind load`s them into the cluster (no registry needed).
3. Applies the k8s manifests in dependency order and waits for each to become
   ready.

The first run takes a few minutes because it downloads Spark and the Iceberg
jars while building the image. Subsequent runs reuse Docker layer cache and the
existing cluster.

## Verify it works

```bash
make smoke
```

The smoke test runs PySpark **inside the cluster** to create an Iceberg table on
SeaweedFS, insert two rows, read them back, and assert the data files landed
under `s3://warehouse/`. A passing run ends with `SMOKE TEST PASSED`. This is the
primary regression test — run it after any change to manifests, scripts, Spark
config, or storage/catalog wiring.

```bash
make status
```

Shows nodes, pods, services, and the host URLs.

## Endpoints

| URL | What |
|---|---|
| http://localhost:8888 | Jupyter Lab (no login — token/password disabled) |
| http://localhost:4040 | Spark driver UI (live only while a notebook Spark session runs) |
| http://localhost:8181/v1/config | Iceberg REST catalog |
| http://localhost:8333 | SeaweedFS S3 API (`admin` / `password`) |
| http://localhost:9333 | SeaweedFS master UI |
| localhost:5432 | Postgres `oneshop` source (`etluser` / `etlpassword`) |

## Run the data pipeline

```bash
make pipeline
```

Seeds the Postgres + pageview sources and runs the full medallion (bronze →
silver → gold) ETL, ending with a top-items-by-revenue table. The same steps are
walked through interactively in notebooks `01`–`04`. See
[pipeline.md](pipeline.md) for the details.

## Using the lakehouse

Open **http://localhost:8888** and start with
[`notebooks/00-getting-started.ipynb`](../notebooks/00-getting-started.ipynb),
then work through `01`–`04` for the pipeline. Notebooks are persisted on a PVC,
so your edits survive pod restarts (but not `make down`).

### From Spark (PySpark)

The image ships `spark-defaults.conf`, so a Spark session in any notebook comes
pre-wired with the `demo` catalog (it is also the default catalog):

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.nyc")
spark.sql("""
  CREATE TABLE IF NOT EXISTS demo.nyc.taxis (id BIGINT, fare DOUBLE)
  USING iceberg
""")
spark.sql("INSERT INTO demo.nyc.taxis VALUES (1, 12.5), (2, 8.0)")
spark.sql("SELECT * FROM demo.nyc.taxis").show()
```

### With the `%%sql` magic

The image registers a `%sql` / `%%sql` magic backed by the active Spark session,
rendering results as tidy HTML tables:

```
%%sql
SELECT * FROM demo.nyc.taxis ORDER BY id
```

Options: `%%sql --limit 20` caps the rows returned, and `%%sql --var df` also
binds the result DataFrame to a Python variable named `df`.

### From PyIceberg (no Spark)

The image also includes a PyIceberg config at `/root/.pyiceberg.yaml` (catalog
name `default`) so you can talk to the catalog directly:

```python
from pyiceberg.catalog import load_catalog
cat = load_catalog("default")
cat.list_namespaces()
```

## Tear down

```bash
make down      # deletes the kind cluster and all its data (both PVCs)
```

For day-to-day commands and the rebuild loop, see [operations.md](operations.md).
If something goes wrong, see [troubleshooting.md](troubleshooting.md).
