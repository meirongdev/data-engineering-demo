# The medallion data pipeline

This lab layers a classic **medallion (bronze → silver → gold)** ETL on top of
the Iceberg lakehouse. New to the pattern? [medallion.md](medallion.md)
introduces what each layer is for and the principles behind it; this page is the
concrete implementation. It's a kind-native take on a classic Docker Compose
lakehouse tutorial, adapted to this stack:

| Typical Docker Compose tutorial | This lab (kind) |
|---|---|
| MinIO for object storage | **SeaweedFS** (S3 API) |
| Postgres source (`oneshop`) | same, as `k8s/50-postgres.yaml` |
| `loadgen` container | **loadgen Job** (`k8s/60-loadgen.yaml`), one-shot + idempotent |
| `spark/scripts/*.py` run by hand | `docker/spark/pipeline/*.py` run by `make pipeline` |
| default `spark_catalog` | the `demo` **REST** catalog (default catalog) |
| `s3a://` reads on MinIO | `s3a://` reads on SeaweedFS (hadoop-aws jars baked in) |

## Sources

Two very different inputs, mirroring a real ingestion job:

- **Postgres `oneshop`** — the operational store, with `users`, `items` and
  `purchases`. Read over **JDBC**.
- **`pageviews` bucket on SeaweedFS** — raw clickstream events as
  newline-delimited JSON (`{"user_id", "url": "/products/{id}", "channel",
  "received_at"}`). Read over **`s3a://`**.

Both are populated by the **loadgen** Job, which TRUNCATEs + reseeds Postgres and
clears + rewrites the bucket, so every run starts from a clean, reproducible
dataset. Volumes are tunable via env on the Job (`USER_COUNT`, `ITEM_COUNT`,
`PURCHASE_COUNT`, `PAGEVIEWS_PER_PURCHASE`).

## The layers

Everything lands in the `demo` catalog on SeaweedFS (`s3://warehouse/`).

### Bronze — raw copies

| Table | Source | Stage script |
|---|---|---|
| `demo.bronze.users` | Postgres `users` | `10_postgres_to_bronze.py` |
| `demo.bronze.items` | Postgres `items` | `10_postgres_to_bronze.py` |
| `demo.bronze.purchases` | Postgres `purchases` | `10_postgres_to_bronze.py` |
| `demo.bronze.pageviews` | `s3a://pageviews/` JSON | `20_s3_to_bronze.py` |

Type-cast to the target schema, full-`overwrite` for a clean reload. No business
logic — bronze is the faithful landing zone.

### Silver — validated + enriched (`30_bronze_to_silver.py`)

| Table | What the transform does |
|---|---|
| `demo.silver.users` | add `valid_email` (regex check) and `full_name` |
| `demo.silver.items` | clamp negative `price` to 0, upper-case `category` |
| `demo.silver.purchases_enriched` | join users + items; derive `total_price` (= `quantity × purchase_price`), `user_email`, `item_name`, `item_category`, `purchase_date`, `purchase_hour` |
| `demo.silver.pageviews_by_items` | parse `/products/{id}` URLs into `page` + `item_id`, join items for `item_name`/`item_category` |

### Gold — analytics (`40_silver_to_gold.py`)

`demo.gold.item_performance` — one row per item combining sales and traffic:
`items_sold`, `orders`, `revenue` (from purchases), `pageviews` (from product
pageviews), and `conversion_rate = orders / pageviews`. Anchored on
`silver.items` so every catalogued item appears, even with no sales or traffic.

## Running it

```bash
make pipeline      # loadgen Job -> 00 create -> 10/20 bronze -> 30 silver -> 40 gold
```

`scripts/pipeline.sh` (re)applies the loadgen Job, waits for it, then
`spark-submit`s each stage in `/opt/pipeline/` inside the Spark pod, in order.

Re-run just the seeding without the Spark stages:

```bash
make loadgen
```

> The pipeline scripts are **baked into the Spark image**. After editing anything
> under `docker/spark/pipeline/`, run `make build` (it reloads the image and
> restarts the pod) before `make pipeline` so the pod runs the new code.

### Or interactively, in notebooks

Open http://localhost:8888 and run, in order:

1. `01-create-tables.ipynb` — create the bronze/silver/gold tables
2. `02-ingest-bronze.ipynb` — JDBC + `s3a://` ingestion
3. `03-bronze-to-silver.ipynb` — the transforms, with sample output at each step
4. `04-gold-analytics.ipynb` — build and query `item_performance`

(Run `make loadgen` first so there's data to ingest.)

## Exploring the results

From a notebook (`%%sql`) or `make shell` → `spark-sql`:

```sql
-- Top earners
SELECT item_name, item_category, orders, revenue, pageviews,
       round(conversion_rate, 4) AS conversion_rate
FROM demo.gold.item_performance
ORDER BY revenue DESC
LIMIT 10;

-- Data-quality signal that silver surfaces
SELECT valid_email, count(*) FROM demo.silver.users GROUP BY valid_email;

-- Revenue by category
SELECT item_category, sum(revenue) revenue, sum(orders) orders
FROM demo.gold.item_performance
GROUP BY item_category ORDER BY revenue DESC;
```

## How it wires into the stack

- **JDBC**: the Postgres driver (`postgresql-*.jar`) is baked into the Spark
  image; the pipeline connects as `etluser` to `jdbc:postgresql://postgres:5432/oneshop`.
- **`s3a://`**: `hadoop-aws` + the AWS SDK v1 bundle are baked in and pointed at
  SeaweedFS via `fs.s3a.*` in `spark-defaults.conf`. This is separate from
  Iceberg's own `S3FileIO` (AWS SDK v2) used for table data — they coexist.
- **Catalog**: `demo` is the default REST catalog, so `demo.bronze.users` etc.
  resolve straight to Iceberg tables backed by `s3://warehouse/`.

See [architecture.md](architecture.md) for the component-level view.
