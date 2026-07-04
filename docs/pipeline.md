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

### Gold — analytics

The gold layer has **two scripts** that build from silver:

#### Stage 4: Core product analytics (`40_silver_to_gold.py`)

`demo.gold.item_performance` — one row per item combining sales and traffic:
`items_sold`, `orders`, `revenue` (from purchases), `pageviews` (from product
pageviews), and `conversion_rate = orders / pageviews`. Anchored on
`silver.items` so every catalogue item appears, even with no sales or traffic.

#### Stage 5: Extended gold analytics (`50_gold_analytics.py`)

Five tables built on silver, split across two categories:

**Product analytics:**

| Table | What it contains |
|---|---|
| `demo.gold.top_selling_items` | Top 10 items by total revenue (`item_name`, `item_category`, `total_revenue`). A materialised answer to "which products make the most money?" — cheap enough to put on a dashboard without re-aggregating. |
| `demo.gold.sales_performance_24h` | Hourly revenue over the last 24 hours (`purchase_hour`, `total_revenue`). A snapshot of recent sales velocity. Since the loadgen scatters ~200 purchases across a 24 h window, the first `make pipeline` run populates it (see caveats below). |
| `demo.gold.pageviews_by_channel` | Pageview count per traffic channel (`channel`, `total_pageviews`). Shows which channels drive the most product-page traffic. Read from `silver.pageviews_by_items` (item-page views only, which is all this loadgen generates). |

**User analytics (RFM-style segmentation):**

> **What is RFM?** **R**ecency, **F**requency, **M**onetary — a classic marketing
> model that scores each customer on how *recently* they engaged, how *often*, and
> how *much* they spend, then buckets them so campaigns can target the valuable
> ones. This lab implements an **RF** variant: it scores on **recency** (days since
> last active) and **frequency** (total pageviews) only, and **deliberately omits
> the monetary dimension** to keep the example focused on behavioural signals from
> the pageview stream. To get true RFM, join a per-user `sum(total_price)` from
> `silver.purchases_enriched` and add it as a third scoring axis.

| Table | What it contains |
|---|---|
| `demo.gold.user_engagement_segments` | Per-user engagement scoring: rows with `user_id`, `email`, `full_name`, `total_pageviews`, `active_days`, `last_active_date`, `days_since_last_active`, and an `engagement_segment` label. The segment is computed from pageview frequency (how many total pageviews) plus recency (how many days since the last active day): |

The segmentation logic:

```
total_pageviews ≥ 8  AND  days_since_last_active ≤ 3  →  high_engagement
total_pageviews ≥ 3  AND  days_since_last_active ≤ 7  →  medium_engagement
otherwise                                               →  low_engagement
```

Only users with `valid_email = TRUE` are included (~900 out of 1,000 seeded
users, since the loadgen sets ~10% of emails to `NULL`). The thresholds are
calibrated to this repo's loadgen (1,000 users, 200 purchases × 30
pageviews/purchase = ~6,000 pageviews); if you change `PAGEVIEWS_PER_PURCHASE`,
re-tune them.

**Data product delivery — CSV export to SeaweedFS:**

The same `50_gold_analytics.py` script writes the user engagement segments to
`coalesce(1).write.csv(...)` at `s3a://customer-segments/segmented_users/` as a
single CSV file for a downstream team (comma-separated, `header=true`). This
demonstrates the **data-product delivery** pattern: a gold table exported as a
single-part CSV onto object storage so a separate process (marketing tool, email
platform) can pick it up without touching the lakehouse.

Spark's CSV output produces a file named `part-00000-<uuid>.csv` (plus a
`_SUCCESS` marker) — that is normal Spark behaviour; consumers glob on `*.csv`.

### Full gold table reference

| Table | Script | Purpose |
|---|---|---|
| `demo.gold.item_performance` | `40_silver_to_gold.py` | Per-item revenue, orders, pageviews, conversion rate |
| `demo.gold.top_selling_items` | `50_gold_analytics.py` | Top 10 items by revenue |
| `demo.gold.sales_performance_24h` | `50_gold_analytics.py` | Hourly revenue, last 24 hours |
| `demo.gold.pageviews_by_channel` | `50_gold_analytics.py` | Pageview counts per channel |
| `demo.gold.user_engagement_segments` | `50_gold_analytics.py` | RFM-style user engagement segments |

## Running it

```bash
make pipeline      # loadgen Job -> 00 create -> 10/20 bronze -> 30 silver -> 40 gold -> 50 gold (extended)
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

### Automated daily refresh (optional)

A **CronJob** (`k8s/90-pipeline-cron.yaml`) runs the full stage sequence every
day at 06:00 UTC — bronze → silver → gold — so gold tables reflect the most
recent source data. Deploy it with:

```bash
kubectl apply -f k8s/90-pipeline-cron.yaml
```

**Important caveat:** This re-runs bronze→silver→gold but **not** the loadgen,
so no *new* source rows arrive between runs. The `sales_performance_24h` table
will trend empty as purchases age past the 24-hour window. To keep it populated
you would also schedule the loadgen Job — which is out of scope for this lab.
The CronJob is primarily useful as a demonstration of the orchestration pattern:
K8s-native scheduling instead of adding Airflow (~2 GB).

## Exploring the results

From a notebook (`%%sql`) or `make shell` → `spark-sql`:

```sql
-- Top earners
SELECT item_name, item_category, orders, revenue, pageviews,
       round(conversion_rate, 4) AS conversion_rate
FROM demo.gold.item_performance
ORDER BY revenue DESC
LIMIT 10;

-- Which items get traffic but don't convert?
SELECT item_name, item_category, pageviews, orders, conversion_rate
FROM demo.gold.item_performance
WHERE pageviews > 0
ORDER BY conversion_rate ASC
LIMIT 10;

-- Revenue by category
SELECT item_category, sum(revenue) revenue, sum(orders) orders
FROM demo.gold.item_performance
GROUP BY item_category ORDER BY revenue DESC;

-- Traffic by channel
SELECT channel, total_pageviews FROM demo.gold.pageviews_by_channel
ORDER BY total_pageviews DESC;

-- Engagement segment breakdown
SELECT engagement_segment, count(*) AS user_count
FROM demo.gold.user_engagement_segments GROUP BY engagement_segment;

-- Data-quality signal that silver surfaces
SELECT valid_email, count(*) FROM demo.gold.user_engagement_segments GROUP BY valid_email;
```

The user engagement data is also exported as a single CSV file to SeaweedFS:

```bash
# Check it from the Spark pod:
kubectl exec <spark-pod> -- mc ls sw/customer-segments/segmented_users/
# Or from any pod with mc and access:
kubectl run mc-check --image=minio/mc --restart=Never --command -- /bin/sh -c \
  'mc alias set sw http://seaweedfs:8333 admin password 2>/dev/null; mc cat sw/customer-segments/segmented_users/part-*.csv' | head -5
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

## Design caveats

- **`sales_performance_24h` is a static snapshot**, not a streaming window. The
  loadgen scatters ~200 purchases over ~24 h, so the first `make pipeline` run
  populates it. A subsequent run without new source data will see fewer rows as
  purchases age past the 24 h cutoff.
- **`purchase_hour` is computed as `hour(created_at)` (0–23)**, so a 24 h window
  straddling midnight merges two different clock hours into one bucket. Use
  `date_trunc('hour', created_at)` for exact per-hour buckets if needed.
- **RFM segment distribution depends on the loadgen parameters.** The default
  thresholds (8 pageviews / 3 days for high; 3 pageviews / 7 days for medium)
  are calibrated to 1,000 users × 200 purchases × 30 pageviews/purchase. If you
  raise `PAGEVIEWS_PER_PURCHASE`, re-tune the thresholds.
- **`pageviews_by_channel` reads `silver.pageviews_by_items`**, which only
  contains `/products/{id}` pages. In this loadgen every pageview matches, so it
  equals total traffic. For a true all-traffic channel count, read
  `demo.bronze.pageviews` instead.
- **CSV file naming.** Spark outputs `part-00000-<uuid>.csv` (plus a `_SUCCESS`
  marker). Consumers should glob on `*.csv` or use the directory path.
- **Null email handling.** Users with `NULL` email (seeded at ~10% rate) are
  excluded from the engagement segment table. This is intentional — users
  without a valid email cannot be scored.
