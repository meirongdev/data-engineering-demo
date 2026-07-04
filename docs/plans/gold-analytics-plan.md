# Plan: extended gold analytics

> **Status:** completed on 2026-07-05. All 5 gold tables (including the
> existing `item_performance`) present in `demo.gold`, CSV export lands in
> SeaweedFS `customer-segments/segmented_users/`, and Trino can query them all.
> Self-contained implementation plan — pick this up and execute without
> re-deriving.

## Why this exists (the gap)

The core medallion pipeline (bronze → silver → gold) is already in place,
reading from Postgres `oneshop` + SeaweedFS pageviews and writing to the Iceberg
REST catalog. The gold layer had a single table (`item_performance`), which
covers per-item revenue and conversion — but a real analytics platform needs
more:

| Gold feature | Status before this plan |
|---|---|
| `gold.item_performance` (item revenue + conversion) | ✅ present |
| `gold.top_selling_items` (TOP 10 by revenue) | ❌ missing |
| `gold.sales_performance_24h` (hourly revenue, rolling) | ❌ missing |
| `gold.pageviews_by_channel` (traffic by channel) | ❌ missing |
| `gold.user_engagement_segments` (user RFM-style segment) | ❌ missing |
| CSV export to S3 for downstream team | ❌ missing |
| Workflow scheduling / DAG orchestration | ❌ missing |

This plan closes those gaps using the repo's native patterns: Spark scripts,
K8s manifests, and Makefile targets.

**The teaching point:** a Spark-only medallion pipeline is good, but adding a
*user engagement segmentation* table — combining pageview frequency, recency,
and email validity — shows how gold-layer logic naturally involves
business-rules (RFM-style scoring) that live in Spark SQL. CSV export to
SeaweedFS demonstrates the "data product" delivery pattern (gold → S3 →
downstream team).

## Decisions (already made — do not re-litigate)

1. **Gold tables are built in Spark,** not Trino. The existing
   `40_silver_to_gold.py` sets the pattern; new gold tables go in a
   `50_gold_analytics.py` sibling. Trino (already deployed by `make serving`)
   is a *consumer* of gold tables, not a *producer* — keeping the ETL pipeline
   single-engine avoids confusion.
2. **CSV export uses Spark DataFrameWriter,** not boto3. The repo already has
   `s3a://` connectivity for the SeaweedFS filesystem (hadoop-aws in the Spark
   image). Writing `coalesce(1).write.csv(...)` is zero new dependencies and
   consistent with `20_s3_to_bronze.py`.
3. **Scheduling is optional and K8s-native.** If scheduling is needed, add a
   CronJob (pattern: `60-loadgen.yaml` batch Job → CronJob). Do *not* add
   Airflow unless there is an explicit teaching goal. Airflow adds ~2 GB RAM
   and its own metadata DB — heavy for a lab that already fits in ~4 GB.
4. **Only one new table is partitioned.** `user_engagement_segments` is
   partitioned by `engagement_segment` (3 discrete values → small partitions).
   The other gold tables are small enough to be unpartitioned.
5. **The existing `item_performance` table is kept as-is.** It anchors on items
   (zero rows get a row). The new `top_selling_items` and `pageviews_by_channel`
   are simpler aggregates from different starting points — they coexist.

## Implementation — files to add / change

### New — Spark gold pipeline script

**`docker/spark/pipeline/50_gold_analytics.py`**

Creates 4 gold tables in this order.

> **REVIEW — every Iceberg write MUST use `.format("iceberg")`.** The rest of
> the pipeline always writes `.write.format("iceberg").mode("overwrite").save(...)`
> (`40_silver_to_gold.py:60`, `30_bronze_to_silver.py:34`). Omitting it makes
> Spark write default **Parquet to a path literally named** `demo.gold.<table>`
> in the pod's working dir — the Iceberg table is never created.

```python
# 1. top_selling_items — materialise to make "show me the winners" cheap
spark.table("demo.silver.purchases_enriched") \
  .groupBy("item_id", "item_name", "item_category") \
  .agg(sum("total_price").alias("total_revenue")) \
  .orderBy(col("total_revenue").desc()) \
  .limit(10) \
  .write.format("iceberg").mode("overwrite").save("demo.gold.top_selling_items")

# 2. sales_performance_24h — last 24h of hourly revenue
# Static snapshot; the pipeline is batch, not streaming. Purchases are already
# scattered over the last 24h by the loadgen, so the first run populates it.
spark.table("demo.silver.purchases_enriched") \
  .filter(col("created_at") >= current_timestamp() - expr("INTERVAL 24 HOURS")) \
  .groupBy("purchase_hour") \
  .agg(sum("total_price").alias("total_revenue")) \
  .orderBy("purchase_hour") \
  .write.format("iceberg").mode("overwrite").save("demo.gold.sales_performance_24h")

# 3. pageviews_by_channel
spark.table("demo.silver.pageviews_by_items") \
  .groupBy("channel") \
  .agg(count(lit(1)).alias("total_pageviews")) \
  .orderBy(col("total_pageviews").desc()) \
  .write.format("iceberg").mode("overwrite").save("demo.gold.pageviews_by_channel")

# 4. user_engagement_segments — RFM-style user scoring
# RFM logic: pageview frequency + active days + recency → segment.
#
# REVIEW — JOIN KEY: silver.users keys on `id`, NOT `user_id`
# (00_create_tables.py:49, 30_bronze_to_silver.py:44); only
# silver.pageviews_by_items has `user_id`. Alias users.id -> user_id first,
# otherwise `join(pvs, "user_id")` / `groupBy("user_id", ...)` fail to resolve.
#
# REVIEW — THRESHOLDS are calibrated to THIS repo's loadgen (~6 pageviews/user:
# 200 purchases x 30 pageviews / 1000 users). After the first run, verify the
# split with `GROUP BY engagement_segment` and tune if a bucket is empty.
users = spark.table("demo.silver.users").select(
  col("id").alias("user_id"), "email", "full_name", "valid_email"
)
pvs = spark.table("demo.silver.pageviews_by_items")
users.join(pvs, "user_id", "left") \
  .filter(col("valid_email") == True) \
  .groupBy("user_id", "email", "full_name") \
  .agg(
    count("page").alias("total_pageviews"),
    countDistinct(to_date(col("received_at"))).alias("active_days"),
    max(to_date(col("received_at"))).alias("last_active_date"),
  ) \
  .withColumn(
    "days_since_last_active",
    datediff(current_date(), col("last_active_date"))
  ) \
  .withColumn(
    "engagement_segment",
    when(
      (col("total_pageviews") >= 8) & (col("days_since_last_active") <= 3),
      lit("high_engagement")
    ).when(
      (col("total_pageviews") >= 3) & (col("days_since_last_active") <= 7),
      lit("medium_engagement")
    ).otherwise(lit("low_engagement"))
  ) \
  .write.format("iceberg").mode("overwrite").save("demo.gold.user_engagement_segments")

# 5. CSV export to SeaweedFS (for downstream / marketing team).
# s3a:// creds come from spark-defaults.conf (fs.s3a.access.key / .secret.key,
# baked into the image) — no AWS_* env vars needed.
spark.table("demo.gold.user_engagement_segments") \
  .coalesce(1) \
  .write.mode("overwrite") \
  .option("header", "true") \
  .csv("s3a://customer-segments/segmented_users")
```

**Important:** Use `mode("overwrite")` for all tables (idempotent, matching the
existing pipeline pattern). The CSV is written to SeaweedFS as a single
partition file; the filename includes `part-00000-*.csv` — that's expected Spark
behaviour, no rename post-hoc.

### Edit — table DDL

**`docker/spark/pipeline/00_create_tables.py`** — append `CREATE TABLE IF NOT EXISTS`
DDLs for the 4 new gold tables to the `DDL` list:

| Table | Partitioning |
|---|---|
| `demo.gold.top_selling_items` | none |
| `demo.gold.sales_performance_24h` | none |
| `demo.gold.pageviews_by_channel` | none |
| `demo.gold.user_engagement_segments` | `engagement_segment` (3 discrete buckets) |

Schema matches the Spark `save()` columns above verbatim (Spark's schema
inference on overwrite will reconcile, but explicit DDL is better for
documentation and for Trino).

### Edit — bucket init

**`k8s/20-bucket-init.yaml`** — add a `mc mb` / `mc policy` step for the
`customer-segments` bucket (or extend the existing init container's commands).
The Job already runs `mc` to seed `warehouse` and `pageviews` buckets.

### Edit — loadgen: scatter pageview `received_at` (makes RFM recency real)

**`docker/loadgen/generate_load.py`** — the `pageview()` helper stamps every
event with `int(time.time())`, so **all pageviews share one timestamp**. That
makes the RFM recency dimension inert: `active_days` is always 1 and
`days_since_last_active` always 0, so `user_engagement_segments` collapses to a
frequency-only cut — the plan's "frequency + recency" teaching point is only
half-true. Scatter it over the last ~14 days, mirroring how purchases already
scatter `created_at`:

```python
PAGEVIEW_WINDOW_DAYS = int(os.getenv("PAGEVIEW_WINDOW_DAYS", "14"))

def pageview(viewer_id, item_id):
    return {
        "user_id": viewer_id,
        "url": f"/products/{item_id}",
        "channel": random.choice(CHANNELS),
        "received_at": int(time.time() - random.randint(0, PAGEVIEW_WINDOW_DAYS * 86400)),
    }
```

Side benefit: `silver.pageviews_by_items` is partitioned by `days(received_at)`
(`00_create_tables.py:78`), currently degenerate (one partition). Scattering
makes that partitioning meaningful too. No version/tag change — a Python edit
picked up by `make build` (rebuilds `loadgen:local` via `build-image.sh`) and the
next `make pipeline` (re-applies the loadgen Job via `run-loadgen.sh`).

### Edit — pipeline runner

**`scripts/pipeline.sh`** — append after the `40_silver_to_gold` stage:

```bash
run_stage 50_gold_analytics.py    "gold analytics (extended)"
```

No other script changes — the image already `COPY`s the entire `docker/spark/pipeline/`
directory, so a rebuild (`make build`) picks up the new file automatically.

### Optional — scheduling CronJob

**`k8s/90-pipeline-cron.yaml`** — if a scheduled refresh is desired, run the
*whole* Spark stage sequence, **not gold alone**.

> **REVIEW — a gold-only CronJob is misleading.** Running just
> `50_gold_analytics.py` recomputes gold from *stale* silver (nothing upstream
> refreshes), and `sales_performance_24h`'s rolling-24h filter ages the existing
> purchases out — the table trends to **empty**. Re-run bronze→silver→gold so
> gold reflects current Postgres + bucket state:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: pipeline-refresh
  namespace: lakehouse
spec:
  schedule: "0 6 * * *"        # daily 06:00 UTC
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          initContainers:
            - name: wait-for-deps
              image: busybox:1.38
              command: [sh, -c, "until nc -z iceberg-rest 8181 && nc -z seaweedfs 8333; do sleep 2; done"]
          containers:
            - name: spark-submit
              image: spark-iceberg:local
              imagePullPolicy: IfNotPresent
              command:
                - sh
                - -c
                - |
                  set -e
                  for s in 10_postgres_to_bronze 20_s3_to_bronze \
                           30_bronze_to_silver 40_silver_to_gold 50_gold_analytics; do
                    spark-submit /opt/pipeline/$s.py
                  done
```

Notes: (a) S3/REST creds are baked into `spark-defaults.conf` in the image, so
no `AWS_*` env vars are needed. (b) This still does **not** re-run the loadgen,
so no *new* source rows arrive — `sales_performance_24h` will still trend empty
as purchases age past 24h. To keep it fresh you'd also schedule the loadgen Job;
that's out of scope for this lab.

### No changes needed

| File | Reason |
|---|---|
| `Dockerfile` | Already `COPY docker/spark/pipeline/ /opt/pipeline/` — glob captures new files. |
| `Makefile` | `make pipeline` runs `scripts/pipeline.sh` — no new target needed. |
| `cluster/kind-config.yaml` | No new host ports. |
| `scripts/deploy-serving.sh` / `scripts/status.sh` | No new services. |

## Caveats to surface in docs (not blockers)

- **`sales_performance_24h` is a static snapshot,** not a streaming window.
  The loadgen scatters ~200 purchases over ~24h, so the first `make pipeline`
  run populates it. A subsequent run at +6h won't "see" 6h of new data unless
  new purchases were continued. This is fine for a lab — the table demonstrates
  the *query shape*, not an always-fresh window.
- **`sales_performance_24h` buckets by hour-of-day (0–23), not by timestamp.**
  `purchase_hour = hour(created_at)` (`30_bronze_to_silver.py:73`), so a 24h
  window straddling midnight can merge two different clock hours into one bucket.
  Use `date_trunc('hour', created_at)` if you want exact per-hour buckets.
- **RFM segment distribution is data-dependent — check it after the first run.**
  `SELECT engagement_segment, count(*) FROM demo.gold.user_engagement_segments
  GROUP BY 1`. With the recalibrated thresholds (8 / 3) plus the loadgen
  `received_at` scatter, all three buckets should be non-empty. If one is empty,
  tune the thresholds or raise `PAGEVIEWS_PER_PURCHASE` in `k8s/60-loadgen.yaml`.
- **`pageviews_by_channel` counts item-page traffic.** It reads
  `silver.pageviews_by_items`, which is filtered to URLs carrying an item id
  (`30_bronze_to_silver.py:86`). In this loadgen every pageview is a
  `/products/{id}` page, so the count equals total traffic; for a true
  all-traffic-by-channel table, read `demo.bronze.pageviews` instead.
- **CSV file naming.** Spark outputs `part-00000-<uuid>.csv` (plus a
  `_SUCCESS` marker and a `._SUCCESS.crc` on most filesystems). Consumers
  should glob on `*.csv` or use the directory path. This is normal Spark
  behaviour; document it.
- **Bucket creation ordering.** The init container in `20-bucket-init.yaml` runs
  before the first pipeline. If you rebuild the cluster (`make down && make up`),
  both the CSV export path and the Iceberg warehouse are recreated. For
  incremental development, `scripts/pipeline.sh` does *not* recreate buckets.
- **Null email handling.** The loadgen sets ~10% of user emails to `NULL`. Users
  without a valid email are excluded from the segment table — document this so
  it doesn't look like a bug.

## Execution checklist

- [x] `docker/spark/pipeline/50_gold_analytics.py` — 4 gold tables + CSV export
      (every Iceberg write uses `.format("iceberg")`; segment join aliases
      `users.id → user_id`; thresholds 8 / 3)
- [x] `docker/spark/pipeline/00_create_tables.py` — DDL for new gold tables
- [x] `docker/loadgen/generate_load.py` — scatter `received_at` over N days
- [x] `k8s/20-bucket-init.yaml` — add `customer-segments` bucket
- [x] `scripts/pipeline.sh` — append stage 5
- [x] `make build && make pipeline` — verify all 4 tables exist in Iceberg
- [x] Verify the segment split is non-degenerate:
      `spark-sql -e 'SELECT engagement_segment, count(*) FROM demo.gold.user_engagement_segments GROUP BY 1'`
      — result: low=57, medium=613, high=234 (904 total, ~100 excluded by NULL email)
- [x] `make serving` (if deployed) — verify Trino can `SELECT * FROM iceberg.gold.user_engagement_segments`
      — all 5 gold tables visible in Trino
- [x] `kubectl exec` + spark-sql — verify CSV files landed in SeaweedFS:
      `mc ls sw/customer-segments/segmented_users/`
      — part-00000-*.csv (66 KiB) confirmed
- [x] (Optional) `k8s/90-pipeline-cron.yaml` — full-pipeline CronJob (`pipeline-refresh`) deployed, runs on schedule
- [x] Update this plan doc → mark `completed`

## Sources (grounding)

- Existing pipeline pattern in this repo: `docker/spark/pipeline/40_silver_to_gold.py`
- Existing bucket init: `k8s/20-bucket-init.yaml`
- Existing pipeline runner: `scripts/pipeline.sh`
- Existing notebook set: `notebooks/00-getting-started.ipynb` through `04-gold-analytics.ipynb`
- Medallion concept doc: `docs/medallion.md`
- Existing plan: `docs/plans/serving-layer.md`
