# Project overview

This repo is a hands-on **Apache Iceberg lakehouse** lab that runs end to end on
a local [kind](https://kind.sigs.k8s.io/) Kubernetes cluster. It started as a
"can we create and read an Iceberg table on object storage?" demo and now carries
a full **medallion (bronze → silver → gold) data pipeline** over a fictional
e-commerce dataset.

This page introduces the project from two angles — **business** (what problem it
models and what questions it answers) and **technical architecture** (how it's
built). For step-by-step detail, follow the links at the bottom.

---

## Business view

### The "oneshop" scenario

`oneshop` is a fictional online store. Like any real e-commerce business, its data
arrives from two very different places:

- **An operational database (OLTP)** — the source of truth for *what happened*:
  who the customers are (`users`), what's for sale (`items`), and what was bought
  (`purchases`). This is a transactional SQL database.
- **A behavioral event stream** — the source of truth for *what people looked
  at*: every product page view (`pageviews`), captured as raw JSON files dropped
  into object storage.

Those two sources have fundamentally different shapes — a structured SQL database
vs. a firehose of semi-structured files — which is exactly the everyday
ingestion challenge a data platform has to solve.

### The questions we want to answer

The pipeline exists to turn that raw data into decisions:

- **Which products actually make money?** — revenue, orders, and units sold per
  item (`gold.item_performance`), and the top earners at a glance
  (`gold.top_selling_items`).
- **Which products get attention but don't sell?** — the **conversion rate**
  (`orders ÷ pageviews`). High traffic + low conversion is a pricing or
  merchandising problem worth flagging.
- **How healthy is our customer data?** — e.g. what share of customers have a
  missing or invalid email (a reachability signal for marketing).
- **How does revenue break down by category, and when do people buy?** —
  category analysis from `gold.item_performance`, and hourly revenue trends
  from `gold.sales_performance_24h`.
- **Which traffic channels drive the most engagement?** — `gold.pageviews_by_channel`
  shows where your audience comes from.
- **Who are your most valuable users?** — `gold.user_engagement_segments` scores
  every customer by pageview frequency and recency, labelling them
  `high_engagement`, `medium_engagement`, or `low_engagement`. This is the
  kind of RFM-style segmentation (**R**ecency, **F**requency, **M**onetary) a
  marketing team uses to target campaigns — here scored on recency + frequency
  only, with the monetary axis left out (see [pipeline.md](pipeline.md)).
- **How do you deliver data to a downstream team?** — the engagement segments
  are also exported as a CSV file to SeaweedFS (`s3a://customer-segments/segmented_users/`),
  ready for a non-lakehouse consumer (email platform, CRM tool, spreadsheet).**

### Why bronze → silver → gold

Rather than one giant query, the data is refined in layers, each with a clear
job. That separation is what makes an analytics platform maintainable: every
layer is independently testable, reprocessable, and reusable. For the pattern in
general — what each layer is for and the principles behind it — see
[medallion.md](medallion.md).

| Layer | Business meaning | Example output |
|---|---|---|
| **Bronze** | *Land the raw data faithfully.* A trustworthy copy of each source, no business logic — so anything downstream can always be rebuilt from here. | `bronze.purchases`, `bronze.pageviews` |
| **Silver** | *Clean it and connect it.* Validate emails, normalise categories, clamp bad prices, and join raw facts into meaningful entities (a purchase enriched with who bought what; a pageview tied to a real product). The layer analysts actually work on. | `silver.purchases_enriched`, `silver.users` (with `valid_email`) |
| **Gold** | *Answer the question directly.* Aggregated, decision-ready tables. | `gold.item_performance`, `gold.top_selling_items`, `gold.sales_performance_24h`, `gold.pageviews_by_channel`, `gold.user_engagement_segments` |

### What the pipeline update added

Before, the lab could only show that the lakehouse *plumbing* worked (create and
read a toy table). Now it demonstrates a realistic **end-to-end analytics
capability**: from raw operational + behavioral e-commerce data all the way to
five gold tables a business could act on — product analytics (revenue,
conversion, top sellers, hourly performance), user analytics (engagement
segmentation), and channel analysis — plus a CSV export to object storage for
downstream teams. All of it is reproducible with one command (`make pipeline`)
or interactively in notebooks `01`–`04`.

The gold layer now covers two analytic domains:

- **Product analytics** — which items sell, which get attention but don't
  convert, when revenue peaks, and which channels drive traffic.
- **User analytics** — who your most engaged users are, via RFM-style scoring
  combining pageview frequency, active days, and recency.

The CSV export to `s3a://customer-segments/segmented_users/` shows the
**data product delivery** pattern: gold tables aren't only consumed inside the
lakehouse (via Trino or notebooks); they can be pushed out as flat files for
external tools that don't speak Iceberg (marketing platforms, CRM, spreadsheets).

An optional [CronJob](../k8s/90-pipeline-cron.yaml) can schedule the full
pipeline refresh daily at 06:00 UTC — using K8s-native scheduling rather than
adding Airflow.

---

## Technical architecture view

### The lakehouse platform

Four components make up the platform; all run in the `lakehouse` namespace on a
kind cluster.

| Role | Component | Why this one |
|---|---|---|
| Object storage (data files) | **SeaweedFS** (S3 API) | Apache 2.0, lightweight, S3-compatible |
| Table catalog (metadata) | **Iceberg REST catalog** (`iceberg-rest-fixture`) | the reference REST catalog; catalog name `demo` |
| Compute | **Spark 3.5 + Iceberg 1.10** | reads/writes Iceberg tables and runs the ETL |
| Notebooks | **Jupyter Lab** | interactive PySpark / `%%sql` front-end |

The defining lakehouse property: Spark asks the **catalog** for a table's
metadata pointer, then reads/writes the actual Parquet + Iceberg files **directly
on SeaweedFS** — the catalog is never in the data path.

### The pipeline on top

Two more workloads feed and drive the pipeline:

- **Postgres** (`50-postgres.yaml`) — the `oneshop` OLTP source, deployed with
  the stack.
- **loadgen** (`60-loadgen.yaml`) — a one-shot, idempotent Job that seeds
  Postgres and drops pageview JSON into the `pageviews` bucket, then exits.

The ETL itself is five `spark-submit` stages (`docker/spark/pipeline/`): create
tables → Postgres-to-bronze → pageviews-to-bronze → bronze-to-silver →
silver-to-gold.

### Two ingestion paths (the interesting bit)

Bronze is fed by two different readers, because the two sources are different:

- **JDBC path** — Spark reads the Postgres tables via the Postgres JDBC driver.
  Structured, straight from a live database.
- **`s3a://` path** — Spark reads the raw pageview JSON off SeaweedFS via the
  Hadoop `s3a` filesystem. Schema is inferred from semi-structured files.

A subtlety worth knowing: **two independent S3 mechanisms coexist** in the same
image — Iceberg's own `S3FileIO` (AWS SDK **v2**, for Iceberg table data) and
Hadoop's `s3a` (AWS SDK **v1**, for the raw-JSON read). They're configured
separately (`demo.s3.*` vs `fs.s3a.*`) and don't conflict.

### End-to-end data flow

```
loadgen ──► Postgres (users/items/purchases) ──JDBC──┐
        └─► SeaweedFS pageviews bucket (JSON) ──s3a──┤
                                                     ▼
                                    Spark ── bronze ─► silver ─► gold
                                                     │
                                 all table files ────┘  s3://warehouse/ (SeaweedFS)
                                                        catalog holds the pointers
```

### Key design choices & trade-offs

- **kind, not Docker Compose** — learn Kubernetes and data infra together; the
  pipeline runs as Jobs + `spark-submit`, mirroring how you'd orchestrate for
  real.
- **SeaweedFS over MinIO** — Apache 2.0 licensed, lighter footprint.
- **Iceberg REST fixture** — the reference catalog; note its registry is
  **in-memory** (demo-only), while the data files are durable on SeaweedFS.
- **One-shot idempotent loadgen** — TRUNCATEs + reseeds, so every run is a clean,
  reproducible dataset (tunable via env: `USER_COUNT`, `PURCHASE_COUNT`, …).
- **Notebooks *and* `make pipeline`** — the same logic, two entry points:
  interactive teaching vs. automated end-to-end run.
- **Adapted from a Docker Compose tutorial** — MinIO → SeaweedFS, Postgres added
  as a k8s component, `hadoop-aws` jars added for `s3a`, and the default catalog
  is the `demo` REST catalog.

---

## Where to go next

- **Core concepts you hit along the way** → [iceberg-concepts.md](iceberg-concepts.md) (snapshots, manifests, time travel), [spark-basics.md](spark-basics.md) (DataFrame, lazy eval, spark-submit), [kubernetes-resources.md](kubernetes-resources.md) (Deployments, PVCs, Jobs, Services), [docker-images.md](docker-images.md) (why four custom images), [pyiceberg-usage.md](pyiceberg-usage.md) (no-Spark catalog access), [s3-access-paths.md](s3-access-paths.md) (S3FileIO vs. hadoop s3a)

- **Run it** → [getting-started.md](getting-started.md)
- **The medallion pattern itself** → [medallion.md](medallion.md)
- **Pipeline, stage by stage** → [pipeline.md](pipeline.md)
- **Component-level architecture reference** (ports, PVCs, probes, data flow) →
  [architecture.md](architecture.md)
- **Versions, config, credentials** → [configuration.md](configuration.md)
- **Day-to-day commands & the rebuild loop** → [operations.md](operations.md)
- **When something breaks** → [troubleshooting.md](troubleshooting.md)
- **Taking it to production** → [production.md](production.md) — what to swap each lab component for, and why
