# The medallion architecture

The **medallion architecture** (also called *multi-hop* architecture) is a way
of organising a data platform into a small number of layers, each holding the
data at a different level of refinement. Data flows in one direction —
**bronze → silver → gold** — and gets more trustworthy, more structured, and
more business-ready at every hop.

This page introduces the *pattern* in general terms. For how **this lab**
implements it (the actual tables and Spark stages), see
[pipeline.md](pipeline.md).

---

## The problem it solves

You could answer a business question with one big query straight off the raw
data. It works once. Then the raw feed changes shape, a second team needs the
same cleaning logic, a bad row poisons a report, and you can't tell whether the
number is wrong because of the source, the cleaning, or the aggregation.

The medallion architecture fixes this by giving each concern its own layer:

- **Land the data faithfully** before touching it (bronze), so you can always
  reprocess from a known-good copy.
- **Clean and integrate** in one well-defined place (silver), so every consumer
  shares the same definition of "a valid customer" or "a completed order".
- **Aggregate for a specific question** last (gold), so business logic is
  isolated and cheap to change.

Each layer is **independently testable, reprocessable, and reusable** — that
separation is what keeps a growing platform maintainable.

## The three layers

| Layer | Also called | Job | Business logic? | Typical consumers |
|---|---|---|---|---|
| **Bronze** | raw / landing | A faithful copy of each source, as-is | **None** | pipelines only |
| **Silver** | cleansed / conformed | Validated, de-duplicated, integrated entities | Cleaning + joins | analysts, data scientists, downstream pipelines |
| **Gold** | curated / serving | Aggregated, decision-ready tables | Business rules + metrics | dashboards, reports, ML features |

### Bronze — land it faithfully

Bronze is the **landing zone**. It ingests each source with as little
transformation as possible: type-cast to a target schema, maybe add ingestion
metadata (load timestamp, source file), but **no business rules, no filtering,
no joins**. If a row is ugly, bronze keeps it ugly.

Why so hands-off? Because bronze is the platform's **replay log**. As long as
you have a faithful copy, anything downstream can be rebuilt from it — so you
can fix a bug in silver and reprocess, without re-fetching from the (possibly
now-changed) source.

### Silver — clean it and connect it

Silver is where data becomes **trustworthy**. This layer:

- **Validates** — flag or drop malformed records (e.g. mark whether an email
  matches a pattern).
- **Cleans / conforms** — normalise categories, clamp impossible values, cast
  units, standardise keys.
- **Integrates** — join related sources into meaningful entities (an order
  enriched with who bought it and what was sold).

Silver is the layer **most people should build on**. It's detailed enough to
answer new questions, but clean enough that you don't repeat the same fixes in
every query.

### Gold — answer the question

Gold tables are **aggregated and purpose-built**: one table per business
question, shaped so a dashboard or report can read it almost directly. This is
where metrics like revenue, order counts, and conversion rates live. Gold is
usually small, heavily read, and rebuilt from silver whenever the definitions
change.

## Principles that make it work

- **One-directional flow.** Data only moves bronze → silver → gold. A gold table
  never writes back into silver. This keeps lineage easy to reason about.
- **Bronze is the source of truth for reprocessing**, not the original system.
  Everything downstream is a pure function of bronze.
- **Idempotent, reproducible loads.** Re-running a stage should produce the same
  result — typically via full `overwrite` or merge/upsert on a key — so a rerun
  after a fix is safe.
- **Push business logic downstream.** The closer to gold, the more
  business-specific and the more likely to change; keeping it out of bronze
  means source changes don't ripple through your metric definitions.
- **Each layer is a contract.** Consumers depend on silver's shape, not on the
  raw source — so you can swap or fix the source without breaking every report.

## Where people draw the lines differently

The three names are a convention, not a law. Common variations you'll see:

- **More or fewer layers** — some teams add a "silver 1 / silver 2" split
  (cleansed vs. conformed), others collapse to just raw + curated.
- **How much cleaning is "silver".** Light validation vs. full
  master-data-management is a spectrum; teams pick a point and document it.
- **Streaming vs. batch.** The same layering applies to streaming pipelines;
  only the mechanics (incremental merges, watermarks) differ.

The value isn't in the exact number of layers — it's in **separating "keep the
data" from "clean the data" from "answer the question."**

## How this lab maps onto it

This lab implements a concrete bronze → silver → gold pipeline over a fictional
`oneshop` e-commerce dataset, all as Iceberg tables in the `demo` catalog:

| Layer | This lab's tables (examples) | What happens |
|---|---|---|
| **Bronze** | `demo.bronze.purchases`, `demo.bronze.pageviews` | raw copies of the Postgres OLTP tables (via JDBC) and the raw pageview JSON (via `s3a://`) |
| **Silver** | `demo.silver.purchases_enriched`, `demo.silver.users` | validate emails, normalise categories, clamp prices, and join facts into enriched entities |
| **Gold** | `demo.gold.item_performance`, `demo.gold.top_selling_items`, `demo.gold.sales_performance_24h`, `demo.gold.pageviews_by_channel`, `demo.gold.user_engagement_segments` | product analytics (revenue, conversion, top sellers, hourly trends), user analytics (RFM-style engagement segments), channel analysis |
| **Gold CSV export** | `s3a://customer-segments/segmented_users/` | engagement segments delivered as CSV to object storage for downstream teams |

The full table list, the transform in each stage, and how to run it (`make
pipeline` or notebooks `01`–`04`) are documented in
[pipeline.md](pipeline.md).

## Where to go next

- **This lab's implementation, stage by stage** → [pipeline.md](pipeline.md)
- **The business scenario and the questions it answers** → [overview.md](overview.md)
- **The platform the pipeline runs on** → [architecture.md](architecture.md)
