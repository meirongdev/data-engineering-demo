# Plan: add a query + BI serving layer (Trino + Metabase)

> **Status:** proposed, not started. Drafted 2026-07-04.
> Self-contained implementation plan — pick this up and execute without
> re-deriving. Referenced from `MEMORY.md`.

## Why this exists (the gap)

This repo is a modernized, Kubernetes-native rebuild of the Docker Compose
lakehouse in *Practical Data Engineering with Apache Projects*, **chapter-04**
(`~/projects/exploration/Practical-Data-Engineering-with-Apache-Projects/chapter-04`).

The repo has already reproduced **and modernized** almost all of that chapter:

| chapter-04 | this repo | status |
|---|---|---|
| MinIO | SeaweedFS | ✅ modernized |
| `iceberg-rest-fixture` (in-memory) | REST fixture + **JdbcCatalog on Postgres** | ✅ persistent |
| `spark-iceberg` + notebooks | Spark 3.5 + Iceberg 1.10 + Jupyter | ✅ |
| Postgres `oneshop` + loadgen | same | ✅ |
| bronze→silver→gold | richer Spark medallion pipeline | ✅ (better than book) |
| **Trino** (interactive SQL / serving engine) | — | ❌ **the gap** |
| **Superset** (BI dashboards on gold) | — | ❌ **the gap** |

The one layer chapter-04 has that this repo lacks is the **query + BI serving
layer**. Adding it completes the lakehouse story:
*storage → catalog → ETL compute (Spark) → **interactive query (Trino) → BI (Metabase)***.

**The teaching point that makes it worth doing:** Trino talks to the *same*
`iceberg-rest` catalog and the *same* SeaweedFS data files Spark writes — one
copy of data, one catalog, two engines (Spark for batch ETL, Trino for
interactive/BI). That decoupled-compute property *is* the lakehouse value
proposition, and the repo currently only demonstrates one engine.

## Decisions (already made — do not re-litigate)

1. **Stack: Trino + Metabase** (chosen over Trino + Superset and over
   DuckDB-only). Metabase is the lightest full serving layer — single pod with
   embedded H2, trivial setup — the best fit for kind's ~4 GB budget. Superset
   is book-faithful and fully Apache-2.0 but heavier (wants its own metadata DB
   + ideally Redis); Metabase is open-*core* but the community edition is ample
   for a lab.
2. **Trino is read-only by usage.** It points at the existing catalog + storage
   and only reads; Metabase (BI) only reads. **The Spark gold pipeline is
   untouched** — gold stays in Spark (`docker/spark/pipeline/40_silver_to_gold.py`).
   We do *not* port the book's Trino-CTAS gold approach.
3. **Opt-in, not default.** Trino (~1–1.5 GB JVM) + Metabase (~1 GB JVM) would
   push the base lab past ~4 GB. So the serving layer deploys on demand via a
   new `make serving` — mirroring how `make pipeline` / `make loadgen` are
   on-demand today. `make deploy` stays lean at ~4 GB. Query it *after*
   `make pipeline` has populated gold.

### 2026 landscape context (why these choices)

- **Trino** (v482 line) is still the standard engine for Iceberg REST + native
  S3. Modernization vs the book: use the **native S3 filesystem**
  (`fs.s3.enabled=true`, renamed from `fs.native-s3.enabled` in the Trino 4xx
  line) — the old Hadoop-S3 path is deprecated.
- **DuckDB** (v1.5.3, May 2026) now reads *and writes* Iceberg REST catalogs —
  a great lightweight complement (query gold from a notebook, zero extra pods),
  but **not** the served backend here: it currently only supports REST catalogs
  backed by S3 / S3-Tables storage, and SeaweedFS-as-S3 is untested there.
  → note as an optional bonus, not core.
- **Lakekeeper** (Rust, single binary, no JVM, Postgres-backed, K8s-native) is
  the 2026 lightweight Iceberg REST catalog that fits this repo's ethos far
  better than Polaris (JVM-heavy). → one-line note in `docs/production.md` as
  the aligned catalog-upgrade path. **Not a change in this plan.**

## Implementation — files to add / change

### New — Trino (stock image + ConfigMap, no build needed)

- **`k8s/70-trino.yaml`** — Deployment + NodePort (host **8080 → 30080**),
  init container blocking on `iceberg-rest:8181` + `seaweedfs:8333`, and a
  ConfigMap mounting `etc/catalog/iceberg.properties`:

  ```properties
  connector.name=iceberg
  iceberg.catalog.type=rest
  iceberg.rest-catalog.uri=http://iceberg-rest:8181
  iceberg.rest-catalog.warehouse=s3://warehouse/
  fs.s3.enabled=true              # ⚠ renamed from fs.native-s3.enabled in Trino 4xx — verify vs pinned version
  s3.endpoint=http://seaweedfs:8333
  s3.region=us-east-1
  s3.aws-access-key=admin
  s3.aws-secret-key=password
  s3.path-style-access=true
  ```

  Also mount a `jvm.config` capping heap (~1.5 GB) so it fits kind. Namespaces
  `bronze`/`silver`/`gold` appear automatically under the Trino catalog `iceberg`.

### New — Metabase (needs a built image)

- **`docker/metabase/Dockerfile`** — Metabase does **not** ship a Trino driver.
  Bake the **Starburst Metabase driver JAR** into `/plugins/`. (This is the one
  real gotcha vs Superset's `pip install trino[sqlalchemy]`.)
- **`k8s/80-metabase.yaml`** — Deployment + NodePort (host **3000 → 30300**),
  embedded **H2** metadata store on a small PVC (single pod — lighter than
  Superset's separate Postgres). `MB_DB_FILE` env → the H2 file on the PVC so
  saved questions/dashboards survive pod restarts.

### Edits — wire into repo conventions

- **`cluster/kind-config.yaml`** — add two `extraPortMappings` (30080, 30300).
  ⚠️ kind fixes host ports at cluster-creation, so existing clusters need
  `make down && make up` **or** `kubectl port-forward` — document both.
- **`scripts/deploy-serving.sh`** (new) + **`Makefile`** target `serving:` —
  build/load the Metabase image, `kc apply` `70-` then `80-`, `kc rollout status`.
  Kept separate from `deploy.sh` so the base lab stays lean.
- **`scripts/status.sh`** — add Trino (`http://localhost:8080`) + Metabase
  (`http://localhost:3000`) to the access-URL block.
- **`scripts/build-image.sh`** — include the Metabase image in the build +
  `kind load` step.
- **Version pins in BOTH places** (see `MEMORY.md` → version-pins rule): the
  Trino image tag, Metabase image tag, and Starburst driver version must be
  pinned in the Dockerfile/manifests *and* wherever the repo tracks its version
  matrix (`docs/configuration.md`, `pyproject.toml`).

### New — notebook + docs

- **`notebooks/05-query-with-trino.ipynb`** — connect to Trino, `SELECT` from
  `iceberg.gold.item_performance`, show it is the *same* table Spark wrote
  (the shared-catalog teaching point).
- **`docs/serving.md`** — the query/BI layer, the shared-catalog concept, and a
  Metabase quickstart (add Trino connection → dashboard on gold).
- **`README.md`** + **`docs/architecture.md`** — diagram, ports table,
  persistence table updates.
- **`docs/production.md`** — one-line Lakekeeper note (catalog-upgrade path).

## Caveats to surface in docs (not blockers)

- **Trino S3 property rename** — `fs.native-s3.enabled` → `fs.s3.enabled` in the
  Trino 4xx line. Pin a Trino version and use the matching name (verify against
  that version's docs at build time).
- **kind host ports** — adding NodePorts to a running cluster needs a recreate;
  `kubectl port-forward svc/trino 8080:8080` is the no-recreate escape hatch.
- **Resource budget** — serving layer adds ~2.5 GB. Base lab stays ~4 GB because
  serving is opt-in; document ~6 GB free RAM when running `make serving`.

## Execution checklist

- [ ] `k8s/70-trino.yaml` (Deployment + Svc + ConfigMap + init container)
- [ ] `docker/metabase/Dockerfile` (+ Starburst Trino driver JAR)
- [ ] `k8s/80-metabase.yaml` (Deployment + Svc + PVC, embedded H2)
- [ ] `cluster/kind-config.yaml` port maps (30080, 30300)
- [ ] `scripts/deploy-serving.sh` + `Makefile` `serving:` target
- [ ] `scripts/build-image.sh` + `scripts/status.sh` updates
- [ ] Pin versions in both places
- [ ] `notebooks/05-query-with-trino.ipynb`
- [ ] `docs/serving.md`; update `README.md`, `docs/architecture.md`, `docs/production.md`
- [ ] Verify end-to-end: `make up` → `make pipeline` → `make serving` →
      Trino reads `iceberg.gold.item_performance` → Metabase dashboard renders
- [ ] Suggested branch: `feat/serving-layer`

## Sources (2026 grounding)

- Trino: https://trino.io/docs/current/connector/iceberg.html ·
  https://trino.io/docs/current/object-storage/file-system-s3.html
- DuckDB Iceberg: https://duckdb.org/docs/lts/core_extensions/iceberg/iceberg_rest_catalogs ·
  https://duckdb.org/2026/05/29/new-iceberg-features
- BI comparison: https://blog.elest.io/apache-superset-vs-metabase-vs-redash-which-open-source-bi-tool-to-self-host-in-2026/ ·
  https://www.metabase.com/lp/metabase-vs-superset
- Catalogs: https://dev.to/alexmercedcoder/the-state-of-apache-iceberg-catalogs-in-june-2026-265e ·
  https://github.com/lakekeeper/lakekeeper
