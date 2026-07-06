# Airflow Integration: When & How

> **Status**: Implemented · **Last updated**: 2026-07-06
> **Reference**: `chapter-05` from *Practical Data Engineering with Apache Projects*

This doc is deliberately small. Its job is to show **when** you reach for Airflow
and **how** Airflow solves the problem — not to build a production platform. We
add exactly one DAG (`medallion_pipeline`) that replaces the existing CronJob,
and nothing more. Data-quality and reporting DAGs are left as
[future extensions](#5-future-extensions).

---

## 1. When do you introduce Airflow?

Not on day one. The lab already runs the full medallion pipeline two ways:

- `make pipeline` — a human runs the five stages on demand.
- `k8s/90-pipeline-cron.yaml` — a Kubernetes CronJob runs them daily at 06:00.

The CronJob is a single pod running a serial shell loop:

```sh
for s in 00_create_tables 10_postgres_to_bronze 20_s3_to_bronze \
          30_bronze_to_silver 40_silver_to_gold 50_gold_analytics; do
    spark-submit /opt/pipeline/$s.py     # stop the world if any one fails
done
```

That is genuinely fine — right up until the pipeline matters to someone. The
moment a stale gold table causes a bad decision, these questions start hurting:

| You need to… | The CronJob gives you… |
|---|---|
| Retry a stage after a transient JDBC/S3 blip | Nothing — the whole run dies |
| Run the two independent bronze ingests at once | Nothing — they run one after the other |
| See why last night's run failed | `kubectl logs`, if the pod is still around |
| Re-run *just* the failed stage | Nothing — re-run all five |
| Know a run failed *before* users tell you | Nothing — no history, no alerting |

**That table is the "when."** When the answers stop being "don't care" and start
being "I need that", you introduce an orchestrator. Airflow is the industry
standard for exactly these needs.

---

## 2. How does Airflow solve it?

The five stages don't change. The **way they're run** changes: from a shell loop
to a **DAG** (directed acyclic graph) of tasks.

```
                    ┌─ postgres_to_bronze ─┐
create_tables ──────┤                      ├──> bronze_to_silver ──> silver_to_gold ──> gold_analytics
                    └─ s3_to_bronze ───────┘
                     (run IN PARALLEL)
```

Expressing the work as a graph is what unlocks everything the CronJob lacked:

- **Dependency graph** — the two bronze ingests have no dependency on each other,
  so Airflow runs them in parallel; silver waits for *both*; gold waits for silver.
- **Per-task retries** — `retries=2` with backoff. A transient error retries that
  one task instead of taking down the run.
- **Visibility** — the web UI shows per-task logs, duration, run history, and a
  Graph/Gantt view. You can **re-run a single failed task**.

Crucially, **Airflow orchestrates; Spark still computes.** Each task launches an
*ephemeral pod* from the existing `spark-iceberg:local` image and runs the same
`spark-submit /opt/pipeline/XX.py` the CronJob ran — unchanged. Airflow adds the
control plane; it doesn't touch the ETL logic.

---

## 3. Architecture (kept small on purpose)

Airflow is an **opt-in layer**, deployed by `make airflow` on top of the base
stack — just like the serving layer (`make serving`). It needs only the base
stack (Spark image + Iceberg REST + SeaweedFS + Postgres). Trino/Metabase are
**not** required.

```
┌──────────────────────────── Kind Cluster (data-eng) ────────────────────────────┐
│                                                                                  │
│  Orchestration layer  (opt-in: make airflow)                                     │
│  ┌────────────────┐   ┌──────────────────────────────────────────────────────┐  │
│  │ airflow-postgres│  │ airflow  (1 pod, 2 containers)                        │  │
│  │ (metadata DB)   │◄─┤   scheduler  — parses DAGs, runs LocalExecutor tasks  │  │
│  └────────────────┘   │   webserver  — UI on :30880 → host :8880             │  │
│                       └───────────────────────┬──────────────────────────────┘  │
│                                               │ KubernetesPodOperator            │
│                                               ▼ (one ephemeral pod per task)     │
│  Base stack           ┌────────────────────────────────────────────────────┐    │
│  (make up)            │ spark-iceberg pod → spark-submit /opt/pipeline/XX.py │    │
│                       └────────────────────────────────────────────────────┘    │
│   SeaweedFS (S3) · Postgres (oneshop) · Iceberg REST catalog                     │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 Components (`k8s/85-airflow.yaml`)

| Component | K8s resource | Purpose |
|---|---|---|
| `airflow-postgres` | Deployment + Service | Airflow metadata DB (DAG runs, task instances). Separate from `oneshop` so the layer is self-contained. |
| `airflow` | Deployment (1 pod, 2 containers) | `scheduler` + `webserver`. An initContainer runs `airflow db migrate` + creates the `admin` user. |
| `airflow` | ServiceAccount + Role + RoleBinding | RBAC so `KubernetesPodOperator` can create/watch/delete pods and read their logs. |
| `airflow-webserver` | Service (NodePort 30880) | Web UI at `http://localhost:8880`. |

### 3.2 Why these choices

- **LocalExecutor**, not Celery/Kubernetes executor. This is a single node. The
  scheduler runs task subprocesses directly, which gives real parallelism (the
  two bronze ingests at once) without extra worker pods. The *distributed*
  compute comes from the ephemeral Spark pods, not from Airflow itself.
- **KubernetesPodOperator**, not a `spark-submit` baked into the Airflow image.
  Reusing the existing `spark-iceberg:local` image keeps the ETL in one place and
  maps 1:1 to what the CronJob did. Airflow stays tiny.
- **Dedicated metadata Postgres** (LocalExecutor needs a real DB — SQLite only
  supports the SequentialExecutor, which can't run tasks in parallel).

### 3.3 Airflow image (`docker/airflow/Dockerfile`)

```
FROM apache/airflow:2.10.5-python3.11
  └── apache-airflow-providers-cncf-kubernetes   # KubernetesPodOperator
      (resolved via Airflow's official constraints file → reproducible build)
COPY docker/airflow/dags/ → /opt/airflow/dags/    # DAGs baked into the image
```

---

## 4. The DAG (`docker/airflow/dags/medallion_pipeline.py`)

**Schedule**: `0 6 * * *` (daily 06:00 UTC — the same slot the CronJob used).
Un-paused on creation, so you can trigger it by hand right away.

```python
create_tables >> [postgres_to_bronze, s3_to_bronze] >> bronze_to_silver
bronze_to_silver >> silver_to_gold >> gold_analytics
```

Each `spark_stage(...)` is a `KubernetesPodOperator` that launches one
`spark-iceberg:local` pod running `spark-submit /opt/pipeline/<stage>.py`, with:

- `retries=2`, `retry_delay=1m` — the retry the CronJob never had.
- `in_cluster=True` — uses the mounted service account (see RBAC above).
- `on_finish_action="delete_succeeded_pod"` — tidy up on success, but **keep
  failed pods** so you can `kubectl logs` them.

### Run it

```bash
make build      # ensure spark-iceberg:local exists in the cluster (base stack)
make airflow    # build + load the Airflow image, deploy the layer
make airflow-ui # open http://localhost:8880  (admin / admin)
```

Then in the UI, trigger `medallion_pipeline` and watch the Graph view. In another
terminal, watch the ephemeral Spark pods come and go:

```bash
kubectl -n lakehouse get pods -w
```

---

## 5. Before vs After

| Aspect | Before (`90-pipeline-cron.yaml`) | After (`medallion_pipeline` DAG) |
|---|---|---|
| Orchestration | Serial shell loop | DAG with a parallel bronze fan-out |
| Retry on failure | None | 2 retries with backoff, per task |
| Re-run after a fix | Re-run all 5 stages | Re-run just the failed task |
| Monitoring | `kubectl logs` (if the pod survives) | Web UI: logs, duration, history, Gantt |
| Compute model | 1 pod runs everything | 1 ephemeral pod per stage |
| Cost | 1 CronJob pod | ~3 standing pods (+ ephemeral Spark pods) |

The last row is the honest trade-off: an orchestrator is not free. You take on
~3 standing pods (scheduler, webserver, metadata DB) in exchange for retries,
parallelism, and visibility. That's the "when" from §1 restated as a cost —
introduce Airflow when that trade is worth it, not before.

> **Resource note**: running Airflow *and* two parallel Spark pods on top of the
> base stack is heavier than the base lab's ~4 GB. Give Docker ~6–8 GB when you
> run the Airflow layer.

---

## 6. Future Extensions

These were intentionally cut to keep the demo focused. Each is a natural next DAG:

- **`data_quality` DAG** — `COUNT(*) > 0` / null / uniqueness gates on the
  Iceberg tables (with `retries=0` — quality checks should fail fast).
- **`gold_reporting` DAG** — `TrinoOperator` → CSV → upload to SeaweedFS →
  email. This is the DAG the reference `chapter-05` builds; it depends on the
  serving layer (Trino) and an SMTP sink (e.g. MailHog).
- **Alerting** — `on_failure_callback` → Slack/email when a run fails.
- **Sensors** — `S3KeySensor` to start the pipeline when new pageview files land.
- **Backfill** — Airflow's `catchup` to reprocess history after a schema change.

---

## 7. References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
- Project files: `k8s/85-airflow.yaml`, `docker/airflow/`, `scripts/deploy-airflow.sh`
- The "before": `k8s/90-pipeline-cron.yaml`
