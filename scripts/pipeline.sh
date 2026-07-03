#!/usr/bin/env bash
# Run the full medallion pipeline end to end:
#   1. loadgen Job          -> seed Postgres + pageview JSON on SeaweedFS
#   2. 00_create_tables     -> bronze/silver/gold Iceberg tables
#   3. 10_postgres_to_bronze, 20_s3_to_bronze   -> bronze ingest
#   4. 30_bronze_to_silver  -> validate + enrich
#   5. 40_silver_to_gold    -> item_performance analytics
#
# Stages 2-5 run via spark-submit inside the spark-iceberg pod (scripts baked
# into the image at /opt/pipeline). Rebuild the image after changing them:
# `make build`.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd kubectl
cluster_exists || die "kind cluster '${CLUSTER_NAME}' not found. Run scripts/up.sh first."

# --- 1. load generation -------------------------------------------------------
"${SCRIPT_DIR}/run-loadgen.sh"

# --- 2-5. spark stages --------------------------------------------------------
POD="$(kc get pod -l app=spark-iceberg -o jsonpath='{.items[0].metadata.name}')"
[ -n "${POD}" ] || die "spark-iceberg pod not found"

run_stage() {
  local script="$1" label="$2"
  log "Stage: ${label}  (/opt/pipeline/${script})"
  kc exec -i "${POD}" -- spark-submit "/opt/pipeline/${script}"
  ok "${label} done"
}

run_stage 00_create_tables.py       "create tables"
run_stage 10_postgres_to_bronze.py  "Postgres -> bronze"
run_stage 20_s3_to_bronze.py        "pageviews JSON -> bronze"
run_stage 30_bronze_to_silver.py    "bronze -> silver"
run_stage 40_silver_to_gold.py      "silver -> gold"

echo
ok "Pipeline complete. Explore the results in Jupyter (notebooks 01-04) or:"
dim "  make shell  # then: spark-sql -e 'SELECT * FROM demo.gold.item_performance ORDER BY revenue DESC LIMIT 10'"
