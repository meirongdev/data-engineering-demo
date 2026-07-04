#!/usr/bin/env bash
# Build the Metabase image (if needed), load it into kind, and deploy the
# serving layer (Trino + Metabase) on top of the base lakehouse stack.
#
# Trino uses the stock trinodb/trino:482 image (no build needed).
# Metabase needs the Starburst Trino driver baked in (docker/metabase/Dockerfile).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd docker
require_cmd kubectl
cluster_exists || die "kind cluster '${CLUSTER_NAME}' not found. Run scripts/up.sh first."

K8S_DIR="${ROOT_DIR}/k8s"

# --- build Metabase image ---
log "Building Metabase image (metabase:local) ..."
docker build \
  -f "${ROOT_DIR}/docker/metabase/Dockerfile" \
  -t metabase:local \
  "${ROOT_DIR}"
ok "Built metabase:local"

log "Loading metabase:local into kind cluster '${CLUSTER_NAME}' ..."
kind load docker-image metabase:local --name "${CLUSTER_NAME}"
ok "Loaded metabase:local"

# --- deploy Trino ---
log "Deploying Trino (interactive SQL engine) ..."
kc apply -f "${K8S_DIR}/70-trino.yaml"
kc rollout status deploy/trino --timeout=300s
ok "Trino is ready"

# --- deploy Metabase ---
log "Deploying Metabase (BI dashboards) ..."
kc apply -f "${K8S_DIR}/80-metabase.yaml"
kc rollout status deploy/metabase --timeout=300s
ok "Metabase is ready"

echo
cat <<EOF
${_C_GREEN}Serving layer is up.${_C_RESET}
  Trino    http://localhost:8080   (SQL engine — jdbc:trino://localhost:8080/iceberg)
  Metabase http://localhost:3000   (BI dashboards — set up the Trino connection on first login)

Trino sees the same Iceberg catalog as Spark. Query it with:
  trino> SELECT * FROM iceberg.gold.item_performance LIMIT 10;

Metabase's Trino driver is already installed. On first login:
  1. Add a database connection: type=Trino, host=trino, port=8080, database=iceberg
  2. Browse the gold-layer tables and build a dashboard.
EOF
