#!/usr/bin/env bash
# Build the demo images and load them into the kind cluster:
#   * spark-iceberg:local — Spark + Iceberg + Jupyter + pipeline scripts
#   * loadgen:local       — one-shot Postgres/pageview seeder
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd docker
require_cmd kind

log "Building image ${IMAGE} (native $(uname -m)) ..."
docker build \
  -f "${ROOT_DIR}/docker/spark/Dockerfile" \
  -t "${IMAGE}" \
  "${ROOT_DIR}"
ok "Built ${IMAGE}"

log "Building image ${LOADGEN_IMAGE} ..."
docker build \
  -f "${ROOT_DIR}/docker/loadgen/Dockerfile" \
  -t "${LOADGEN_IMAGE}" \
  "${ROOT_DIR}"
ok "Built ${LOADGEN_IMAGE}"

log "Building image ${ICEBERG_REST_IMAGE} ..."
docker build \
  -f "${ROOT_DIR}/docker/iceberg-rest/Dockerfile" \
  -t "${ICEBERG_REST_IMAGE}" \
  "${ROOT_DIR}"
ok "Built ${ICEBERG_REST_IMAGE}"

# NOTE: the serving-layer image (metabase:local) is intentionally NOT built here.
# It is opt-in and built/loaded by `make serving` (scripts/deploy-serving.sh) to
# keep the base stack lean.

if ! cluster_exists; then
  warn "Cluster '${CLUSTER_NAME}' not found — skipping load. Create it first (scripts/up.sh)."
  exit 0
fi

log "Loading images into kind cluster '${CLUSTER_NAME}' ..."
kind load docker-image "${IMAGE}" "${LOADGEN_IMAGE}" "${ICEBERG_REST_IMAGE}" --name "${CLUSTER_NAME}"
ok "Images loaded into cluster"

# The image tag is unchanged, so kubectl apply alone won't restart a running
# pod. If spark-iceberg is already deployed, roll it so it runs the new image.
if kc get deploy/spark-iceberg >/dev/null 2>&1; then
  log "Restarting spark-iceberg to pick up the new image ..."
  kc rollout restart deploy/spark-iceberg
  kc rollout status deploy/spark-iceberg --timeout=300s
  ok "spark-iceberg restarted on the new image"
fi

# Same for iceberg-rest.
if kc get deploy/iceberg-rest >/dev/null 2>&1; then
  log "Restarting iceberg-rest to pick up the new image ..."
  kc rollout restart deploy/iceberg-rest
  kc rollout status deploy/iceberg-rest --timeout=180s
  ok "iceberg-rest restarted on the new image"
fi
