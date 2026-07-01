#!/usr/bin/env bash
# Build the Spark+Iceberg+Jupyter image and load it into the kind cluster.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd docker
require_cmd kind

log "Building image ${IMAGE} (native $(uname -m)) ..."
docker build \
  -f "${ROOT_DIR}/docker/spark/Dockerfile" \
  -t "${IMAGE}" \
  "${ROOT_DIR}"
ok "Image built"

if ! cluster_exists; then
  warn "Cluster '${CLUSTER_NAME}' not found — skipping load. Create it first (scripts/up.sh)."
  exit 0
fi

log "Loading ${IMAGE} into kind cluster '${CLUSTER_NAME}' ..."
kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"
ok "Image loaded into cluster"
