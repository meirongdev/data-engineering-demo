#!/usr/bin/env bash
# One command: kind cluster -> build/load image -> deploy the lakehouse stack.
# Idempotent: safe to re-run.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# --- preflight ----------------------------------------------------------------
log "Preflight: checking tools ..."
for c in docker kind kubectl; do require_cmd "$c"; done
docker info >/dev/null 2>&1 || die "Docker daemon is not running. Start Docker Desktop and retry."
ok "docker / kind / kubectl present, Docker daemon up"

# --- cluster ------------------------------------------------------------------
if cluster_exists; then
  ok "kind cluster '${CLUSTER_NAME}' already exists — reusing it"
else
  log "Creating kind cluster '${CLUSTER_NAME}' ..."
  kind create cluster --name "${CLUSTER_NAME}" --config "${ROOT_DIR}/cluster/kind-config.yaml" --wait 120s
  ok "Cluster created"
fi
kubectl --context "kind-${CLUSTER_NAME}" get nodes

# --- image --------------------------------------------------------------------
"${SCRIPT_DIR}/build-image.sh"

# --- deploy -------------------------------------------------------------------
"${SCRIPT_DIR}/deploy.sh"

echo
ok "Stack is up. Open http://localhost:8888 and run notebooks/00-getting-started.ipynb"
dim "Tear it all down with: scripts/down.sh   (or: make down)"
