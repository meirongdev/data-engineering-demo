#!/usr/bin/env bash
# Delete the kind cluster (and everything in it).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd kind

if cluster_exists; then
  log "Deleting kind cluster '${CLUSTER_NAME}' ..."
  kind delete cluster --name "${CLUSTER_NAME}"
  ok "Cluster deleted"
else
  warn "No kind cluster named '${CLUSTER_NAME}' — nothing to do"
fi
