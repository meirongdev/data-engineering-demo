#!/usr/bin/env bash
# (Re)run just the load generator Job: seed Postgres + pageview JSON on SeaweedFS.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd kubectl
cluster_exists || die "kind cluster '${CLUSTER_NAME}' not found. Run scripts/up.sh first."

log "Running loadgen Job (seed Postgres + pageviews) ..."
kc delete job loadgen --ignore-not-found >/dev/null 2>&1 || true
kc apply -f "${ROOT_DIR}/k8s/60-loadgen.yaml"

if ! kc wait --for=condition=complete job/loadgen --timeout=300s 2>/dev/null; then
  kc logs job/loadgen --tail=50 || true
  kc wait --for=condition=failed job/loadgen --timeout=5s 2>/dev/null && die "loadgen Job failed"
  die "loadgen Job did not complete within timeout"
fi
kc logs job/loadgen --tail=20 || true
ok "Load generation complete"
