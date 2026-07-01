#!/usr/bin/env bash
# Show cluster status + the URLs you can hit from the host.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd kubectl
cluster_exists || die "kind cluster '${CLUSTER_NAME}' not found."

log "Nodes"
kubectl --context "kind-${CLUSTER_NAME}" get nodes

echo
log "Workloads in '${NAMESPACE}'"
kc get pods,svc -o wide 2>/dev/null || true

cat <<EOF

${_C_GREEN}Access from your host:${_C_RESET}
  Jupyter Lab          http://localhost:8888
  Spark driver UI      http://localhost:4040   (live only while a notebook Spark session is running)
  Iceberg REST catalog http://localhost:8181/v1/config
  SeaweedFS S3 API     http://localhost:8333
  SeaweedFS master UI  http://localhost:9333
  Postgres (oneshop)   postgres://etluser@localhost:5432/oneshop  (password: etlpassword)

${_C_DIM}In-cluster DNS: seaweedfs:8333 (S3), iceberg-rest:8181 (catalog), postgres:5432 (source)
S3 credentials: admin / password${_C_RESET}
EOF
