#!/usr/bin/env bash
# Apply the k8s manifests in dependency order and wait for everything to be ready.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd kubectl
cluster_exists || die "kind cluster '${CLUSTER_NAME}' not found. Run scripts/up.sh first."

K8S_DIR="${ROOT_DIR}/k8s"

log "Creating namespace ..."
kubectl --context "kind-${CLUSTER_NAME}" apply -f "${K8S_DIR}/00-namespace.yaml"

log "Deploying SeaweedFS (object storage) ..."
kc apply -f "${K8S_DIR}/10-seaweedfs.yaml"
kc rollout status deploy/seaweedfs --timeout=180s

log "Creating the 'warehouse' bucket ..."
kc apply -f "${K8S_DIR}/20-bucket-init.yaml"
kc wait --for=condition=complete job/bucket-init --timeout=120s

log "Deploying the Iceberg REST catalog ..."
kc apply -f "${K8S_DIR}/30-iceberg-rest.yaml"
kc rollout status deploy/iceberg-rest --timeout=180s

log "Deploying Spark + Jupyter ..."
kc apply -f "${K8S_DIR}/40-spark-iceberg.yaml"
kc rollout status deploy/spark-iceberg --timeout=300s

ok "All components are ready."
echo
"${SCRIPT_DIR}/status.sh" || true
