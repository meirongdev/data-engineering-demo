#!/usr/bin/env bash
# Build the Airflow image, load it into kind, and deploy the orchestration layer
# (Airflow scheduler + webserver + metadata DB) on top of the base lakehouse stack.
#
# Airflow is the "after" to the serial CronJob (k8s/90-pipeline-cron.yaml): each
# medallion stage becomes a DAG task that launches an ephemeral spark-iceberg pod.
# It needs only the BASE stack (Spark image + iceberg-rest + seaweedfs + postgres);
# the serving layer (Trino/Metabase) is NOT required.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd docker
require_cmd kind
require_cmd kubectl
cluster_exists || die "kind cluster '${CLUSTER_NAME}' not found. Run scripts/up.sh first."

AIRFLOW_IMAGE="${AIRFLOW_IMAGE:-airflow-iceberg:local}"
K8S_DIR="${ROOT_DIR}/k8s"

# The DAG launches spark-iceberg:local pods, so that image must exist in the
# cluster. It's built by `make build` / `make up`.
if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  warn "${IMAGE} not found locally — the DAG needs it. Run 'make build' first."
fi

# --- build Airflow image ---
log "Building Airflow image (${AIRFLOW_IMAGE}) ..."
docker build \
  -f "${ROOT_DIR}/docker/airflow/Dockerfile" \
  -t "${AIRFLOW_IMAGE}" \
  "${ROOT_DIR}"
ok "Built ${AIRFLOW_IMAGE}"

log "Loading ${AIRFLOW_IMAGE} into kind cluster '${CLUSTER_NAME}' ..."
kind load docker-image "${AIRFLOW_IMAGE}" --name "${CLUSTER_NAME}"
ok "Loaded ${AIRFLOW_IMAGE}"

# --- deploy Airflow ---
log "Deploying Airflow (metadata DB + scheduler + webserver) ..."
kc apply -f "${K8S_DIR}/85-airflow.yaml"
kc rollout status deploy/airflow-postgres --timeout=180s
kc rollout status deploy/airflow --timeout=300s
ok "Airflow is ready"

# The image tag is unchanged, so `kubectl apply` alone won't restart a running
# pod. If Airflow is already deployed, roll it so it picks up new DAG code.
if kc get deploy/airflow >/dev/null 2>&1; then
  log "Restarting Airflow to pick up the new image/DAGs ..."
  kc rollout restart deploy/airflow
  kc rollout status deploy/airflow --timeout=300s
  ok "Airflow restarted on the new image"
fi

echo
cat <<EOF
${_C_GREEN}Orchestration layer is up.${_C_RESET}
  Airflow UI   http://localhost:8880   (login: admin / admin)

The 'medallion_pipeline' DAG turns the 5-stage shell loop (90-pipeline-cron.yaml)
into a task graph: parallel bronze ingest, per-task retries, full run history.

Try it:
  1. Open http://localhost:8880   (admin / admin)
  2. Trigger the 'medallion_pipeline' DAG (the play button) and watch the Graph view.
  3. Each task launches an ephemeral spark-iceberg pod — watch them come and go:
       kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} get pods -w

If localhost:8880 is unreachable (your cluster predates the port map in
cluster/kind-config.yaml), either recreate it with 'make down && make up', or:
  kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} port-forward svc/airflow-webserver 8880:8080
EOF
