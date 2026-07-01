#!/usr/bin/env bash
# Shared helpers + config for the demo scripts. Source this, don't run it.

# --- config (override via env) ------------------------------------------------
CLUSTER_NAME="${CLUSTER_NAME:-data-eng}"
NAMESPACE="${NAMESPACE:-lakehouse}"
IMAGE="${IMAGE:-spark-iceberg:local}"

# Repo root = parent of this scripts/ dir, resolved regardless of caller cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# --- pretty logging -----------------------------------------------------------
if [ -t 1 ]; then
  _C_BLUE='\033[0;34m'; _C_GREEN='\033[0;32m'; _C_YELLOW='\033[0;33m'
  _C_RED='\033[0;31m'; _C_DIM='\033[0;90m'; _C_RESET='\033[0m'
else
  _C_BLUE=''; _C_GREEN=''; _C_YELLOW=''; _C_RED=''; _C_DIM=''; _C_RESET=''
fi

log()  { printf "${_C_BLUE}==>${_C_RESET} %s\n" "$*"; }
ok()   { printf "${_C_GREEN}  ✓${_C_RESET} %s\n" "$*"; }
warn() { printf "${_C_YELLOW}  !${_C_RESET} %s\n" "$*"; }
err()  { printf "${_C_RED} ✗ ${_C_RESET} %s\n" "$*" >&2; }
dim()  { printf "${_C_DIM}%s${_C_RESET}\n" "$*"; }

die()  { err "$*"; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

kc() { kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" "$@"; }

cluster_exists() {
  kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"
}
