#!/usr/bin/env bash
# Static pre-flight checks — fast, dependency-free (bash + python3, no cluster).
#
# Run manually with `make check`, or automatically before every commit via the
# git pre-commit hook (`make hooks`). Each of these guards a class of bug found
# in review, so the same problem can't silently come back:
#
#   1. shell syntax errors in scripts/*.sh                 (bash -n)
#   2. Python syntax errors in notebook code cells, checked at Python 3.11 —
#      the Spark image's runtime — so f-string / 3.12-only breakage is caught
#      even when this checker runs on a newer interpreter
#   3. malformed k8s / cluster YAML
#   4. base images pinned to :latest  (reproducibility regression; the
#      iceberg-rest → tabulario/latest slip is exactly this class)
#   5. iceberg-rest drifting off apache/iceberg-rest
#   6. version pins out of sync between pyproject.toml and docker/spark/Dockerfile
#
# NOTE: no `set -e` — we run every check and report ALL failures at once.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require_cmd python3
cd "${ROOT_DIR}"

_FAILED=0
fail() { _FAILED=$((_FAILED + 1)); }

# --- 1. shell script syntax --------------------------------------------------
log "Checking shell script syntax ..."
_shell_ok=1
for f in scripts/*.sh; do
  if ! out="$(bash -n "$f" 2>&1)"; then
    err "shell syntax: $f"
    printf '%s\n' "$out" | sed 's/^/      /'
    _shell_ok=0
  fi
done
# Optional: shellcheck (errors only) if the dev has it installed.
if command -v shellcheck >/dev/null 2>&1; then
  if ! shellcheck -S error -x scripts/*.sh; then _shell_ok=0; fi
fi
if [ "${_shell_ok}" -eq 1 ]; then ok "shell scripts OK"; else fail; fi

# --- 2-6. python-based checks (notebooks, YAML, image pins, version sync) -----
log "Checking notebooks, YAML, image pins, and version sync ..."
if ! python3 - <<'PY'
import ast, glob, json, re, sys

errors = []

# Match the Spark image runtime (docker/spark/Dockerfile: FROM python:3.11) so
# 3.12-only syntax that would crash in-cluster is rejected here too. feature_version
# cannot exceed the host, so clamp for anyone still on an older interpreter.
host = sys.version_info[:2]
FV = min((3, 11), host)

# 2. notebooks: valid JSON + every code cell parses (IPython magics stripped) --
nb_bad = []
for path in sorted(glob.glob("notebooks/*.ipynb")):
    try:
        nb = json.load(open(path, encoding="utf-8"))
    except Exception as e:
        nb_bad.append(f"{path}: invalid JSON: {e}")
        continue
    for i, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        if src.lstrip().startswith("%%"):        # cell magic → not Python, skip
            continue
        # blank out line magics / shell escapes so ast sees valid Python
        clean = "\n".join(
            "" if ln.lstrip().startswith(("%", "!")) else ln
            for ln in src.splitlines()
        )
        try:
            ast.parse(clean, filename=f"{path}[cell {i}]", feature_version=FV)
        except SyntaxError as e:
            nb_bad.append(f"{path} cell {i}: {e.msg} (line {e.lineno})")
if nb_bad:
    print("   ✗ notebook syntax:")
    for m in nb_bad:
        print(f"      {m}")
    errors.append("notebooks")
else:
    print(f"   ✓ notebooks parse at py{FV[0]}.{FV[1]}")

# 3. k8s / cluster YAML is well-formed ---------------------------------------
try:
    import yaml
    yaml_bad = []
    for path in sorted(glob.glob("k8s/*.yaml") + glob.glob("cluster/*.yaml")):
        try:
            list(yaml.safe_load_all(open(path, encoding="utf-8")))
        except Exception as e:
            yaml_bad.append(f"{path}: {e}")
    if yaml_bad:
        print("   ✗ YAML:")
        for m in yaml_bad:
            print(f"      {m}")
        errors.append("yaml")
    else:
        print("   ✓ k8s/cluster YAML valid")
except ImportError:
    print("   ! PyYAML not installed — skipping YAML validation (pip install pyyaml)")

# 4. no unpinned :latest base images (minio/mc is intentional — see docs) ------
ALLOW_LATEST = {"minio/mc"}
latest_bad = []
for path in sorted(glob.glob("k8s/*.yaml")):
    for n, line in enumerate(open(path, encoding="utf-8"), 1):
        m = re.search(r"image:\s*(\S+):latest\b", line)
        if m and m.group(1) not in ALLOW_LATEST:
            latest_bad.append(f"{path}:{n}: {line.strip()}")
for path in sorted(glob.glob("docker/*/Dockerfile")):
    for n, line in enumerate(open(path, encoding="utf-8"), 1):
        if re.match(r"\s*FROM\s+\S+:latest\b", line):
            latest_bad.append(f"{path}:{n}: {line.strip()}")
if latest_bad:
    print("   ✗ unpinned :latest image:")
    for m in latest_bad:
        print(f"      {m}")
    errors.append("pinned-images")
else:
    print("   ✓ no unpinned :latest base images")

# 5. iceberg-rest stays on apache/iceberg-rest (not tabulario/latest) ---------
df = open("docker/iceberg-rest/Dockerfile", encoding="utf-8").read()
if "apache/iceberg-rest:" in df and "tabulario" not in df.lower():
    print("   ✓ iceberg-rest pinned to apache/iceberg-rest")
else:
    print("   ✗ iceberg-rest Dockerfile must FROM apache/iceberg-rest:<pin>, not tabulario/latest")
    errors.append("iceberg-rest-base")

# 6. version pins in sync: pyproject.toml <-> docker/spark/Dockerfile ---------
def pins(text):
    out = {}
    for name, ver in re.findall(r"([A-Za-z0-9_.\-]+)(?:\[[^\]]*\])?==([0-9][0-9A-Za-z.\-]*)", text):
        out[name.lower().replace("_", "-")] = ver
    return out

proj = pins(open("pyproject.toml", encoding="utf-8").read())
dock = pins(open("docker/spark/Dockerfile", encoding="utf-8").read())
shared = sorted(set(proj) & set(dock))
drift = [f"{n}: pyproject={proj[n]} vs Dockerfile={dock[n]}" for n in shared if proj[n] != dock[n]]
if drift:
    print("   ✗ version pin drift (sync both — see project-version-pins-two-sources):")
    for m in drift:
        print(f"      {m}")
    errors.append("pin-sync")
else:
    print(f"   ✓ version pins in sync ({', '.join(shared) or 'none shared'})")

sys.exit(1 if errors else 0)
PY
then
  fail
fi

# --- summary -----------------------------------------------------------------
echo
if [ "${_FAILED}" -gt 0 ]; then
  err "checks FAILED — fix the items above (bypass a commit with: git commit --no-verify)"
  exit 1
fi
ok "all checks passed"
