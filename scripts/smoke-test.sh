#!/usr/bin/env bash
# End-to-end smoke test: run PySpark inside the cluster to create an Iceberg
# table on SeaweedFS, write rows, read them back. Proves catalog + storage work.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_cmd kubectl
cluster_exists || die "kind cluster '${CLUSTER_NAME}' not found. Run scripts/up.sh first."

POD="$(kc get pod -l app=spark-iceberg -o jsonpath='{.items[0].metadata.name}')"
[ -n "${POD}" ] || die "spark-iceberg pod not found"
log "Running smoke test in pod ${POD} ..."

kc exec -i "${POD}" -- python3 - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.smoke")
spark.sql("DROP TABLE IF EXISTS demo.smoke.t")
spark.sql("CREATE TABLE demo.smoke.t (id BIGINT, msg STRING) USING iceberg")
spark.sql("INSERT INTO demo.smoke.t VALUES (1, 'hello'), (2, 'lakehouse')")

rows = spark.sql("SELECT * FROM demo.smoke.t ORDER BY id").collect()
print("Rows read back:", [(r.id, r.msg) for r in rows])
assert len(rows) == 2, f"expected 2 rows, got {len(rows)}"

files = spark.sql("SELECT file_path FROM demo.smoke.t.files").collect()
print("Data files on SeaweedFS:")
for f in files:
    print("  ", f.file_path)
    assert f.file_path.startswith("s3://warehouse/"), f"unexpected location: {f.file_path}"

print("SMOKE TEST PASSED")
spark.stop()
PY

ok "Smoke test passed — Iceberg table created and read back from SeaweedFS"
