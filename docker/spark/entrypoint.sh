#!/usr/bin/env bash
set -euo pipefail

# Seed the notebooks volume from the image on first run (don't clobber edits).
if [ -d /opt/seed-notebooks ]; then
  cp -rn /opt/seed-notebooks/. /home/iceberg/notebooks/ 2>/dev/null || true
fi

# Launch Jupyter Lab. SPARK_HOME / PYTHONPATH are set in the image, so any
# notebook can do `SparkSession.builder.getOrCreate()` and get the demo catalog.
exec jupyter lab \
  --notebook-dir=/home/iceberg/notebooks \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --allow-root \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --ServerApp.allow_origin='*'
