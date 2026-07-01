# syntax=docker/dockerfile:1
#
# One-shot load generator for the lakehouse demo.
#
# Seeds the "oneshop" Postgres tables (users / items / purchases) and writes a
# bounded batch of pageview events as newline-delimited JSON into the SeaweedFS
# `pageviews` bucket. Runs to completion and exits (see k8s/60-loadgen.yaml).
FROM python:3.11-slim-bookworm

ENV PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY docker/loadgen/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docker/loadgen/generate_load.py .

ENTRYPOINT ["python", "generate_load.py"]
