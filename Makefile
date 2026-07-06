# Thin wrapper around scripts/. Run `make help` for the list.
.DEFAULT_GOAL := help
SHELL := /bin/bash

# Config — single source of truth in scripts/defaults.env; override via env or `make VAR=value`.
include scripts/defaults.env
CONTEXT := kind-$(CLUSTER_NAME)

.PHONY: up down deploy build build-serving status serving airflow airflow-ui check hooks smoke pipeline loadgen logs jupyter shell help

up: ## Create the kind cluster, build/load the image, and deploy everything
	./scripts/up.sh

down: ## Delete the kind cluster (and all data in it)
	./scripts/down.sh

build: ## Build the base images (Spark, loadgen, iceberg-rest) and load into cluster
	./scripts/build-image.sh

build-serving: ## Build just the serving-layer images (metabase)
	docker build -f docker/metabase/Dockerfile -t metabase:local .
	kind load docker-image metabase:local --name $(CLUSTER_NAME)

deploy: ## (Re)apply the k8s manifests and wait for readiness
	./scripts/deploy.sh

serving: ## Deploy the serving layer (Trino + Metabase) on top of the base stack
	./scripts/deploy-serving.sh

airflow: ## Deploy the orchestration layer (Airflow) on top of the base stack
	./scripts/deploy-airflow.sh

airflow-ui: ## Open the Airflow web UI in your browser (admin/admin)
	open http://localhost:8880 || xdg-open http://localhost:8880

check: ## Run static pre-flight checks (shell/notebook/yaml/image-pins) — no cluster needed
	./scripts/check.sh

hooks: ## Enable the git pre-commit hook (runs `make check` before each commit)
	git config core.hooksPath .githooks
	@printf '  \033[0;32m✓\033[0m git pre-commit hook enabled (core.hooksPath=.githooks)\n'
	@echo '    bypass a single commit with: git commit --no-verify'

status: ## Show pods/services and the host access URLs
	./scripts/status.sh

smoke: ## Run the end-to-end Iceberg smoke test inside the cluster
	./scripts/smoke-test.sh

pipeline: ## Run the full medallion pipeline (loadgen -> bronze -> silver -> gold)
	./scripts/pipeline.sh

loadgen: ## (Re)run just the load generator Job (seed Postgres + pageviews)
	./scripts/run-loadgen.sh

logs: ## Tail the Spark/Jupyter pod logs
	kubectl --context $(CONTEXT) -n $(NAMESPACE) logs -f deploy/spark-iceberg

jupyter: ## Open Jupyter Lab in your browser
	open http://localhost:8888 || xdg-open http://localhost:8888

shell: ## Shell into the Spark/Jupyter pod
	kubectl --context $(CONTEXT) -n $(NAMESPACE) exec -it deploy/spark-iceberg -- bash

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
