# Thin wrapper around scripts/. Run `make help` for the list.
.DEFAULT_GOAL := help
SHELL := /bin/bash

# Config — override via env or `make VAR=value`. Mirrors scripts/lib.sh.
CLUSTER_NAME ?= data-eng
NAMESPACE    ?= lakehouse
CONTEXT      := kind-$(CLUSTER_NAME)

.PHONY: up down deploy build status smoke logs jupyter shell help

up: ## Create the kind cluster, build/load the image, and deploy everything
	./scripts/up.sh

down: ## Delete the kind cluster (and all data in it)
	./scripts/down.sh

build: ## Build the Spark/Iceberg image and load it into the cluster
	./scripts/build-image.sh

deploy: ## (Re)apply the k8s manifests and wait for readiness
	./scripts/deploy.sh

status: ## Show pods/services and the host access URLs
	./scripts/status.sh

smoke: ## Run the end-to-end Iceberg smoke test inside the cluster
	./scripts/smoke-test.sh

logs: ## Tail the Spark/Jupyter pod logs
	kubectl --context $(CONTEXT) -n $(NAMESPACE) logs -f deploy/spark-iceberg

jupyter: ## Open Jupyter Lab in your browser
	open http://localhost:8888 || xdg-open http://localhost:8888

shell: ## Shell into the Spark/Jupyter pod
	kubectl --context $(CONTEXT) -n $(NAMESPACE) exec -it deploy/spark-iceberg -- bash

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
