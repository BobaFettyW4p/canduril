# canduril

NATS_CONTAINER := canduril-nats
PG_CONTAINER := canduril-postgres
MINIKUBE_PROFILE := minikube
NAMESPACE := canduril

.PHONY: help build test test-integration bench capture nats-up nats-down pg-up pg-down \
	minikube-up minikube-down up down images clean

INTEGRATION_TESTS := tests/test_normalizer_integration.py tests/test_ingestor_integration.py tests/test_gateway_integration.py tests/test_snapshotter_integration.py

help:
	@echo "canduril"
	@echo "========"
	@echo "  build            - build the native extension (uv sync)"
	@echo "  test             - run fast correctness tests (no external services needed)"
	@echo "  test-integration - run the ingestor+normalizer+gateway+snapshotter against real dockerized NATS+JetStream+Postgres"
	@echo "  bench            - run the baseline-vs-C++ benchmark against the sample fixture"
	@echo "  capture          - capture a fresh live Coinbase fixture"
	@echo "  nats-up/nats-down- start/stop a local NATS+JetStream container on :4222"
	@echo "  pg-up/pg-down    - start/stop a local Postgres container on :5432"
	@echo "  images           - build all 5 service images with plain docker (no cluster needed)"
	@echo "  up               - start Minikube and deploy the full stack via Skaffold"
	@echo "  down             - tear down the deployed stack and delete the Minikube cluster"
	@echo "  clean            - remove build artifacts"

build:
	uv sync

test: build
	uv run pytest tests/ -v $(addprefix --ignore=,$(INTEGRATION_TESTS))

nats-up:
	docker run --rm -d --name $(NATS_CONTAINER) -p 4222:4222 nats:latest -js

nats-down:
	-docker stop $(NATS_CONTAINER) >/dev/null 2>&1

pg-up:
	docker run --rm -d --name $(PG_CONTAINER) \
		-e POSTGRES_PASSWORD=canduril -e POSTGRES_DB=canduril \
		-p 5432:5432 postgres:16-alpine

pg-down:
	-docker stop $(PG_CONTAINER) >/dev/null 2>&1

test-integration: build nats-up pg-up
	PG_USER=postgres PG_PASSWORD=canduril PG_DATABASE=canduril \
	uv run pytest $(INTEGRATION_TESTS) -v; \
	status=$$?; \
	$(MAKE) nats-down; \
	$(MAKE) pg-down; \
	exit $$status

images:
	./docker/build-images.sh

minikube-up:
	minikube start -p $(MINIKUBE_PROFILE) --memory=8192 --cpus=4 --disk-size=50g --driver=docker
	-kubectl create namespace $(NAMESPACE)

minikube-down:
	minikube delete -p $(MINIKUBE_PROFILE)

up: minikube-up
	skaffold run -p minikube --status-check --cache-artifacts=false

down:
	-skaffold delete -p minikube
	$(MAKE) minikube-down

bench: build
	uv run python bench/run_bench.py

capture:
	uv run python bench/capture_coinbase.py --duration 60

clean:
	rm -rf build/ .pytest_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
