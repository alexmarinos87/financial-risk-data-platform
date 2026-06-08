PYTHON ?= .venv/bin/python
PIP ?= $(PYTHON) -m pip

.PHONY: setup lint test format benchmark-io docker-build k8s-render-dev k8s-render-prod clean-generated local-db-up local-db-down local-db-logs postgres-shell mongo-shell

setup:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -e '.[dev]'

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff check . --fix

test:
	$(PYTHON) -m pytest -q

benchmark-io:
	$(PYTHON) -m src.benchmarks.io_engine_benchmark --summary-json .benchmarks/io_engine/summary.json

docker-build:
	docker build -t financial-risk-data-platform:local .

k8s-render-dev:
	kubectl kustomize deploy/kubernetes/overlays/dev

k8s-render-prod:
	kubectl kustomize deploy/kubernetes/overlays/prod

clean-generated:
	rm -rf data .demo .benchmarks .pytest_cache .mypy_cache .ruff_cache

local-db-up:
	docker compose up -d postgres mongo

local-db-down:
	docker compose down -v

local-db-logs:
	docker compose logs -f postgres mongo

postgres-shell:
	docker compose exec postgres psql -U risk_user -d risk_platform

mongo-shell:
	docker compose exec mongo mongosh risk_source
