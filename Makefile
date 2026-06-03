PYTHON ?= .venv/bin/python
PIP ?= $(PYTHON) -m pip

.PHONY: setup lint test format benchmark-io docker-build k8s-render-dev k8s-render-prod clean-generated

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
