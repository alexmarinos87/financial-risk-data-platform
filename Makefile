PYTHON ?= .venv/bin/python
PIP ?= $(PYTHON) -m pip

LOCAL_POSTGRES_DSN ?= postgresql://risk_user:risk_password@localhost:5433/risk_platform

.PHONY: setup lint test format benchmark-io docker-build k8s-render-dev k8s-render-prod k8s-check terraform-check infrastructure-check iteration-check clean-generated security-check readiness-check sandbox-once overnight-sandbox local-db-up local-db-down local-db-wait local-db-logs postgres-shell mongo-shell run-demo load-postgres-demo load-postgres-dry-run check-postgres-consistency consistency-demo

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

k8s-check:
	kubectl kustomize deploy/kubernetes/overlays/dev >/tmp/financial-risk-k8s-dev.yaml
	kubectl kustomize deploy/kubernetes/overlays/prod >/tmp/financial-risk-k8s-prod.yaml

terraform-check:
	terraform -chdir=infra/terraform fmt -check -diff
	terraform -chdir=infra/terraform init -backend=false
	terraform -chdir=infra/terraform validate

infrastructure-check: k8s-check terraform-check

iteration-check: security-check infrastructure-check readiness-check

clean-generated:
	rm -rf data .demo .benchmarks .pytest_cache .mypy_cache .ruff_cache

security-check:
	$(PYTHON) scripts/security_check.py

readiness-check: lint test clean-generated run-demo load-postgres-dry-run

sandbox-once:
	PYTHONUNBUFFERED=1 $(PYTHON) scripts/overnight_sandbox.py --cycles 1 --sleep-seconds 0

overnight-sandbox:
	PYTHONUNBUFFERED=1 $(PYTHON) scripts/overnight_sandbox.py --hours 8 --sleep-seconds 1800

local-db-up:
	docker compose up -d postgres mongo

local-db-down:
	docker compose down -v

local-db-wait:
	@until docker compose exec -T postgres pg_isready -U risk_user -d risk_platform >/dev/null 2>&1; do \
		echo "Waiting for PostgreSQL..."; \
		sleep 1; \
	done

local-db-logs:
	docker compose logs -f postgres mongo

postgres-shell:
	docker compose exec postgres psql -U risk_user -d risk_platform

mongo-shell:
	docker compose exec mongo mongosh risk_source

run-demo:
	$(PYTHON) -m src.orchestration.run_pipeline \
		--input tests/fixtures/demo_events.json \
		--late-seconds 60 \
		--vol-window 2 \
		--summary-json .demo/pipeline-summary.json

load-postgres-demo:
	$(PYTHON) -m src.warehouse.postgres_loader --dsn "$(LOCAL_POSTGRES_DSN)"

load-postgres-dry-run:
	$(PYTHON) -m src.warehouse.postgres_loader --dry-run

check-postgres-consistency:
	docker compose exec -T postgres psql -U risk_user -d risk_platform < sql/consistency_checks.sql

consistency-demo: clean-generated run-demo local-db-wait load-postgres-demo check-postgres-consistency
