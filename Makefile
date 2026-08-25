PYTHON ?= .venv/bin/python
PIP ?= $(PYTHON) -m pip
LOCK_FILE ?= requirements.lock

LOCAL_POSTGRES_DSN ?= postgresql://risk_user:risk_password@localhost:5433/risk_platform
REVIEW_BASE_REF ?= origin/main
SYMBOL ?= IBM
PORTFOLIO_ID ?= us-tech-equal
PORTFOLIO_CONFIG ?= config/portfolios.yaml
RISK_LIMIT_POLICY_ID ?= us-tech-standard
RISK_LIMITS_CONFIG ?= config/portfolio_risk_limits.yaml
START_DATE ?=
END_DATE ?=
MAX_RECORDS ?= 100
VOL_WINDOW ?= 20
VAR_WINDOW ?= 60
VAR_CONFIDENCE ?= 0.95
COVARIANCE_WINDOW ?= 20
ATTRIBUTION_MAX_SNAPSHOTS ?= 2500
RISK_LIMIT_MAX_EVALUATIONS ?= 10000

.PHONY: setup lint type-check test dependency-check format benchmark-io docker-build k8s-render-dev k8s-render-prod k8s-check terraform-check infrastructure-check quality-check iteration-check clean-generated security-check readiness-check sandbox-once overnight-sandbox morning-review daily-risk-demo portfolio-risk-demo portfolio-attribution-demo portfolio-attribution-history-demo portfolio-risk-limits-demo warehouse-schema daily-risk-warehouse-dry-run daily-risk-warehouse-load check-daily-risk-consistency portfolio-risk-warehouse-dry-run portfolio-risk-warehouse-load check-portfolio-risk-consistency portfolio-attribution-warehouse-dry-run portfolio-attribution-warehouse-load check-portfolio-attribution-consistency portfolio-risk-limits-warehouse-dry-run portfolio-risk-limits-warehouse-load check-portfolio-risk-limits-consistency postgres-contract-fixture postgres-contract-check local-db-up local-db-down local-db-wait local-db-logs postgres-shell mongo-shell run-demo load-postgres-demo load-postgres-dry-run check-postgres-consistency consistency-demo

setup:
	python3 -m venv .venv
	PIP_CONSTRAINT=$(LOCK_FILE) $(PIP) install -e '.[dev]'

lint:
	$(PYTHON) -m ruff check .

type-check:
	$(PYTHON) -m mypy --package src

format:
	$(PYTHON) -m ruff check . --fix

test:
	$(PYTHON) -m pytest -q

dependency-check:
	$(PYTHON) -m pip check

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

quality-check: lint type-check test dependency-check

iteration-check: security-check infrastructure-check quality-check clean-generated run-demo load-postgres-dry-run

clean-generated:
	rm -rf data .demo .benchmarks .pytest_cache .mypy_cache .ruff_cache

security-check:
	$(PYTHON) scripts/security_check.py

readiness-check: quality-check clean-generated run-demo load-postgres-dry-run

sandbox-once:
	PYTHONUNBUFFERED=1 $(PYTHON) scripts/overnight_sandbox.py --cycles 1 --sleep-seconds 0

overnight-sandbox:
	PYTHONUNBUFFERED=1 $(PYTHON) scripts/overnight_sandbox.py --hours 8 --sleep-seconds 1800

morning-review:
	PYTHONUNBUFFERED=1 $(PYTHON) scripts/morning_review.py --base-ref "$(REVIEW_BASE_REF)"

daily-risk-demo:
	@set -eu; \
	if [ -z "$${ALPHA_VANTAGE_API_KEY:-}" ]; then \
		echo "ALPHA_VANTAGE_API_KEY must be set without printing its value" >&2; \
		exit 2; \
	fi; \
	end_date="$(END_DATE)"; \
	if [ -z "$$end_date" ]; then \
		end_date="$$($(PYTHON) -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat())')"; \
	fi; \
	start_args=""; \
	if [ -n "$(START_DATE)" ]; then start_args="--start-date $(START_DATE)"; fi; \
	$(PYTHON) -m src.orchestration.ingest_alpha_vantage_daily \
		--source alpha_vantage \
		--symbol "$(SYMBOL)" \
		--end-date "$$end_date" \
		--max-records "$(MAX_RECORDS)"; \
	$(PYTHON) -m src.orchestration.run_daily_risk \
		--symbol "$(SYMBOL)" \
		$$start_args \
		--end-date "$$end_date" \
		--vol-window "$(VOL_WINDOW)" \
		--var-window "$(VAR_WINDOW)" \
		--var-confidence "$(VAR_CONFIDENCE)" \
		--summary-json .demo/daily-risk-summary.json

portfolio-risk-demo:
	@set -eu; \
	end_date="$(END_DATE)"; \
	if [ -z "$$end_date" ]; then \
		end_date="$$($(PYTHON) -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat())')"; \
	fi; \
	start_args=""; \
	if [ -n "$(START_DATE)" ]; then start_args="--start-date $(START_DATE)"; fi; \
	$(PYTHON) -m src.orchestration.run_portfolio_risk \
		--portfolio-id "$(PORTFOLIO_ID)" \
		--portfolio-config "$(PORTFOLIO_CONFIG)" \
		$$start_args \
		--end-date "$$end_date" \
		--vol-window "$(VOL_WINDOW)" \
		--var-window "$(VAR_WINDOW)" \
		--var-confidence "$(VAR_CONFIDENCE)" \
		--summary-json .demo/portfolio-risk-summary.json

portfolio-attribution-demo:
	@set -eu; \
	end_date="$(END_DATE)"; \
	if [ -z "$$end_date" ]; then \
		end_date="$$($(PYTHON) -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat())')"; \
	fi; \
	$(PYTHON) -m src.orchestration.run_portfolio_attribution \
		--portfolio-id "$(PORTFOLIO_ID)" \
		--portfolio-config "$(PORTFOLIO_CONFIG)" \
		--end-date "$$end_date" \
		--covariance-window "$(COVARIANCE_WINDOW)" \
		--summary-json .demo/portfolio-attribution-summary.json

portfolio-attribution-history-demo:
	@set -eu; \
	end_date="$(END_DATE)"; \
	if [ -z "$$end_date" ]; then \
		end_date="$$($(PYTHON) -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat())')"; \
	fi; \
	start_args=""; \
	if [ -n "$(START_DATE)" ]; then start_args="--start-date $(START_DATE)"; fi; \
	$(PYTHON) -m src.orchestration.run_portfolio_attribution_history \
		--portfolio-id "$(PORTFOLIO_ID)" \
		--portfolio-config "$(PORTFOLIO_CONFIG)" \
		$$start_args \
		--end-date "$$end_date" \
		--covariance-window "$(COVARIANCE_WINDOW)" \
		--max-snapshots "$(ATTRIBUTION_MAX_SNAPSHOTS)" \
		--summary-json .demo/portfolio-attribution-history-summary.json

portfolio-risk-limits-demo:
	@set -eu; \
	end_date="$(END_DATE)"; \
	if [ -z "$$end_date" ]; then \
		end_date="$$($(PYTHON) -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat())')"; \
	fi; \
	start_args=""; \
	if [ -n "$(START_DATE)" ]; then start_args="--start-date $(START_DATE)"; fi; \
	$(PYTHON) -m src.orchestration.run_portfolio_risk_limits \
		--policy-id "$(RISK_LIMIT_POLICY_ID)" \
		--limits-config "$(RISK_LIMITS_CONFIG)" \
		--portfolio-config "$(PORTFOLIO_CONFIG)" \
		$$start_args \
		--end-date "$$end_date" \
		--max-evaluations "$(RISK_LIMIT_MAX_EVALUATIONS)" \
		--summary-json .demo/portfolio-risk-limits-summary.json

warehouse-schema: local-db-wait
	docker compose exec -T postgres psql -U risk_user -d risk_platform \
		< sql/postgres_schema.sql
	docker compose exec -T postgres psql -U risk_user -d risk_platform \
		< sql/portfolio_schema.sql
	docker compose exec -T postgres psql -U risk_user -d risk_platform \
		< sql/portfolio_attribution_schema.sql
	docker compose exec -T postgres psql -U risk_user -d risk_platform \
		< sql/portfolio_risk_limits_schema.sql

daily-risk-warehouse-dry-run:
	$(PYTHON) -m src.warehouse.postgres_loader --dry-run

daily-risk-warehouse-load: warehouse-schema
	$(PYTHON) -m src.warehouse.postgres_loader --dsn "$(LOCAL_POSTGRES_DSN)"

check-daily-risk-consistency:
	docker compose exec -T postgres psql -U risk_user -d risk_platform \
		< sql/daily_risk_consistency_checks.sql

portfolio-risk-warehouse-dry-run:
	$(PYTHON) -m src.warehouse.postgres_loader --dry-run

portfolio-risk-warehouse-load: warehouse-schema
	$(PYTHON) -m src.warehouse.postgres_loader --dsn "$(LOCAL_POSTGRES_DSN)"

check-portfolio-risk-consistency:
	docker compose exec -T postgres psql -U risk_user -d risk_platform \
		< sql/portfolio_risk_consistency_checks.sql

portfolio-attribution-warehouse-dry-run:
	$(PYTHON) -m src.warehouse.portfolio_attribution_loader --dry-run

portfolio-attribution-warehouse-load: warehouse-schema
	$(PYTHON) -m src.warehouse.postgres_loader --dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.portfolio_attribution_loader \
		--dsn "$(LOCAL_POSTGRES_DSN)"

check-portfolio-attribution-consistency:
	docker compose exec -T postgres psql -U risk_user -d risk_platform \
		< sql/portfolio_attribution_consistency_checks.sql

portfolio-risk-limits-warehouse-dry-run:
	$(PYTHON) -m src.warehouse.portfolio_risk_limits_loader --dry-run

portfolio-risk-limits-warehouse-load: warehouse-schema
	$(PYTHON) -m src.warehouse.postgres_loader --dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.portfolio_attribution_loader \
		--dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.portfolio_risk_limits_loader \
		--dsn "$(LOCAL_POSTGRES_DSN)"

check-portfolio-risk-limits-consistency:
	docker compose exec -T postgres psql -U risk_user -d risk_platform \
		< sql/portfolio_risk_limits_consistency_checks.sql

postgres-contract-fixture:
	$(PYTHON) -m src.orchestration.build_postgres_contract_fixture \
		--summary-json .demo/postgres-contract-fixture.json

postgres-contract-check:
	docker compose down -v --remove-orphans
	docker compose up -d postgres
	$(MAKE) local-db-wait
	$(PYTHON) -m src.warehouse.notification_delivery_lock_contract_check \
		--dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.postgres_consistency \
		--dsn "$(LOCAL_POSTGRES_DSN)" \
		--check sql/consistency_checks.sql
	$(MAKE) clean-generated
	$(MAKE) postgres-contract-fixture
	$(MAKE) warehouse-schema
	$(PYTHON) -m src.warehouse.postgres_loader --dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.portfolio_attribution_loader \
		--dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.portfolio_risk_limits_loader \
		--dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.postgres_loader --dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.portfolio_attribution_loader \
		--dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.portfolio_risk_limits_loader \
		--dsn "$(LOCAL_POSTGRES_DSN)"
	$(PYTHON) -m src.warehouse.postgres_consistency \
		--dsn "$(LOCAL_POSTGRES_DSN)" \
		--check sql/daily_risk_consistency_checks.sql \
		--check sql/portfolio_risk_consistency_checks.sql \
		--check sql/portfolio_attribution_consistency_checks.sql \
		--check sql/portfolio_risk_limits_consistency_checks.sql
	$(PYTHON) -m src.warehouse.notification_retry_follow_up_postgres_contract_check \
		--dsn "$(LOCAL_POSTGRES_DSN)"

local-db-up:
	docker compose up -d postgres mongo

local-db-down:
	docker compose down -v

local-db-wait:
	@until docker compose exec -T postgres sh -c 'test "$$(cat /proc/1/comm)" = postgres && pg_isready -U risk_user -d risk_platform >/dev/null' >/dev/null 2>&1; do \
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
		--summary-json .demo/pipeline-summary.json \
		--lineage-json .demo/lineage.json

load-postgres-demo:
	$(PYTHON) -m src.warehouse.postgres_loader --dsn "$(LOCAL_POSTGRES_DSN)"

load-postgres-dry-run:
	$(PYTHON) -m src.warehouse.postgres_loader --dry-run

check-postgres-consistency:
	docker compose exec -T postgres psql -U risk_user -d risk_platform < sql/consistency_checks.sql

consistency-demo: clean-generated run-demo local-db-wait load-postgres-demo check-postgres-consistency
