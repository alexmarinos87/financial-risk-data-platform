.PHONY: lint format typecheck test check install-hooks precommit run-pipeline backfill

lint:
	ruff check .

format:
	ruff check . --fix

typecheck:
	mypy src tests

test:
	python3 -m pytest -q

check: lint typecheck test

install-hooks:
	pre-commit install

precommit:
	pre-commit run --all-files

run-pipeline:
	python3 -m src.orchestration.run_pipeline --run-id local-demo

backfill:
	python3 -m src.orchestration.backfill --start-date 2025-01-01 --end-date 2025-01-03 --allow-dq-breach
