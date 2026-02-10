.PHONY: lint format typecheck test check install-hooks precommit

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
