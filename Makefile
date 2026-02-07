.PHONY: lint test format

lint:
	ruff check .

format:
	ruff check . --fix

test:
	pytest -q
