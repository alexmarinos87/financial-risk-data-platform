.PHONY: lint test format benchmark-io docker-build k8s-render-dev k8s-render-prod

lint:
	ruff check .

format:
	ruff check . --fix

test:
	pytest -q

benchmark-io:
	python -m src.benchmarks.io_engine_benchmark --summary-json .benchmarks/io_engine/summary.json

docker-build:
	docker build -t financial-risk-data-platform:local .

k8s-render-dev:
	kubectl kustomize deploy/kubernetes/overlays/dev

k8s-render-prod:
	kubectl kustomize deploy/kubernetes/overlays/prod
