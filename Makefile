.PHONY: lint test format docker-build k8s-render-dev k8s-render-prod

lint:
	ruff check .

format:
	ruff check . --fix

test:
	pytest -q

docker-build:
	docker build -t financial-risk-data-platform:local .

k8s-render-dev:
	kubectl kustomize deploy/kubernetes/overlays/dev

k8s-render-prod:
	kubectl kustomize deploy/kubernetes/overlays/prod
