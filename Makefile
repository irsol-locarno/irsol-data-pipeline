.PHONY: lint test help

help:
	@echo "Available targets:"
	@echo "  lint  - Run pre-commit checks"
	@echo "  test  - Run pytest with coverage"
	@echo "  prefect/dashboard - Start the Prefect dashboard"
	@echo "  prefect/reset - Reset the Prefect database"
	@echo "  prefect/serve-pipeline - Serve the pipeline using Prefect"
	@echo "  clean - Removes temporary python artifacts"

lint:
	uv run pre-commit run --all-files

test:
	uv run pytest --cov=src --cov-report=html --cov-report=term tests/

prefect/dashboard:
	uv run prefect server start

prefect/reset:
	uv run prefect server database reset

prefect/serve-pipeline:
	PREFECT_ENABLED=true uv run entrypoints/serve_pipeline.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete


.DEFAULT_GOAL := help
