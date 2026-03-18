.PHONY: lint test help

help:
	@echo "Available targets:"
	@echo "  lint  - Run pre-commit checks"
	@echo "  test  - Run pytest with coverage"
	@echo "  prefect/dashboard - Start the Prefect dashboard"
	@echo "  prefect/reset - Reset the Prefect database"
	@echo "  prefect/serve-flat-field-correction-pipeline - Serve processing deployment"
	@echo "  prefect/serve-maintenance-pipeline - Serve maintenance deployment"
	@echo "  clean - Removes temporary python artifacts"

lint:
	uv run pre-commit run --all-files

test:
	uv run pytest --cov=src --cov-report=html --cov-report=term --cov-report=xml:coverage.xml tests/

prefect/setup:
	uv run prefect config set PREFECT_API_URL=http://localhost:4200/api
	uv run prefect config set PREFECT_SERVER_ANALYTICS_ENABLED=false

prefect/dashboard: prefect/setup
	uv run prefect server start

prefect/reset:
	uv run prefect server database reset

prefect/serve-maintenance-pipeline:
	PREFECT_ENABLED=true uv run entrypoints/serve_prefect_maintenance.py

prefect/serve-flat-field-correction-pipeline:
	PREFECT_ENABLED=true uv run entrypoints/serve_flat_field_correction_pipeline.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete


.DEFAULT_GOAL := help
