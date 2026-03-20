.PHONY: lint test help

help:
	@echo "Available targets:"
	@echo "  lint  - Run pre-commit checks"
	@echo "  test  - Run pytest with coverage"
	@echo "  prefect/dashboard - Start the Prefect dashboard"
	@echo "  prefect/configure - Configure Prefect Variables through the unified CLI"
	@echo "  prefect/reset - Reset the Prefect database"
	@echo "  prefect/serve-flat-field-correction-pipeline - Serve flat-field correction flow group"
	@echo "  prefect/serve-slit-image-pipeline - Serve slit image generation flow group"
	@echo "  prefect/serve-maintenance-pipeline - Serve maintenance flow group"
	@echo "  clean - Removes temporary python artifacts"

lint:
	uv run pre-commit run --all-files

test:
	uv run pytest --cov=src --cov-report=html --cov-report=term --cov-report=xml:coverage.xml tests/

prefect/dashboard:
	uv run idp prefect start

prefect/configure:
	uv run idp prefect variables configure

prefect/reset:
	uv run idp prefect reset-database

prefect/serve-maintenance-pipeline:
	uv run idp prefect flows serve maintenance

prefect/serve-flat-field-correction-pipeline:
	uv run idp prefect flows serve flat-field-correction

prefect/serve-slit-image-pipeline:
	uv run idp prefect flows serve slit-images

clean:
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete || true
	@rm coverage.xml 2>/dev/null || true
	@rm *.log 2>/dev/null || true


.DEFAULT_GOAL := help
