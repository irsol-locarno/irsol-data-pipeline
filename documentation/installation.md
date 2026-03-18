# Installation

## Prerequisites

- Python ≥ 3.10
- [`uv`](https://github.com/astral-sh/uv) package manager

Install `uv` if you don't have it:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Set up the environment

```bash
# Clone the repository (if not already done)
git clone <repo-url>
cd irsol-data-pipeline

# Create a virtual environment and install all dependencies
uv sync
```

All commands in this guide use `uv run <script>` which automatically activates the virtual environment.

## Available Make targets

```bash
make help                                      # List all targets
make lint                                      # Run pre-commit checks
make test                                      # Run tests with coverage
make prefect/dashboard                         # Start the Prefect server + dashboard
make prefect/serve-flat-field-correction-pipeline  # Serve the processing deployments
make prefect/serve-maintenance-pipeline        # Serve the maintenance deployment
make prefect/reset                             # Reset the local Prefect database
make clean                                     # Remove __pycache__ and .pyc files
```
