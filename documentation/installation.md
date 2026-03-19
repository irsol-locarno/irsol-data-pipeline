# Installation

## Prerequisites

- Python ≥ 3.10
- [`uv`](https://github.com/astral-sh/uv) package manager

Install `uv` if you don't have it:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Install For Development (Editable)

Use this path if you want to contribute to the repository.

```bash
# Clone the repository (if not already done)
git clone <repo-url>
cd irsol-data-pipeline

# Create a virtual environment and install project + all dev dependencies in editable mode
uv sync
```

With `uv sync`, the local package is installed from the working tree, so source code changes are immediately picked up without reinstalling.

All commands in this guide use `uv run <script>` which automatically uses the project virtual environment.

## Install From PyPI (As A Dependency)

Use this path when consuming the package in another project.

```bash
# In your target project
uv add irsol-data-pipeline
```

This installs `irsol-data-pipeline` from the Python Package Index (PyPI) like any other dependency.

Installed package commands:

```bash
irsol-configure
irsol-dashboard
irsol-serve-flat-field-correction
irsol-serve-slit-images
irsol-serve-maintenance
```

These commands are implemented in `src/irsol_data_pipeline/cli/`. Repository
files under `entrypoints/` are thin wrappers for development-time use from a
checkout.

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

These `make` targets are mainly for repository-local development. When the
project is installed as a package, use the `irsol-*` commands above instead.
