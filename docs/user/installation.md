# Installation

This guide covers installing the IRSOL Data Pipeline for both development and production use.

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| **Python** | ≥ 3.10 | Required runtime |
| **[uv](https://github.com/astral-sh/uv)** | Latest | Package manager (recommended) |

## Development Installation

Clone the repository and install in editable mode:

```bash
git clone https://github.com/irsol-locarno/irsol-data-pipeline.git
cd irsol-data-pipeline
uv sync
```

This installs the package with all dependencies in a virtual environment managed by `uv`. The `idp` CLI command becomes available:

```bash
uv run idp --version
uv run idp info
```

### Make Targets

The project includes a `Makefile` for common development tasks:

| Target | Description |
|--------|-------------|
| `make lint` | Run pre-commit checks (formatting, linting) |
| `make test` | Run pytest with coverage reports |
| `make clean` | Remove cache files, logs, and coverage artifacts |

### Prefect Development Targets

| Target | Description |
|--------|-------------|
| `make prefect/dashboard` | Start the Prefect dashboard server |
| `make prefect/configure` | Configure Prefect variables interactively |
| `make prefect/reset` | Reset the Prefect database (destructive) |
| `make prefect/serve-flat-field-correction-pipeline` | Serve flat-field correction flows |
| `make prefect/serve-slit-image-pipeline` | Serve slit image generation flows |
| `make prefect/serve-maintenance-pipeline` | Serve maintenance flows |

## Production Installation

Install as a standalone tool using `uv`:

```bash
uv tool install irsol-data-pipeline
```

Upgrade to the latest version:

```bash
uv tool upgrade irsol-data-pipeline
```

After installation, the `idp` command is available globally:

```bash
idp --version
idp info
```

> Note: `uv tool install <package>` installs Python CLI tools into isolated, managed environments, adding their executables to your `PATH` for global access. It ensures tools have dedicated environments to prevent dependency conflicts, acting as a faster alternative to `pipx`

## Dependencies

The pipeline relies on the following key dependencies:

### Scientific Computing

| Package | Purpose |
|---------|---------|
| `numpy` (< 2) | Array operations |
| `scipy` (≥ 1.10) | Curve fitting, IDL file reading |
| `astropy` (≥ 5.0) | FITS I/O, coordinates, units |
| `sunpy` (≥ 5.0) | Solar coordinate transforms, Map objects |
| `matplotlib` (≥ 3.7) | Plotting and visualization |

### Domain-Specific

| Package | Purpose |
|---------|---------|
| `spectroflat` (≥ 2.1) | Flat-field and smile correction engine |
| `qollib` (≥ 0.1) | Supporting library for spectroflat |
| `drms` (≥ 0.7) | JSOC Data Record Management System client |

### Infrastructure

| Package | Purpose |
|---------|---------|
| `prefect` (≥ 3.0) | Workflow orchestration (optional at runtime) |
| `pydantic` (≥ 2.0) | Data validation and domain models |
| `cyclopts` (≥ 4.8) | CLI framework |
| `loguru` (≥ 0.7) | Structured logging |

## Environment Setup

### Prefect (Optional)

To use Prefect orchestration, set the environment variable before running:

```bash
export PREFECT_ENABLED=1
```

Without this variable, all `@task` and `@flow` decorators are transparent no-ops, and the pipeline runs as plain Python.

### Prefect Server

For scheduled flows and the web dashboard, start the Prefect server:

```bash
# Via make (development)
make prefect/dashboard

# Or directly
idp prefect start
```

> **Note:** `idp prefect start` automatically configures the Prefect API URL and analytics settings before launching the server — no separate configuration step is required.

### JSOC Registration

To generate slit images, register an email with the [JSOC](http://jsoc.stanford.edu/) service for DRMS data queries. Then configure it as a Prefect variable or pass it via CLI arguments.

## Verifying the Installation

```bash
# Check version
idp --version

# Show runtime info
idp info

# Check Prefect status (if server is running)
idp prefect status
```

## Related Documentation

- [Quick Start](quickstart.md) — first steps with the pipeline
- [CLI Usage](../cli/cli_usage.md) — full command reference
- [Prefect Operations](../maintainer/prefect_operations.md) — production deployment guide
