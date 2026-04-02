# Installation

This guide covers installing the IRSOL Data Pipeline for both development and production use.

## Installation Modes

The IRSOL Data Pipeline can be installed in two ways depending on your use case:

### Python Package — programmatic development

Install `irsol-data-pipeline` when you need to import and use the library in your own Python
code, extend the pipeline, or contribute to its development.

```bash
pip install irsol-data-pipeline
# or, for development from source (see Development Installation below):
uv sync
```

### CLI Tool — running commands and Prefect orchestration

Install `irsol-data-pipeline-cli` when you want a standalone command-line tool that does **not**
need to be imported as a library. The CLI package:

- Wraps all pipeline functionalities behind the `idp` command (flat-field correction, slit
  images, plots, …).
- Manages and interacts with the Prefect orchestration server (`idp prefect …`).
- Serves Prefect flows locally (`idp prefect flows serve …`).
- Installs the pipeline as systemd services on production servers (`idp install service`).

The CLI package ships with pinned, production-tested dependencies (including a compatible Prefect
version) and follows a more conservative release cadence than the main library.

```bash
uv tool install irsol-data-pipeline-cli --no-cache-dir --python 3.10
```

> **Which one should I install?**  
> For day-to-day operations on the `sirius` production server, always install
> `irsol-data-pipeline-cli`.  For development or scripting use the `irsol-data-pipeline`
> package (or install from source as described below).

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| **Python** | ≥ 3.10, < 3.12 | Required runtime — `spectroflat` is incompatible with Python 3.12+ |
| **[uv](https://docs.astral.sh/uv/getting-started/installation/)** | Latest | Package manager (recommended) |

## Development Installation

Clone the repository and install in editable mode:

```bash
git clone https://github.com/irsol-locarno/irsol-data-pipeline.git
cd irsol-data-pipeline
# With uv (recommended)
uv sync
# With pip
pip install -e .
```

This installs the package with all dependencies in a virtual environment. The `idp` CLI command becomes available:

```bash
idp --version
idp info
```

### Make Targets

The project includes a `Makefile` for common development tasks:

| Target | Description |
|--------|-------------|
| `make lint` | Run pre-commit checks (formatting, linting) |
| `make test` | Run pytest with coverage reports |
| `make clean` | Remove cache files, logs, and coverage artifacts |

## Production Installation (CLI Tool)

[Install](https://docs.astral.sh/uv/getting-started/installation/) `uv` globally:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Install the CLI tool (note: `spectroflat` is incompatible with Python 3.12+, so Python 3.10 is recommended):

```bash
uv tool install irsol-data-pipeline-cli --no-cache-dir --python 3.10
```

Upgrade to the latest version:

```bash
uv tool upgrade irsol-data-pipeline-cli --no-cache-dir --python 3.10
```

After installation, the `idp` command is available globally:

```bash
idp --version
idp info
```

Optionally, install shell auto-completion:
```bash
idp --install-completion
source ~/.bashrc
```

> **Note:** `uv tool install <package>` installs Python CLI tools into isolated, managed
> environments, adding their executables to your `PATH` for global access. It prevents
> dependency conflicts and acts as a faster alternative to `pipx`.

### `idp` CLI overview

Once installed, the `idp` command provides the following top-level command groups:

| Command group | Description |
|---------------|-------------|
| `idp info` | Show runtime and operational information |
| `idp flat-field` | Apply flat-field and smile corrections |
| `idp slit-image` | Generate SDO/AIA slit context images |
| `idp plot` | Render Stokes profile and slit plots |
| `idp prefect` | Manage and interact with the Prefect server |
| `idp prefect flows serve` | Serve Prefect flows locally |
| `idp setup` | Configure Prefect client and server profiles |
| `idp install service` | Install pipeline as systemd services (requires root) |

See the [CLI Usage guide](../cli/cli_usage.md) for the full command reference.

## Dependencies

The pipeline relies on the following key dependencies:

### Scientific Computing

| Package | Purpose |
|---------|---------|
| `numpy` (< 2) | Array operations — pinned below 2 because `spectroflat` does not declare a numpy upper bound but its internals are incompatible with numpy 2.x |
| `scipy` (≥ 1.10) | Curve fitting, IDL file reading |
| `astropy` (≥ 5.0) | FITS I/O, coordinates, units |
| `sunpy` (≥ 5.0) | Solar coordinate transforms, Map objects |
| `matplotlib` (≥ 3.7) | Plotting and visualization |

### Domain-Specific

| Package | Purpose |
|---------|---------|
| `spectroflat` (≥ 2.1) | Flat-field and smile correction engine |
| `drms` (≥ 0.7) | JSOC Data Record Management System client |

### Infrastructure

| Package | Purpose |
|---------|---------|
| `prefect` (≥ 3.0) | Workflow orchestration (optional at runtime) |
| `pydantic` (≥ 2.0) | Data validation and domain models |
| `cyclopts` (≥ 4.8) | CLI framework |
| `loguru` (≥ 0.7) | Structured logging |

### JSOC Registration

To generate slit images, register an email with the [JSOC](http://jsoc.stanford.edu/) service for DRMS data queries. Then configure it as a Prefect variable or pass it via CLI arguments.

## Related Documentation

- [Quick Start](quickstart.md) — first steps with the pipeline
- [CLI Usage](../cli/cli_usage.md) — full command reference
- [Prefect Operations](../maintainer/prefect_operations.md) — production deployment guide for the `sirius` server
