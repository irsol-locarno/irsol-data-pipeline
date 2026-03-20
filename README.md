# IRSOL Data Pipeline

[![CI](https://github.com/irsol-locarno/irsol-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/irsol-locarno/irsol-data-pipeline/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/irsol-data-pipeline.svg?icon=si%3Apython)](https://badge.fury.io/py/irsol-data-pipeline)

IRSOL Data Pipeline processes reduced ZIMPOL spectro-polarimetric observations and produces calibrated scientific outputs and operational artifacts.

The repository contains three independent pipelines over the same dataset root.

```mermaid
flowchart LR
    DAT["Reduced ZIMPOL .dat files"]
    FF["Flat-field correction\nFITS + metadata + profile PNGs"]
    SI["Slit image generation\nSDO context PNGs"]
    MT["Maintenance\nPrefect run cleanup + cache cleanup"]
    PF["Prefect prefect\nUI + schedules + manual runs"]

    DAT --> FF
    DAT --> SI
    PF -. serves .-> FF
    PF -. serves .-> SI
    PF -. serves .-> MT
```

## Quick Start

```bash
uv sync
uv run entrypoints/process_single_measurement.py /path/to/reduced/6302_m1.dat
```

For installation options (editable development install from a clone, or dependency install from PyPI with `uv add irsol-data-pipeline`), see [documentation/installation.md](documentation/installation.md).

## Documentation

Use this section as the canonical traversal path.

### 1. Getting Started

| Page | Purpose |
|---|---|
| [documentation/installation.md](documentation/installation.md) | Install dependencies, set up local environment, discover `make` targets |
| [documentation/concepts.md](documentation/concepts.md) | Domain vocabulary used in code and logs |
| [documentation/configuration.md](documentation/configuration.md) | Constants and filename conventions from `core/config.py` |

### 2. Architecture

| Page | Purpose |
|---|---|
| [documentation/architecture.md](documentation/architecture.md) | Module layout, layer boundaries, dependency direction |
| [documentation/library-usage.md](documentation/library-usage.md) | Use core/io/pipeline modules directly without Prefect |

### 3. Pipelines

| Page | Purpose |
|---|---|
| [documentation/pipeline.md](documentation/pipeline.md) | Cross-pipeline overview: inputs, outputs, idempotency, data layout |
| [documentation/pipeline-flat-field-correction.md](documentation/pipeline-flat-field-correction.md) | Flat-field correction pipeline behavior and outputs |
| [documentation/pipeline-slit-image-generation.md](documentation/pipeline-slit-image-generation.md) | Slit image generation behavior and outputs |
| [documentation/pipeline-maintenance.md](documentation/pipeline-maintenance.md) | Maintenance flows and cleanup behavior |

### 4. Operations

| Page | Purpose |
|---|---|
| [documentation/prefect-introduction.md](documentation/prefect-introduction.md) | What Prefect is, why it is used here, and a minimal flow/deployment tutorial |
| [documentation/running.md](documentation/running.md) | Single source of truth for run commands, runtime parameters, and Prefect Variables |
| [documentation/prefect-production.md](documentation/prefect-production.md) | Production serving model, monitoring, and lifecycle management |
| [documentation/ops-sirius-prefect.md](documentation/ops-sirius-prefect.md) | OPS onboarding and runbook for managing Prefect services on `sirius` |

### 5. Development

| Page | Purpose |
|---|---|
| [documentation/extending.md](documentation/extending.md) | Add new policies, outputs, and flows safely |
| [documentation/testing.md](documentation/testing.md) | Test strategy, conventions, and commands |
| [documentation/info_array.md](documentation/info_array.md) | Reference fields from `.dat` info arrays |
