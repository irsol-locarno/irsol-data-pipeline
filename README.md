# IRSOL Data Pipeline

[![CI](https://github.com/irsol-locarno/irsol-data-pipeline/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/irsol-locarno/irsol-data-pipeline/actions/workflows/ci.yml)
[![Release](https://github.com/irsol-locarno/irsol-data-pipeline/actions/workflows/release.yml/badge.svg)](https://github.com/irsol-locarno/irsol-data-pipeline/actions/workflows/release.yml)

| Package    | Version |
| -------- | ------- |
| Library  | [![PyPI version](https://badge.fury.io/py/irsol-data-pipeline.svg?icon=si%3Apython)](https://badge.fury.io/py/irsol-data-pipeline)    |
| CLI | [![PyPI version](https://badge.fury.io/py/irsol-data-pipeline-cli.svg?icon=si%3Apython)](https://badge.fury.io/py/irsol-data-pipeline-cli) |

IRSOL Data Pipeline processes reduced ZIMPOL spectro-polarimetric observations and produces calibrated scientific outputs and operational artifacts.

This project is structured as a Python package with a command-line interface (CLI) and is orchestrated using Prefect for workflow management. The pipeline includes *core* data processing modules for __flat-field correction__, __wavelength auto-calibration__, __slit image generation__ and integration with the PIOMBO data server, as well as IO modules for handling various data formats. The outputs are designed to be compatible with web serving and include metadata for traceability.

The project can be installed as a simple python dependency and used programamtically.

The installed package also provides a CLI for common operations, including running the full pipeline, executing individual steps, and managing Prefect flows.

The following diagram illustrates the high-level data flow and module interactions within the IRSOL Data Pipeline:

```mermaid
flowchart LR
    DAT["Reduced ZIMPOL .dat files"]
    FF["Flat-field correction<br>FITS + metadata + profile PNGs"]
    SI["Slit image generation<br>SDO context PNGs"]
    WEB["Web Asset compatibility<br>FITS + PNGs + metadata for web serving"]
    MT["Maintenance<br>Prefect run cleanup + cache cleanup"]
    PF["Prefect UI<br>UI + schedules + manual runs"]

    DAT --> FF
    DAT --> SI
    PF -. serves .-> FF
    PF -. serves .-> SI
    PF -. serves .-> WEB
    PF -. serves .-> MT
    FF --> WEB
    SI --> WEB

    classDef data fill:#9191c0
    classDef flow fill:#c09191
    classDef service fill:#91a091

    class DAT data
    class FF flow
    class SI flow
    class WEB flow
    class MT flow
    class PF service
```

## Quick Start

```bash
uv tool install irsol-data-pipeline
idp --version
```

## Documentation

Please refer to the detailed [documentaton](docs/README.md)
