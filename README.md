# IRSOL Data Pipeline

## Overview

`irsol-data-pipeline` processes IRSOL ZIMPOL solar spectro-polarimetric measurements.
It discovers unprocessed observations, computes and reuses flat-field corrections,
applies those corrections to matching measurements, auto-calibrates wavelength,
and writes processed outputs plus metadata.

The project supports two operation styles:

- Local CLI processing (single day or single measurement).
- Prefect-orchestrated processing (scan and process multiple observation days).

Expected dataset layout:

```text
<root>/
	<year>/
		<day>/
			raw/
			reduced/
			processed/
```

Inside `reduced/`:

- Measurement files: `<wavelength>_m<id>.dat` (example: `6302_m1.dat`)
- Flat-field files: `ff<wavelength>_m<id>.dat` (example: `ff6302_m3.dat`)
- Ignored as measurements: files starting with `cal` or `dark`

## Installation (via UV)

### 1. Install `uv`

If needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Create and sync environment

From the repository root:

```bash
uv sync
```

### 3. Set dataset root

```bash
export SOLAR_PIPELINE_ROOT=/path/to/your/dataset/root
```

### 4. Run commands

Examples:

```bash
uv run solar-pipeline scan
uv run solar-pipeline process-day /path/to/<year>/<day>
uv run solar-pipeline process-measurement /path/to/file.dat
uv run solar-pipeline export-fits /path/to/file.dat
uv run solar-pipeline plot-stokes /path/to/file.dat --calibrate
```

Optional Make targets:

```bash
make lint
make test
```

## Architecture Overview

The codebase is split into focused layers.

### Core functionalities

- Domain models (`src/irsol_data_pipeline/core/`):
	- `Measurement`, `FlatField`, `FlatFieldCorrection`
	- `MeasurementMetadata`
	- `StokesParameters`
	- `CalibrationResult`
- I/O (`src/irsol_data_pipeline/io/`):
	- Read `.dat`/`.sav`/`.npz` (`dat_reader.py`)
	- Write corrected data (`dat_writer.py`)
	- Discover observation days/files (`filesystem.py`)
	- Persist metadata/error JSON (`metadata_store.py`)
	- Persist/load cached flat-field correction objects
- Flat-field analysis and correction:
	- Analyze flat-fields with `spectroflat` (`correction/analyzer.py`)
	- Build and query correction cache (`pipeline/flatfield_cache.py`)
	- Apply dust-flat and smile correction (`correction/corrector.py`)
- Wavelength auto-calibration:
	- Cross-correlate with bundled reference spectra and fit line positions
		(`calibration/autocalibrate.py`)
- Processing pipeline:
	- Scan pending measurements (`pipeline/scanner.py`)
	- Process one day / one measurement (`pipeline/day_processor.py`)
- Orchestration:
	- Prefect flows for dataset-wide and per-day processing
		(`orchestration/flows.py`)
	- Prefect-aware logging bridge (`orchestration/patch_logging.py`)
- CLI:
	- User-facing commands via Typer (`cli/main.py`)

### Processing pipeline

Per day, the processing behavior is:

1. Discovery: find measurement files in `reduced/` and skip already processed
	 measurements (`*_corrected.dat.npz` or `*_error.json` in `processed/`).
2. Flat-field analysis: build/load a cache of flat-field corrections per
	 wavelength.
3. Matching and correction: for each measurement, select the closest-time
	 flat-field with matching wavelength within `max_delta` and apply correction.
4. Auto-calibration: calibrate corrected Stokes spectra against reference data.
5. Write outputs: corrected data, correction payload, metadata, and per-file
	 error JSON when a measurement fails.

```mermaid
flowchart TD
		A[Dataset Root] --> B[Discovery Scan]
		B --> C{Pending measurements?}
		C -- No --> Z[Stop]
		C -- Yes --> D[Process observation day]

		D --> E[Discover measurement and flat-field files]
		E --> F[Build flat-field cache]
		F --> G[For each measurement]

		G --> H[Load measurement]
		H --> I[Find best flat-field by
        wavelength + nearest timestamp]
		I --> J{Match within max_delta?}

		J -- No --> K[Write *_error.json]
		J -- Yes --> L[Apply flat-field and smile]
		L --> M[Auto-calibrate wavelength]
		M --> N[Write *_corrected.dat.npz]
		N --> O[Write correction pickle
        *_flat_field_correction_data.pkl]
		O --> P[Write *_metadata.json]

		K --> Q[Continue with next measurement]
		P --> Q
		Q --> R{More measurements?}
		R -- Yes --> G
		R -- No --> S[Day summary result]
```

## Prefect Usage

### Create a deployment by serving a flow

The main flow is
`irsol_data_pipeline.orchestration.flows.process_unprocessed_measurements`.

Start a local Prefect server (if not already running):

```bash
uv run prefect server start
```

In another python module, enable Prefect and serve the flow as a deployment:

```py
# serve_pipeline.py
from irsol_data_pipeline.orchestration.flows import process_unprocessed_measurements as f

f.serve(name='irsol-process-unprocessed')"
```

Then serve the pipeline:
```bash
PREFECT_ENABLED=1 SOLAR_PIPELINE_ROOT=/data/mdata/pdata/irsol/zimpol uv run serve_pipeline.py
```

Notes:

- `PREFECT_ENABLED=1` activates Prefect decorators used in shared modules.
- `serve(...)` creates the deployment and keeps a worker process running.

### Invoking a flow via the UI

1. Open the Prefect UI (default): `http://127.0.0.1:4200`.
2. Go to `Deployments` and select `irsol-process-unprocessed`.
3. Click `Run` / `Quick Run`.
4. Optionally adjust parameters (for example `root`, `max_delta_hours`,
	 `refdata_dir`, `max_concurrency`).
5. Inspect run logs and artifacts:
	 - The scan summary is published as a markdown artifact.
	 - Each day processing run reports processed/skipped/failed counts.
