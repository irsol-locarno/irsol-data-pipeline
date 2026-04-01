# Frequently Asked Questions

Common questions and answers for developing with and operating the IRSOL Data Pipeline.

## Installation Issues

### Should I use `uv` or `pip`?

We recommend **[uv](https://docs.astral.sh/uv/getting-started/installation/)** for both development and production use. `uv` is faster, manages virtual environments automatically, and is the officially supported package manager for this project.

| Scenario | Recommended tool | Command |
|----------|-----------------|---------|
| **Development** (editable install) | `uv` | `uv sync` |
| **Development** (editable install, alternative) | `pip` | `pip install -e .` |
| **Production** (standalone CLI tool) | `uv tool` | `uv tool install irsol-data-pipeline-cli --no-cache-dir --python 3.10` |

> **Note:** `pip install -e .` works for development, but `uv sync` is preferred because it automatically creates an isolated virtual environment and resolves dependencies from the lock file (`uv.lock`), ensuring reproducible installs.

### When should I use `irsol-data-pipeline-cli` vs `irsol-data-pipeline`?

Two packages are published to PyPI on each [release](./../../.github/workflows/release.yml). They contain the **same code** but differ in how their dependencies are declared:

| Aspect | `irsol-data-pipeline` | `irsol-data-pipeline-cli` |
|--------|----------------------|---------------------------|
| **Dependencies** | Flexible version ranges | All pinned to exact versions |
| **Target audience** | Developers, Python scripts | Sysadmins, production servers |
| **Install method** | `pip install` or `uv sync` (dev) | `uv tool install` (production) |
| **Use as a library** | ✅ Yes | ❌ No — will break dependency resolution |

**Rule of thumb:**

- Use **`irsol-data-pipeline`** when you are writing Python scripts that import from the library, or when developing locally.
- Use **`irsol-data-pipeline-cli`** when you only need the `idp` command-line tool in a production environment (e.g., on `sirius`). Install it as an isolated tool with `uv tool install` — **never** add it as a project dependency.

## How to Run Flat-Field Correction on a Specific Measurement

The pipeline's flat-field correction involves two stages: (1) **analyzing** a flat-field file to produce a dust-flat map and an offset map, and (2) **applying** that correction to a measurement's Stokes parameters.

### Minimal code example

```python
from pathlib import Path

from irsol_data_pipeline.core.correction.analyzer import analyze_flatfield
from irsol_data_pipeline.core.correction.corrector import apply_correction
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.io import fits as fits_io

# Paths to your files
measurement_path = Path("/path/to/measurement.dat")
flatfield_path = Path("/path/to/flatfield.dat")
output_path = Path("/path/to/output_corrected.fits")

# 1. Read the flat-field file and analyze it
ff_stokes, ff_metadata = dat_io.read(flatfield_path)
dust_flat, offset_map, desmiled = analyze_flatfield(ff_stokes.i)

# 2. Read the measurement to correct
stokes, metadata = dat_io.read(measurement_path)

# 3. Apply the flat-field and smile correction
corrected_stokes = apply_correction(
    stokes=stokes,
    dust_flat=dust_flat,
    offset_map=offset_map,
)

# 4. (Optional) Save the corrected data as a FITS file
fits_io.write(output_path, corrected_stokes, metadata)
```

### What happens under the hood

1. `analyze_flatfield(stokes_i)` runs the [spectroflat](https://github.com/irsol-locarno/spectroflat) `Analyser` on the flat-field Stokes I array and returns:
   - `dust_flat` — a 2D dust-flat correction map.
   - `offset_map` — a spectral-distortion (smile) offset map.
   - `desmiled` — the desmiled flat-field reference (mainly for diagnostics).

2. `apply_correction(stokes, dust_flat, offset_map)` first divides Stokes I by the dust flat, then desmiles all four Stokes parameters (I, Q, U, V) using the offset map.

For more details see [Flat-Field Correction](../core/flat_field_correction.md) and the [Pipeline Overview](../pipeline/pipeline_overview.md).

## How to Run Slit-Image Generation on a Specific Measurement

Slit-image generation produces a six-panel SDO context image showing the spectrograph slit position on the solar disc. It requires a [JSOC](http://jsoc.stanford.edu/) registered email to fetch SDO/AIA and SDO/HMI data.

### Minimal code example

```python
from pathlib import Path

from irsol_data_pipeline.core.slit_images.coordinates import compute_slit_geometry
from irsol_data_pipeline.core.slit_images.solar_data import fetch_sdo_maps
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.plotting import plot_slit

# Configuration
measurement_path = Path("/path/to/measurement.dat")
output_path = Path("/path/to/slit_preview.png")
jsoc_email = "your-email@example.com"  # must be registered with JSOC

# 1. Read measurement metadata
stokes, metadata = dat_io.read(measurement_path)

# 2. Compute slit geometry (position on the solar disc)
slit_geometry = compute_slit_geometry(metadata=metadata)

# 3. Fetch SDO images (AIA 1600/131/193/304, HMI continuum, HMI magnetogram)
maps = fetch_sdo_maps(
    start_time=slit_geometry.start_time,
    end_time=slit_geometry.end_time,
    jsoc_email=jsoc_email,
)

# 4. Render the 6-panel slit preview image
plot_slit(maps=maps, slit=slit_geometry, output_path=output_path)
```

### Optional parameters

- `use_limbguider=True` — attempt to use limbguider coordinates from the raw z3bd file.
- `offset_corrections=(dx, dy)` — apply an (x, y) correction in arcsec to the slit center.
- `angle_correction=0.5` — apply a correction in degrees to the derotator angle.
- `cache_dir=Path("/tmp/sdo_cache")` — cache downloaded SDO FITS files for faster re-runs.

For more details see [Slit Image Creation](../core/slit_image_creation.md).

## How to Create a New Release

Releases are automated via GitHub Actions. When a git tag matching `v*` is pushed, the [workflow](./../../.github/workflows/release.yml) builds and publishes both `irsol-data-pipeline` and `irsol-data-pipeline-cli` to PyPI.

### Steps

1. Navigate to the [GitHub repository](https://github.com/irsol-locarno/irsol-data-pipeline) → **Releases** → **Draft a new release**.
2. Select the branch to release from (usually `master`).
3. Create a new tag following semantic versioning (e.g., `v1.0.0`, `v2.3.1-alpha.1`).
4. Write release notes describing the changes.
5. Click **Publish release**.
6. Monitor the GitHub Actions workflow — it will automatically:
   - Build and publish `irsol-data-pipeline` (library, flexible dependencies) to PyPI.
   - Build and publish `irsol-data-pipeline-cli` (CLI tool, pinned dependencies) to PyPI.

### Semantic versioning

| Change type | Example | When to use |
|-------------|---------|-------------|
| MAJOR (`v2.0.0`) | Breaking API changes | Incompatible changes to the public API |
| MINOR (`v1.1.0`) | New features | Backwards-compatible new functionality |
| PATCH (`v1.0.1`) | Bug fixes | Backwards-compatible bug fixes |
| Pre-release (`v1.0.0-alpha.1`) | Testing | Unstable versions for testing before a final release |

For the full step-by-step guide with screenshots, see [Creating a Release](../maintainer/create_a_release.md).

## How to Install the Deployed Release on Sirius as a UV Tool

On the production server (`sirius`), install the CLI package as an isolated tool using `uv`:

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the CLI package (use Python 3.10 — required for spectroflat compatibility)
uv tool install irsol-data-pipeline-cli --no-cache-dir --python 3.10

# Verify installation
idp --version
idp info
```

To upgrade to the latest version:

```bash
uv tool upgrade irsol-data-pipeline-cli --no-cache-dir --python 3.10
```

> **Why `irsol-data-pipeline-cli`?** The CLI package ships with all dependencies pinned to exact versions, including a compatible Prefect version. This guarantees a reproducible environment on production servers. See [Prefect Operations](../maintainer/prefect_operations.md) for the complete production deployment guide.

After installation, configure Prefect and start the services:

```bash
# Configure the default Prefect profile for running the server
idp setup server

# Start the Prefect server
idp prefect start

# Configure variables and secrets
idp prefect variables configure
idp prefect secrets configure
```

## How Can I Access the Deployed Prefect Dashboard Locally?

The Prefect dashboard runs on `sirius` on port `4200`. Since it is not exposed to the public network, you need to use SSH port forwarding to access it from your local machine.

### Steps

1. Open an SSH tunnel from your local machine to `sirius`:

   ```bash
   ssh -L 4200:localhost:4200 <username>@sirius
   ```

   This forwards port `4200` on your local machine to port `4200` on `sirius`.

2. Open your web browser and navigate to:

   ```
   http://localhost:4200
   ```

3. The Prefect dashboard is now available with the following tabs:
   - **Deployments** — view registered deployments and their schedules.
   - **Flow Runs** — inspect completed, running, and failed runs.
   - **Tasks** — drill into individual task execution.
   - **Logs** — view Prefect-captured log output.

> **Prerequisite:** The Prefect server must be running on `sirius`. If it is not, ask a maintainer to start it with `idp prefect start` or check the systemd service status with `systemctl status irsol-prefect-server`.

For the full production operations guide, see [Prefect Operations](../maintainer/prefect_operations.md).

## Related Documentation

- [Installation](installation.md) — full installation guide
- [Quick Start](quickstart.md) — first steps with the pipeline
- [CLI Usage](../cli/cli_usage.md) — full command reference
- [Prefect Operations](../maintainer/prefect_operations.md) — production deployment and operations
- [Creating a Release](../maintainer/create_a_release.md) — release process with screenshots
