# Web Asset Compatibility

This document describes the web asset compatibility subsystem, which converts and deploys PNG visualization outputs to web-accessible formats on SFTP-based asset servers.

## Overview

The web asset compatibility system enables integration between the IRSOL data pipeline and web-based visualization platforms. It:

1. **Discovers** measurements in the observation day's processed directory
2. **Identifies** which assets exist per measurement (profile plots and slit previews)
3. **Validates** which assets need deployment or update (by checking the remote file system)
4. **Converts** PNG images to JPEG format (configurable quality) into a staging area
5. **Deploys** JPEGs to a remote SFTP server (Piombo) for web consumption in a single batch upload

The system is transport-agnostic via a protocol abstraction (`RemoteFileSystem`), allowing different deployment backends.

## Why This Layer Exists

This layer exists to replace the previous script-based deployment model (`quick-look` and
`image-generator` cron jobs) with a pipeline-native compatibility stage.

The science pipeline already generates local PNGs, but existing public systems consume deployed JPGs
at fixed legacy paths. This compatibility layer makes that translation explicit and reliable.

It is required to preserve backward compatibility with consumers that already expect:

1. `img_quicklook/{observation}/{measurement}.jpg` for quicklook previews.
2. `img_data/{observation}/{measurement}.jpg` for slit context images.
3. Public URLs that must resolve before SVO publication references thumbnail URLs.

In short, this is not an optional export helper: it is a contract-preserving migration layer.

## Architecture

### Core Domain Layer

**Module:** `core.web_asset_compatibility`

The core layer contains the domain models and business logic:

```
src/irsol_data_pipeline/core/web_asset_compatibility/
├── models.py       # Domain types: WebAssetKind, WebAssetFolderName, WebAssetSource
├── discovery.py    # Measurement-centric discovery of PNG assets
└── conversion.py   # PNG → JPEG conversion via Pillow
```

#### Models

**`WebAssetKind`** (enum):
- `quicklook` — Corrected Stokes profile visualization
- `context` — Slit geometry context image (SDO overlay)

**`WebAssetFolderName`** (enum):
- `img_quicklook` — Legacy folder for quicklook assets
- `img_data` — Legacy folder for context assets

**`WebAssetSource`** (immutable Pydantic model):
Represents a single deployable PNG asset for a measurement:

```python
class WebAssetSource(BaseModel):
    kind: WebAssetKind
    observation_name: str          # e.g., "250101"
    measurement_name: str          # e.g., "5876_m01"
    source_path: Path              # Input PNG in processed/

    @property
    def remote_target_path(self) -> str:
        """Compute the POSIX remote path, e.g. img_quicklook/250101/5876_m01.jpg"""
```

#### Discovery

**`discover_measurement_names(processed_dir: Path) -> list[str]`**

Scans the `processed/` directory for all PNG files matching any known output suffix and returns a deduplicated, sorted list of measurement base names.

**`discover_assets_for_measurement(measurement_name, observation_name, processed_dir) -> list[WebAssetSource]`**

For a single measurement name, checks which asset PNGs exist on disk and returns a `WebAssetSource` for each one found.

**`discover_day_web_asset_sources(day: ObservationDay) -> list[WebAssetSource]`**

Combines the above two helpers: iterates all measurements in the day's processed directory, and collects all available assets. Returns a sorted list.

#### Conversion

**Function:** `convert_png_to_jpeg(source_path: Path, target_path: Path, jpeg_quality: int) -> None`

Validates quality level (1-95) and converts PNG to JPEG using Pillow:

```python
image_data = Image.open(source_path).convert("RGB")
image_data.save(target_path, format="JPEG", quality=jpeg_quality, optimize=True, progressive=False)
```

### Pipeline Layer

**Module:** `pipeline.web_asset_compatibility`

The main orchestration entry point is:

```python
def process_day_web_asset_compatibility(
    day: ObservationDay,
    remote_fs: RemoteFileSystem,
    jpeg_quality: int = 50,
    force_overwrite: bool = False,
) -> DayProcessingResult:
```

The pipeline runs in three sequential phases:

1. **Plan** — Iterate over each measurement in the day. For each measurement, identify
   available assets (context and quick-look). For each asset, compute its remote target
   path and check whether it already exists on the remote file system. Assets that already
   exist are skipped (unless `force_overwrite` is set). All others are queued for conversion.

2. **Convert** — Convert every queued asset PNG to JPEG inside a temporary staging
   directory. Each staged file is placed under
   `<staging_root>/<folder_name>/<day_name>/<measurement_name>.jpg`.

3. **Upload** — Upload all successfully converted assets to the remote file system in a
   single batch.

The `DayProcessingResult` accumulates `processed`, `skipped`, and `failed` counts along
with any error messages.

### Remote File System Protocol

**Module:** `core.remote_filesystem`

Transport-agnostic abstraction for remote file operations:

```python
class RemoteFileSystem(Protocol):
    def ensure_dir(self, remote_dir: str) -> None: ...
    def file_exists(self, remote_path: str) -> bool: ...
    def upload_file(self, local_path: str, remote_path: str) -> None: ...
```

### Integration Layer

**Module:** `integrations.piombo`

Concrete implementation for Piombo SFTP deployment:

```python
class SftpRemoteFileSystem:
    """Paramiko-based SFTP adapter implementing RemoteFileSystem."""

    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        base_path: str = "",
    ): ...
```

Configuration is read from Prefect Variables and Secrets:
- `piombo-hostname` — SFTP server
- `piombo-username` — Login username
- `piombo-base-path` — Base path on server (e.g., `/irsol_db/docs/web-site/assets`)

And from Prefect Secrets:
- `piombo-password` — Login password (stored as a Prefect Secret block)

## Workflows

### Full Scan (web-assets-compatibility-full)

Prefect flow: `publish_web_assets_for_root()`

1. Query Prefect Variables to resolve dataset root and Piombo credentials, and Prefect Secrets to resolve `piombo-password`
2. Scan dataset for all observation days
3. For each day, run `process_day_web_asset_compatibility`:
   a. Iterate measurements → identify assets → plan which to convert
   b. Convert PNGs → JPEGs in a staging directory
   c. Upload all converted JPEGs to Piombo in one batch
4. Aggregate results and report summary

### Daily Trigger (web-assets-compatibility-daily)

Prefect flow: `publish_web_assets_for_day(day_path: str)`

1. Construct `ObservationDay` from the provided path argument
2. Run `process_day_web_asset_compatibility` for the single day
3. Log success/failure per asset

## Configuration & Deployment

### Prefect Variables

| Variable | Type | Required | Example |
|----------|------|----------|---------|
| `piombo-hostname` | str | Yes | `piombo.example.com` |
| `piombo-username` | str | Yes | `web-deploy-user` |
| `piombo-base-path` | str | No | `/irsol_db/docs/web-site/assets` |
| `data-root-path` | str | Yes | `/data/observations` |

### Prefect Secrets

| Secret | Type | Required | Description |
|--------|------|----------|-------------|
| `piombo-password` | str | Yes | SFTP password stored as a Prefect Secret block |

### Scheduling

Schedule the `web-assets-compatibility-full` to run daily after other pipelines complete.

## Error Handling

- **Missing PNG sources** — Assets not present on disk are silently omitted during discovery
- **Conversion failures** — PNG malformed or incompatible; error recorded per asset, pipeline continues
- **SFTP upload failures** — Network error, authentication, or permissions; failure logged and counted
- **Validation errors** — Invalid JPEG quality or missing required config; fails fast before staging

All errors are recorded in `DayProcessingResult.errors` and reported to the caller.

## Testing

Tests are located in:

```
tests/unit/irsol_data_pipeline/core/web_asset_compatibility/
├── test_models.py       # Domain model validation, remote_target_path computation
├── test_discovery.py    # Measurement name discovery, per-measurement asset discovery
└── test_conversion.py   # PNG → JPEG conversion

tests/unit/irsol_data_pipeline/pipeline/
└── test_web_asset_compatibility.py  # Full pipeline orchestration (plan/convert/upload)
```

Test patterns:
- Mock the `RemoteFileSystem` protocol for unit tests
- Construct test PNG files in-memory via Pillow (do not use real `.dat` files)
- Validate that discovered assets match expected naming conventions
- Verify proper error handling when uploads fail

## Related Documentation

- [Architecture Overview](../overview/architecture.md) — high-level system design
- [Pipeline Overview](../pipeline/pipeline_overview.md) — processing stages
- [Prefect Integration](../pipeline/prefect_integration.md) — orchestration and flows
- [CLI Usage](../cli/cli_usage.md) — user-facing web asset commands
