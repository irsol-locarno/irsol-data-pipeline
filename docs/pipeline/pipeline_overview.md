# Pipeline Overview

The pipeline layer orchestrates end-to-end processing of solar observation data. It ties together the core algorithms, IO modules, and caching logic into cohesive workflows that process individual measurements, entire observation days, or full dataset scans.

## Flat-field correction

### Happy path (flat-field correction succeeds)

```mermaid
flowchart TD
    SCAN["<b>1. Scan</b><br/>Discover observation days<br/>and pending measurements"]
    DISCOVER["<b>2. Discover</b><br/>Find measurement & flat-field<br/>files in reduced/"]
    CACHE["<b>3. Build Cache</b><br/>Analyze flat-fields<br/>(or load from disk cache)"]
    PROCESS["<b>4. Process Measurement</b><br/>(8-step pipeline)"]
    RESULT["<b>5. Collect Results</b><br/>Processed / skipped / failed"]

    SCAN --> DISCOVER --> CACHE --> PROCESS --> RESULT

    subgraph "Step 4: Per-Measurement Pipeline (happy path)"
        direction TB
        P1["4.1 Load .dat file"]
        P2["4.2 Find closest flat-field"]
        P3["4.3 Apply flat-field correction"]
        P4["4.4 Wavelength auto-calibration"]
        P5["4.5 Write corrected FITS<br/><i>*_corrected.fits</i><br/>FFCORR=True,<br/>FFFILE=&lt;filename&gt;"]
        P6["4.6 Write correction data (FITS)"]
        P7["4.7 Write processing metadata (JSON)"]
        P8["4.8 Generate profile plots (PNG)<br/><i>*_profile_original.png</i><br/><i>*_profile_corrected.png</i>"]

        P1 --> P2 --> P3 --> P4 --> P5 --> P6 --> P7 --> P8
    end

    PROCESS -.-> P1
```

### Non-happy path (flat-field correction fails)

When flat-field correction fails, an `*_error.json` artifact is always written and a `*_profile_original.png` profile plot is always attempted from the raw (uncorrected) Stokes data.
If the `convert_on_ff_failure` option is enabled, the pipeline additionally
converts the raw measurement to a FITS file and generates a second profile plot so
downstream consumers can still access the Stokes data.

```mermaid
flowchart TD
    LOAD["4.1 Load .dat file"]
    FAIL["4.2 Flat-field correction<br/>FAILS"]
    ERROR["Write *_error.json"]
    PLOT_ORIG["Write *_profile_original.png<br/>(best-effort, always)"]
    CONVERT_CHECK{{"convert_on_ff_failure<br/>enabled?"}}
    CALIBRATE["Best-effort wavelength<br/>auto-calibration"]
    CONVERT["Write *_converted.fits<br/>FFCORR=False"]
    PLOT_CONV["Write *_profile_converted.png"]
    SKIP["Skip conversion"]

    LOAD --> FAIL --> ERROR --> PLOT_ORIG --> CONVERT_CHECK
    CONVERT_CHECK -- "Yes" --> CALIBRATE --> CONVERT --> PLOT_CONV
    CONVERT_CHECK -- "No" --> SKIP
```

#### Output file naming

| Scenario | FITS file | Profile plot(s) | `FFCORR` header |
|----------|-----------|-----------------|-----------------|
| Correction *succeeded* | `*_corrected.fits` | `*_profile_original.png`, `*_profile_corrected.png` | `True` |
| Correction *failed* | — | `*_profile_original.png` *(best-effort)* | — |
| Correction *failed*, `convert_on_ff_failure=True` | `*_converted.fits` | `*_profile_original.png` *(best-effort)*, `*_profile_converted.png` | `False` |

The `FFCORR` FITS keyword (boolean) and `FFFILE` keyword (flat-field filename,
only present when `FFCORR=True`) are written to the primary HDU header so that
consumers can unambiguously determine whether the Stokes data has been
flat-field corrected and, if so, by which flat-field file.

### Stage 1 — Dataset Scanning

- Discovers all observation day directories under the dataset root.
- For each day, compares measurements in `reduced/` against outputs in `processed/`.
- A measurement is considered **processed** if any of `*_corrected.fits`, `*_converted.fits`, or `*_error.json` exists.

### Stage 2 — File Discovery

- **Measurement files** match the pattern `<wavelength>_m<number>.dat` (e.g., `6302_m1.dat`).
- **Flat-field files** match the pattern `ff<wavelength>_m<number>.dat` (e.g., `ff6302_m1.dat`).
- Files prefixed with `cal` or `dark` are ignored.

### Stage 3 — Flat-Field Cache

For each flat-field `.dat` file:

1. Check if a cached correction FITS file exists on disk. If so, load it.
2. Otherwise, load the flat-field `.dat`, run `analyze_flatfield()`, and persist the result.
3. Add the `FlatFieldCorrection` to the in-memory `FlatFieldCache`, keyed by wavelength.

The cache applies a **three-filter association policy** to find the best match for a measurement:

```python
cache.find_best_correction(
    wavelength=6302,
    timestamp=measurement_time,
    position_angle=measurement.metadata.derotator.position_angle,
)
```

| Filter | Condition | Default |
|--------|-----------|---------|
| Wavelength | Exact integer match (Å) | — |
| Time delta | `abs(Δt) ≤ max_delta` | 2 hours |
| Angle | Circular `abs(Δangle) ≤ max_angle_delta` | 5° |

Among all eligible candidates the **temporally closest** one is returned.  If either the measurement or the flat-field has no recorded `position_angle`, the angle filter is skipped.

See [Flat-Field Association Policy](../core/flat_field_correction.md#flat-field-association-policy) for the full decision flow and configuration details.

### Stage 4 — Single Measurement Processing

The 8-step pipeline for each measurement (happy path):

| Step | Operation | Output |
|------|-----------|--------|
| 4.1 | Load `.dat` file | `StokesParameters` + `MeasurementMetadata` |
| 4.2 | Find closest flat-field correction | `FlatFieldCorrection` (from cache) |
| 4.3 | Apply dust-flat + smile correction | Corrected `StokesParameters` |
| 4.4 | Run wavelength auto-calibration | `CalibrationResult` |
| 4.5 | Write corrected FITS | `*_corrected.fits` with `FFCORR=True`, `FFFILE=<flat-field filename>` |
| 4.6 | Write flat-field correction data | `*_flat_field_correction_data.fits` |
| 4.7 | Write processing metadata | `*_metadata.json` |
| 4.8 | Generate profile plots | `*_profile_original.png`, `*_profile_corrected.png` |

If any step fails, an error JSON (`*_error.json`) is written and a `*_profile_original.png` is always attempted from the raw Stokes data so downstream consumers retain a quick-look image even for non-successful measurements.
When `convert_on_ff_failure=True`, a best-effort FITS conversion is also attempted (see [Non-happy path](#non-happy-path-flat-field-correction-fails)).

### Configuring `convert_on_ff_failure`

The option is exposed at every level of the stack:

**Prefect flows:**
```python
process_daily_unprocessed_measurements(
    day_path=Path("/data/2025/250101"),
    convert_on_ff_failure=True,
)
```

**CLI — single measurement:**
```bash
idp flat-field apply 6302_m1.dat --output-dir processed/ --convert-on-ff-failure
```

**CLI — observation day:**
```bash
idp flat-field apply-day 2025/250101 --convert-on-ff-failure
```


## Slit Image Generation

```mermaid
flowchart TD
        SCAN["<b>1. Scan</b><br/>Discover eligible observation days<br/>(JSOC delay filter)"]
        DISCOVER["<b>2. Discover</b><br/>Find measurement files<br/>in reduced/"]
        PREP["<b>3. Prepare Cache</b><br/>Resolve day SDO cache dir<br/>processed/_cache/sdo/"]
        PROCESS["<b>4. Generate Slit Preview</b><br/>(4-step pipeline)"]
        RESULT["<b>5. Collect Results</b><br/>Generated / skipped / failed"]

        SCAN --> DISCOVER --> PREP --> PROCESS --> RESULT

        subgraph "Step 4: Per-Measurement Pipeline"
                direction TB
                S1["4.1 Load .dat metadata"]
                S2["4.2 Compute slit geometry"]
                S3["4.3 Fetch SDO maps\n(use cache when available)"]
                S4["4.4 Render slit preview PNG"]

                S1 --> S2 --> S3 --> S4
        end

        PROCESS -.-> S1
```

### Stage 1 — Dataset Scanning

- Discovers observation days under the dataset root.
- Full-scan flow keeps only days older than or equal to `jsoc-data-delay-days` (inclusive).
- Daily flow processes exactly one `day_path`.

### Stage 2 — File Discovery

- Measurement inputs are discovered from `reduced/` using the same rules as flat-field processing.
- Files already considered complete are skipped.
- A measurement is considered **already handled** if either `*_slit_preview.png` or `*_slit_preview_error.json` exists.

### Stage 3 — SDO Cache

- Per-day cache directory is `processed/_cache/sdo/`.
- Downloaded SDO FITS files are reused across measurements for the same day.
- If no cached file is available, data is fetched via JSOC/DRMS using the configured `jsoc-email`.

### Stage 4 — Single Measurement Slit Processing

The 4-step slit image pipeline for each measurement:

| Step | Operation | Output |
|------|-----------|--------|
| 4.1 | Load measurement `.dat` and parse `MeasurementMetadata` | Metadata with solar pointing/time info |
| 4.2 | Compute slit geometry | Slit start/end geometry + `mu` context |
| 4.3 | Fetch SDO/AIA + SDO/HMI context maps | 6-map context set (from cache or download) |
| 4.4 | Render slit overlay figure | `*_slit_preview.png` |

If generation fails, an error file `*_slit_preview_error.json` is written and the measurement is marked as failed.


## Cache Cleanup

```mermaid
flowchart TD
        SCAN["<b>1. Scan</b><br/>Discover observation days"]
        DISPATCH["<b>2. Dispatch</b><br/>Run per-day cleanup"]
        FIND["<b>3. Find Cache</b><br/>Locate processed/_cache/"]
        FILTER["<b>4. Filter by Age</b><br/>Keep recent, mark stale"]
        DELETE["<b>5. Delete Stale Files</b><br/>Remove old cache files"]
        PRUNE["<b>6. Prune Empty Dirs</b><br/>Cleanup empty folders"]
        REPORT["<b>7. Build Report</b><br/>Per-day + total metrics"]

        SCAN --> DISPATCH --> FIND --> FILTER --> DELETE --> PRUNE --> REPORT
```

### Stage 1 — Dataset Scanning

- Discovers all observation days under the configured root.
- Retention is configured in hours (`cache-expiration-hours`, default `672`).

### Stage 2 — Per-Day Cache Discovery

- Resolves cache location as `processed/_cache/` for each day.
- If no cache directory exists, the day returns a zero-count cleanup result.

### Stage 3 — Stale-File Evaluation

- Computes a cutoff timestamp: `now - retention_hours`.
- Iterates cache files and compares each file's modification time against the cutoff.
- Files newer than cutoff are retained; older files are marked for deletion.

### Stage 4 — Deletion and Reporting

- Deletes stale cache files and tracks deleted bytes.
- Keeps counters for checked/deleted/skipped/failed files per day.
- Removes empty cache subdirectories after deletion.
- Produces a markdown cleanup report artifact with totals and per-day breakdown.


## Web Asset Compatibility

```mermaid
flowchart TD
        SCAN["<b>1. Scan</b><br/>Discover observation days"]
        DISCOVER["<b>2. Discover Assets</b><br/>Find PNG outputs in processed/"]
        PLAN["<b>3. Plan Deployment</b><br/>Map PNG to legacy JPG targets"]
        CHECK["<b>4. Check Remote</b><br/>Skip existing unless force_overwrite"]
        CONVERT["<b>5. Convert</b><br/>PNG to JPG in staging"]
        UPLOAD["<b>6. Upload</b><br/>Deploy JPGs to Piombo SFTP"]
        RESULT["<b>7. Collect Results</b><br/>Processed / skipped / failed"]

        SCAN --> DISCOVER --> PLAN --> CHECK --> CONVERT --> UPLOAD --> RESULT
```

### Stage 1 — Dataset Scanning

- Full flow (`web-assets-compatibility-full`) scans all observation days.
- Daily flow (`web-assets-compatibility-daily`) processes one day path.

### Stage 2 — Asset Discovery

- Scans `processed/` for generated PNG assets:
    - `*_profile_corrected.png` (quicklook — successful runs)
    - `*_profile_original.png` (quicklook fallback — failed runs; used when `*_profile_corrected.png` is absent)
    - `*_slit_preview.png` (context)
- For the quicklook kind, `*_profile_corrected.png` takes priority; `*_profile_original.png` is used only when the corrected plot is absent, ensuring exactly one quicklook asset per measurement.
- Builds `WebAssetSource` entries per measurement and asset kind.

### Stage 3 — Target Planning

- Each PNG is mapped to a legacy JPG target path:
    - quicklook: `img_quicklook/<observation_day>/<measurement>.jpg`
    - context: `img_data/<observation_day>/<measurement>.jpg`
- Day-level planning distinguishes upload candidates from already-existing remote assets.

### Stage 4 — Conversion and Deployment

- Converts selected PNG files to JPEG in a temporary staging directory.
- Uploads staged JPGs through the configured `RemoteFileSystem` implementation (Piombo SFTP in production).
- Existing remote files are skipped unless `force_overwrite=True`.

### Stage 5 — Result Aggregation

- Returns per-day `DayProcessingResult` counts (`processed`, `skipped`, `failed`).
- Conversion and upload failures are recorded and surfaced in flow logs and run summaries.


## Related Documentation
- [Flat-Field Correction](../core/flat_field_correction.md) — core correction algorithms
- [Wavelength Auto-Calibration](../core/wavelength_autocalibration.md) — calibration details
- [Slit Image Creation](../core/slit_image_creation.md) — slit geometry and SDO data
- [IO Modules](../io/io_modules.md) — file format details
- [Prefect Integration](prefect_integration.md) — orchestration of pipeline flows
