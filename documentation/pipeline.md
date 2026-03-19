# Pipelines

The repository implements three independent pipelines. Each runs against the same dataset root and writes its outputs into the `processed/` subdirectory of each observation day.

| Pipeline | Purpose | Schedule | Doc |
|---|---|---|---|
| **Flat-field correction** | Correct and calibrate raw Stokes `.dat` measurements → FITS | Daily at 01:00 | [pipeline-flat-field-correction.md](pipeline-flat-field-correction.md) |
| **Slit image generation** | Generate 6-panel SDO context images with slit overlay | Daily at 04:00 | [pipeline-slit-image-generation.md](pipeline-slit-image-generation.md) |
| **Prefect maintenance** | Delete old Prefect flow runs and stale cache `.pkl` files | Daily (00:00, 00:30) | [pipeline-maintenance.md](pipeline-maintenance.md) |

The flat-field correction and slit image generation pipelines are **independent**: they can run in any order and operate on the same input files without interfering with each other.

## Runtime configuration

Dynamic runtime parameters are managed through Prefect Variables, with optional per-run overrides from the UI/CLI. See [running.md](running.md) for the complete parameter resolution policy and managed variable names.

## Dataset layout

All pipelines expect the same dataset directory structure:

```text
<root>/
└── 2025/
    └── 20250312/            ← observation day
        ├── raw/             ← raw camera files (not read by this pipeline)
        ├── reduced/         ← input files for this pipeline
        │   ├── 6302_m1.dat  ← measurement (wavelength 6302 Å, measurement id 1)
        │   ├── 6302_m2.dat
        │   ├── ff6302_m1.dat ← flat-field for wavelength 6302 Å
        │   └── ff6302_m2.dat
        └── processed/       ← output directory (created by each pipeline)
```

**File naming conventions in `reduced/`:**
- Measurements: `<wavelength>_m<id>.dat` — e.g. `6302_m1.dat`
- Flat-fields: `ff<wavelength>_m<id>.dat` — e.g. `ff6302_m1.dat`
- Files starting with `cal` or `dark` are silently ignored.

## Outputs per measurement

All files are written into the `processed/` subdirectory of the observation day. For a source file `6302_m1.dat`:

### Flat-field correction outputs

| File | Description |
|---|---|
| `6302_m1_corrected.fits` | Multi-extension FITS with four Stokes images (I, Q/I, U/I, V/I) and calibration headers |
| `6302_m1_flat_field_correction_data.pkl` | Serialised `FlatFieldCorrection` payload |
| `6302_m1_metadata.json` | Processing summary: timestamps, flat-field used, calibration values |
| `6302_m1_profile_corrected.png` | Stokes profile plot after correction |
| `6302_m1_profile_original.png` | Stokes profile plot before correction |
| `6302_m1_error.json` | Written **only** on failure |

Flat-field cache files live under `processed/_cache/`:

| File | Description |
|---|---|
| `ff6302_m1_correction_cache.pkl` | Cached `FlatFieldCorrection` — reused across runs |

### Slit image generation outputs

| File | Description |
|---|---|
| `6302_m1_slit_preview.png` | 6-panel SDO context image with slit overlay |
| `6302_m1_slit_preview_error.json` | Written **only** on failure |

SDO FITS cache files live under `processed/_sdo_cache/`.

## Idempotency

Both scientific pipelines are **idempotent**: they skip measurements that already have their output files. To re-process a measurement, delete its output files from `processed/`.
