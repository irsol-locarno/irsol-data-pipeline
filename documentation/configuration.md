# Configuration Reference

Canonical constants live in `src/irsol_data_pipeline/core/config.py`.

## Dataset and Folder Names

| Constant | Value |
|---|---|
| `RAW_DIRNAME` | `raw` |
| `REDUCED_DIRNAME` | `reduced` |
| `PROCESSED_DIRNAME` | `processed` |
| `CACHE_DIRNAME` | `_cache` |
| `SDO_CACHE_DIRNAME` | `_sdo_cache` |

## Processing Defaults

| Constant | Value | Meaning |
|---|---|---|
| `DEFAULT_MAX_DELTA` | `timedelta(hours=2)` | Default max measurement-flatfield time difference |
| `V_STOKES_CUTOFF` | `0.4` | V-Stokes filter threshold in auto-calibration |

## Output Suffixes

| Constant | Value |
|---|---|
| `CORRECTED_FITS_SUFFIX` | `_corrected.fits` |
| `ERROR_JSON_SUFFIX` | `_error.json` |
| `METADATA_JSON_SUFFIX` | `_metadata.json` |
| `FLATFIELD_CORRECTION_DATA_SUFFIX` | `_flat_field_correction_data.pkl` |
| `PROFILE_CORRECTED_PNG_SUFFIX` | `_profile_corrected.png` |
| `PROFILE_ORIGINAL_PNG_SUFFIX` | `_profile_original.png` |
| `SLIT_PREVIEW_PNG_SUFFIX` | `_slit_preview.png` |
| `SLIT_PREVIEW_ERROR_JSON_SUFFIX` | `_slit_preview_error.json` |
