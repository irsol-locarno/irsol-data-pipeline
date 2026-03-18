# Configuration Reference

All shared constants are defined in `src/irsol_data_pipeline/core/config.py`:

| Name | Default | Description |
|---|---|---|
| `DEFAULT_MAX_DELTA` | `timedelta(hours=2)` | Default maximum allowed time between measurement and flat-field |
| `RAW_DIRNAME` | `"raw"` | Name of the raw data subdirectory inside an observation day |
| `REDUCED_DIRNAME` | `"reduced"` | Name of the reduced data subdirectory (pipeline input) |
| `PROCESSED_DIRNAME` | `"processed"` | Name of the processed output subdirectory |
| `CACHE_DIRNAME` | `"_cache"` | Name of the flat-field cache subdirectory inside `processed/` |
| `CORRECTED_FITS_SUFFIX` | `"_corrected.fits"` | Suffix for corrected FITS output files |
| `ERROR_JSON_SUFFIX` | `"_error.json"` | Suffix for error JSON files |
| `METADATA_JSON_SUFFIX` | `"_metadata.json"` | Suffix for processing metadata files |
| `FLATFIELD_CORRECTION_DATA_SUFFIX` | `"_flat_field_correction_data.pkl"` | Suffix for serialised correction payloads |
| `PROFILE_CORRECTED_PNG_SUFFIX` | `"_profile_corrected.png"` | Suffix for corrected Stokes profile plots |
| `PROFILE_ORIGINAL_PNG_SUFFIX` | `"_profile_original.png"` | Suffix for original Stokes profile plots |
| `V_STOKES_CUTOFF` | `0.4` | Threshold for filtering high-polarisation rows in auto-calibration |
