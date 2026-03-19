import datetime

# Default maximum time delta between measurement and flat-field
DEFAULT_MAX_DELTA = datetime.timedelta(hours=2)

# Dataset folder naming conventions
RAW_DIRNAME = "raw"
REDUCED_DIRNAME = "reduced"
PROCESSED_DIRNAME = "processed"
CACHE_DIRNAME = "_cache"


# Processed output filename suffix conventions
CORRECTED_FITS_SUFFIX = "_corrected.fits"
ERROR_JSON_SUFFIX = "_error.json"
METADATA_JSON_SUFFIX = "_metadata.json"
FLATFIELD_CORRECTION_DATA_SUFFIX = "_flat_field_correction_data.pkl"
PROFILE_CORRECTED_PNG_SUFFIX = "_profile_corrected.png"
PROFILE_ORIGINAL_PNG_SUFFIX = "_profile_original.png"


# Slit image generation output suffixes
SLIT_PREVIEW_PNG_SUFFIX = "_slit_preview.png"
SLIT_PREVIEW_ERROR_JSON_SUFFIX = "_slit_preview_error.json"
SDO_CACHE_DIRNAME = "_sdo_cache"

# V Stokes intensity threshold for row filtering in auto-calibration
V_STOKES_CUTOFF = 0.4
