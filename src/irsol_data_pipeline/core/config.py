from __future__ import annotations

import datetime

# Default maximum time delta between measurement and flat-field
DEFAULT_MAX_DELTA = datetime.timedelta(hours=2)

# Maximum allowed angular difference in degrees between the derotator position
# angles of a measurement and a flat-field for a valid association.  A flat-field
# whose position angle differs by more than this value (accounting for circular
# wrap-around) is excluded from consideration even if it satisfies the time-delta
# policy.  Set to a large value (e.g. 360) to disable the angle check entirely.
DEFAULT_MAX_ANGLE_DELTA: float = 5.0

# Dataset folder naming conventions
RAW_DIRNAME = "raw"
REDUCED_DIRNAME = "reduced"
PROCESSED_DIRNAME = "processed"
CACHE_DIRNAME = "_cache"


# Processed output filename suffix conventions
CORRECTED_FITS_SUFFIX = "_ffcorr.fits"
ERROR_JSON_SUFFIX = "_error.json"
METADATA_JSON_SUFFIX = "_metadata.json"
FLATFIELD_CORRECTION_DATA_SUFFIX = "_flat_field_correction_data.fits"
PROFILE_CORRECTED_PNG_SUFFIX = "_profile_corrected.png"
PROFILE_ORIGINAL_PNG_SUFFIX = "_profile_original.png"
CONVERTED_FITS_SUFFIX = "_noff.fits"
PROFILE_CONVERTED_PNG_SUFFIX = "_profile_converted.png"


# Slit image generation output suffixes
SLIT_PREVIEW_PNG_SUFFIX = "_slit_preview.png"
SLIT_PREVIEW_ERROR_JSON_SUFFIX = "_slit_preview_error.json"

# V Stokes intensity threshold for row filtering in auto-calibration
V_STOKES_CUTOFF = 0.4

# Default emaul used to access JSOC data
DEFAULT_JSOC_EMAIL = "info@irsol.usi.ch"

# Default delta days to access JSOC data
DEFAULT_JSOC_DATA_DELAY_DAYS = 14

# Default root path used by Piombo SFTP uploads for web assets.
DEFAULT_PIOMBO_BASE_PATH = "/irsol_db/docs/web-site/assets"

# Default host name for piombo host
DEFAULT_PIOMBO_HOST_NAME = "piombo7.usi.ch"

# Default user for piombo
DEFAULT_PIOMBO_USERNAME = "leporan@usi.ch"
