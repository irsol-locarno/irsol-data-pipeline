"""Import FITS measurement content into typed pipeline structures."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
from astropy.io import fits
from loguru import logger

from irsol_data_pipeline.core.models import CalibrationResult, StokesParameters
from irsol_data_pipeline.exceptions import FitsImportError


@dataclass(frozen=True)
class ImportedFitsMeasurement:
    """Typed representation of a measurement loaded from FITS."""

    stokes: StokesParameters
    calibration: Optional[CalibrationResult]
    header: fits.Header


def load_fits_measurement(fits_path: Path) -> ImportedFitsMeasurement:
    """Load Stokes profiles and optional wavelength calibration from FITS."""
    with logger.contextualize(path=fits_path):
        logger.debug("Loading FITS measurement")
        with fits.open(fits_path) as hdul:
            si_hdu = _get_hdu(hdul, "Stokes I", 1)
            sq_hdu = _get_hdu(hdul, "Stokes Q/I", 2)
            su_hdu = _get_hdu(hdul, "Stokes U/I", 3)
            sv_hdu = _get_hdu(hdul, "Stokes V/I", 4)

            header = si_hdu.header.copy()
            stokes = StokesParameters(
                i=_to_spatial_spectral(si_hdu.data),
                q=_to_spatial_spectral(sq_hdu.data),
                u=_to_spatial_spectral(su_hdu.data),
                v=_to_spatial_spectral(sv_hdu.data),
            )
            calibration = _extract_calibration(header)

        logger.debug(
            "Loaded FITS measurement",
            has_calibration=calibration is not None,
            shape_i=stokes.i.shape,
            shape_q=stokes.q.shape,
            shape_u=stokes.u.shape,
            shape_v=stokes.v.shape,
        )

    return ImportedFitsMeasurement(
        stokes=stokes,
        calibration=calibration,
        header=header,
    )


def _extract_calibration(header: fits.Header) -> Optional[CalibrationResult]:
    """Read calibration values from FITS headers when available."""
    wavecal_value = header.get("WAVECAL", 0)
    has_calibration = False

    if isinstance(wavecal_value, (int, float)):
        has_calibration = int(wavecal_value) == 1
    elif isinstance(wavecal_value, str):
        has_calibration = wavecal_value.strip() == "1"

    if not has_calibration:
        logger.debug("No FITS calibration metadata present (WAVECAL != 1)")
        return None

    a0 = _as_float(header.get("CRVAL3"))
    a1 = _as_float(header.get("CDELT3"))
    if a0 is None or a1 is None:
        logger.debug("Incomplete FITS calibration metadata", crval3=a0, cdelt3=a1)
        return None

    a1_err = _as_float(header.get("CRDER3"))
    a0_err = _as_float(header.get("CSYER3"))

    return CalibrationResult(
        pixel_scale=a1,
        wavelength_offset=a0,
        pixel_scale_error=a1_err if a1_err is not None else 0.0,
        wavelength_offset_error=a0_err if a0_err is not None else 0.0,
        reference_file="fits-header",
    )


def _get_hdu(hdul: fits.HDUList, extname: str, fallback_index: int) -> fits.ImageHDU:
    """Get a Stokes image extension by name, falling back to index."""
    for hdu in hdul:
        if isinstance(hdu, fits.ImageHDU) and hdu.header.get("EXTNAME") == extname:
            return hdu

    hdu = hdul[fallback_index]
    if not isinstance(hdu, fits.ImageHDU):
        raise FitsImportError(
            f"Expected ImageHDU at index {fallback_index} for {extname}"
        )
    return hdu


def _to_spatial_spectral(data: np.ndarray) -> np.ndarray:
    """Convert FITS image data to (spatial, spectral) arrays for plotting."""
    arr = np.asarray(data)
    arr = np.squeeze(arr)
    if arr.ndim != 2:
        raise FitsImportError(
            f"Expected 2D Stokes image after squeeze, got shape {arr.shape}"
        )
    return arr.T


def _as_float(value: object) -> Optional[float]:
    """Convert header value to float when possible."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
