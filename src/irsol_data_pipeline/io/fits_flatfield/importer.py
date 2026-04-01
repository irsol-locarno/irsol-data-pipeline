"""Load ``FlatFieldCorrection`` payloads from FITS files.

The expected layout mirrors what ``write_correction_data`` produces:

- Primary HDU: header with provenance metadata (``SRCFFPTH``, ``WAVELEN``,
  ``TIMESTMP``, and optionally ``OMAPFILE``).
- ``DUSTFLAT`` image extension: normalised dust-flat correction array.
- ``DESMILED`` image extension: desmiled flat-field array.

When ``OMAPFILE`` is present in the primary header, the companion FITS file is
loaded using ``spectroflat.smile.OffsetMap.from_file()`` and the resulting
``OffsetMap`` is stored in ``FlatFieldCorrection.offset_map``.  If the key is
absent, ``offset_map`` is set to ``None``.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import numpy as np
from astropy.io import fits
from loguru import logger
from spectroflat.smile import OffsetMap

from irsol_data_pipeline.core.models import FlatFieldCorrection
from irsol_data_pipeline.exceptions import FlatfieldCorrectionImportError


def load_correction_data(path: Path | str) -> FlatFieldCorrection:
    """Load a flat-field correction from a FITS file.

    Args:
        path: Path to the main FITS file produced by ``write_correction_data``.
            A string path is also accepted and resolved to an absolute path.

    Returns:
        Deserialized FlatFieldCorrection.

    Raises:
        FlatfieldCorrectionImportError: On read or parsing failures.
    """
    resolved = Path(path).resolve()
    with logger.contextualize(path=resolved):
        logger.debug("Loading flat-field correction FITS")
        try:
            with fits.open(str(resolved)) as hdul:
                primary_hdr = hdul[0].header
                source_flatfield_path = Path(str(primary_hdr["SRCFFPTH"]))
                wavelength = int(primary_hdr["WAVELEN"])
                timestamp = datetime.datetime.fromisoformat(
                    str(primary_hdr["TIMESTMP"]),
                )
                position_angle_raw = primary_hdr.get("POSANGLE")
                position_angle: float | None = (
                    float(position_angle_raw)
                    if position_angle_raw is not None
                    else None
                )
                dust_flat = np.array(hdul["DUSTFLAT"].data, dtype=np.float64)
                desmiled = np.array(hdul["DESMILED"].data, dtype=np.float64)
                offset_map_filename: str | None = primary_hdr.get("OMAPFILE")
        except FlatfieldCorrectionImportError:
            raise
        except Exception as exc:
            raise FlatfieldCorrectionImportError(
                f"Failed to load FlatFieldCorrection from {resolved}: {exc}",
            ) from exc

        offset_map: OffsetMap | None = None
        if offset_map_filename is not None:
            offset_map_path = resolved.parent / offset_map_filename
            try:
                offset_map = OffsetMap.from_file(str(offset_map_path))
            except Exception as exc:
                raise FlatfieldCorrectionImportError(
                    f"Failed to load OffsetMap from {offset_map_path}: {exc}",
                ) from exc

        logger.debug("Loaded flat-field correction FITS")
        return FlatFieldCorrection(
            source_flatfield_path=source_flatfield_path,
            dust_flat=dust_flat,
            offset_map=offset_map,
            desmiled=desmiled,
            timestamp=timestamp,
            wavelength=wavelength,
            position_angle=position_angle,
        )
