"""Persist ``FlatFieldCorrection`` payloads as FITS files.

The main FITS file contains three HDUs:

- Primary HDU: header with provenance metadata (source path, wavelength, timestamp).
- ``DUSTFLAT`` image extension: the normalised dust-flat correction array.
- ``DESMILED`` image extension: the desmiled flat-field array.

When the ``offset_map`` attribute is a spectroflat ``OffsetMap`` object, a companion
FITS file ``<stem>_offset_map.fits`` is written alongside the main file using the
``OffsetMap.dump()`` method, and the header key ``OMAPFILE`` records its basename.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from astropy.io import fits
from loguru import logger

from irsol_data_pipeline.core.models import FlatFieldCorrection
from irsol_data_pipeline.exceptions import FlatfieldCorrectionExportError

_OFFSET_MAP_SUFFIX = "_offset_map.fits"


def write_correction_data(
    output_path: Path | str,
    data: FlatFieldCorrection,
) -> Path:
    """Write a ``FlatFieldCorrection`` object to disk as a FITS file.

    The correction is stored in a multi-extension FITS file.  When
    ``data.offset_map`` is a spectroflat ``OffsetMap``, a companion file
    ``<stem>_offset_map.fits`` is written in the same directory.

    Args:
        output_path: Destination path for the main FITS file.  Parent
            directories are created automatically if they do not exist.
        data: FlatFieldCorrection to persist.

    Returns:
        The path written to.

    Raises:
        FlatfieldCorrectionExportError: On any write failure.
    """
    path = Path(output_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    with logger.contextualize(path=path):
        logger.debug("Writing flat-field correction FITS")
        try:
            offset_map_filename: str | None = None
            if data.offset_map is not None:
                offset_map_path = path.parent / f"{path.stem}{_OFFSET_MAP_SUFFIX}"
                offset_map_filename = offset_map_path.name
                data.offset_map.dump(str(offset_map_path))

            primary_hdr = fits.Header()
            primary_hdr["CONTENT"] = "Flatfield correction"
            primary_hdr["SRCFFPTH"] = str(data.source_flatfield_path)
            primary_hdr["WAVELEN"] = data.wavelength
            primary_hdr["TIMESTMP"] = data.timestamp.isoformat()
            if offset_map_filename is not None:
                primary_hdr["OMAPFILE"] = offset_map_filename

            dust_hdr = fits.Header()
            dust_hdr["EXTNAME"] = "DUSTFLAT"
            desmiled_hdr = fits.Header()
            desmiled_hdr["EXTNAME"] = "DESMILED"

            hdul = fits.HDUList(
                [
                    fits.PrimaryHDU(header=primary_hdr),
                    fits.ImageHDU(
                        data=np.asarray(data.dust_flat, dtype=np.float64),
                        header=dust_hdr,
                    ),
                    fits.ImageHDU(
                        data=np.asarray(data.desmiled, dtype=np.float64),
                        header=desmiled_hdr,
                    ),
                ]
            )
            hdul.writeto(str(path), overwrite=True)
        except Exception as exc:
            raise FlatfieldCorrectionExportError(
                f"Failed to write FlatFieldCorrection to {path}: {exc}"
            ) from exc

        logger.debug("Flat-field correction FITS written")
    return path
