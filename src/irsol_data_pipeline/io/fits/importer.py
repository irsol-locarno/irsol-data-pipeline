"""Import FITS measurement content into typed pipeline structures."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
from astropy.io import fits
from loguru import logger

from irsol_data_pipeline.core.models import (
    CalibrationInfo,
    CalibrationResult,
    CameraInfo,
    DerotatorInfo,
    MeasurementMetadata,
    ReductionInfo,
    SolarOrientationInfo,
    SpectrographInfo,
    StokesParameters,
    TCUInfo,
)
from irsol_data_pipeline.exceptions import FitsImportError
from irsol_data_pipeline.io.fits.constants import (
    FITS_KEY_CAMPOS,
    FITS_KEY_DRANGL,
    FITS_KEY_DRCSYS,
    FITS_KEY_DROFFS,
    FITS_KEY_FFSTAT,
    FITS_KEY_GLBMEAN,
    FITS_KEY_GLBNOISE,
    FITS_KEY_GUIDST,
    FITS_KEY_IMGLST,
    FITS_KEY_IMGTYPE,
    FITS_KEY_IMGTYPX,
    FITS_KEY_IMGTYPY,
    FITS_KEY_INSTPF,
    FITS_KEY_LMGST,
    FITS_KEY_MODTYPE,
    FITS_KEY_PIGINT,
    FITS_KEY_PLCST,
    FITS_KEY_REDDCFL,
    FITS_KEY_REDDCFT,
    FITS_KEY_REDDMOD,
    FITS_KEY_REDFILE,
    FITS_KEY_REDMODE,
    FITS_KEY_REDNFIL,
    FITS_KEY_REDONAM,
    FITS_KEY_REDPIXR,
    FITS_KEY_REDROWS,
    FITS_KEY_REDSOFT,
    FITS_KEY_REDSTAT,
    FITS_KEY_REDTCUM,
    FITS_KEY_SBSEQLN,
    FITS_KEY_SBSEQNM,
    FITS_KEY_SEQLEN,
    FITS_KEY_SLTANGL,
    FITS_KEY_SOLAR_XY,
    FITS_KEY_SPALPH,
    FITS_KEY_SPGRTWL,
    FITS_KEY_SPORD,
    FITS_KEY_SPSLIT,
    FITS_KEY_STOKVEC,
    FITS_KEY_TCUMODE,
    FITS_KEY_TCUPOSN,
    FITS_KEY_TCURTRN,
    FITS_KEY_TCURTRP,
    FITS_KEY_ZCDESC,
    FITS_KEY_ZCFILE,
    FITS_KEY_ZCSOFT,
    FITS_KEY_ZCSTAT,
)


@dataclass(frozen=True)
class ImportedFitsMeasurement:
    """Typed representation of a measurement loaded from FITS."""

    stokes: StokesParameters
    calibration: Optional[CalibrationResult]
    header: fits.Header
    metadata: Optional[MeasurementMetadata]
    solar_orientation: Optional[SolarOrientationInfo]


def load_fits_measurement(fits_path: Path) -> ImportedFitsMeasurement:
    """Load Stokes profiles, optional wavelength calibration, and measurement
    metadata from FITS."""
    with logger.contextualize(path=fits_path):
        logger.debug("Loading FITS measurement")
        with fits.open(fits_path) as hdul:
            si_hdu = _get_hdu(hdul, "Stokes I", 1)
            sq_hdu = _get_hdu(hdul, "Stokes Q/I", 2)
            su_hdu = _get_hdu(hdul, "Stokes U/I", 3)
            sv_hdu = _get_hdu(hdul, "Stokes V/I", 4)

            header = si_hdu.header.copy()
            primary_header = hdul[0].header.copy()
            stokes = StokesParameters(
                i=_to_spatial_spectral(si_hdu.data),
                q=_to_spatial_spectral(sq_hdu.data),
                u=_to_spatial_spectral(su_hdu.data),
                v=_to_spatial_spectral(sv_hdu.data),
            )
            calibration = _extract_calibration(header)
            metadata = _extract_metadata(header, primary_header)
            solar_orientation = _extract_solar_orientation(primary_header, header)

        logger.debug(
            "Loaded FITS measurement",
            has_calibration=calibration is not None,
            has_metadata=metadata is not None,
            has_solar_orientation=solar_orientation is not None,
            shape_i=stokes.i.shape,
            shape_q=stokes.q.shape,
            shape_u=stokes.u.shape,
            shape_v=stokes.v.shape,
        )

    return ImportedFitsMeasurement(
        stokes=stokes,
        calibration=calibration,
        header=header,
        metadata=metadata,
        solar_orientation=solar_orientation,
    )


def _extract_metadata(
    header: fits.Header,
    primary_header: Optional[fits.Header] = None,
) -> Optional[MeasurementMetadata]:
    """Build a MeasurementMetadata from FITS header fields written by
    write_stokes_fits.

    ``header`` is the Stokes I data extension header, which contains the
    standard WCS/identification fields.  ``primary_header`` is the primary
    HDU header, which holds ``CAMTEMP``, ``SOLAR_P0``, and all the extended
    pipeline-specific metadata written by
    :func:`~irsol_data_pipeline.io.fits.exporter._fill_extended_metadata_primary_header`.

    Returns ``None`` when any required field is absent.
    """
    telescope_name = header.get("TELESCOP")
    instrument = header.get("INSTRUME")
    measurement_type = header.get("DATATYPE")
    measurement_id = header.get("POINT_ID")
    wavelength = header.get("WAVELNTH")
    name = header.get("MEASNAME")
    date_beg = header.get("DATE-BEG")

    required = {
        "TELESCOP": telescope_name,
        "INSTRUME": instrument,
        "DATATYPE": measurement_type,
        "POINT_ID": measurement_id,
        "WAVELNTH": wavelength,
        "MEASNAME": name,
        "DATE-BEG": date_beg,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        logger.debug(
            "FITS header missing required fields for MeasurementMetadata",
            missing_fields=missing,
        )
        return None

    date_end_raw = header.get("DATE-END")
    date_end = _as_str(date_end_raw)

    # CAMTEMP and SOLAR_P0 are in the primary HDU; fall back to the data header.
    camera_temp = _as_float(_from_primary_or_data(primary_header, header, "CAMTEMP"))
    solar_p0_val = _as_float(_from_primary_or_data(primary_header, header, "SOLAR_P0"))

    def _phdr(key: str) -> object:
        """Read *key* from the primary header with a data-header fallback."""
        return _from_primary_or_data(primary_header, header, key)

    # Build sub-models from the extended metadata written to the primary HDU.
    camera = CameraInfo(
        identity=_as_str(header.get("CAMERA")),
        ccd=_as_str(header.get("CCD")),
        temperature=camera_temp,
        position=_as_str(_phdr(FITS_KEY_CAMPOS)),
    )
    spectrograph = SpectrographInfo(
        alpha=_as_float(_phdr(FITS_KEY_SPALPH)),
        grtwl=_as_float(_phdr(FITS_KEY_SPGRTWL)),
        order=_as_int(_phdr(FITS_KEY_SPORD)),
        slit=_as_float(_phdr(FITS_KEY_SPSLIT)),
    )
    derotator = DerotatorInfo(
        coordinate_system=_as_int(_phdr(FITS_KEY_DRCSYS)),
        position_angle=_as_float(_phdr(FITS_KEY_DRANGL)),
        offset=_as_float(_phdr(FITS_KEY_DROFFS)),
    )
    tcu = TCUInfo(
        mode=_as_int(_phdr(FITS_KEY_TCUMODE)),
        retarder_name=_as_str(_phdr(FITS_KEY_TCURTRN)),
        retarder_wl_parameter=_as_str(_phdr(FITS_KEY_TCURTRP)),
        positions=_as_str(_phdr(FITS_KEY_TCUPOSN)),
    )
    reduction = ReductionInfo(
        software=_as_str(_phdr(FITS_KEY_REDSOFT)),
        status=_as_str(_phdr(FITS_KEY_REDSTAT)),
        file=_as_str(_phdr(FITS_KEY_REDFILE)),
        number_of_files=_as_int(_phdr(FITS_KEY_REDNFIL)),
        file_dc_used=_as_str(_phdr(FITS_KEY_REDDCFL)),
        dcfit=_as_str(_phdr(FITS_KEY_REDDCFT)),
        demodulation_matrix=_as_str(_phdr(FITS_KEY_REDDMOD)),
        order_of_rows=_as_str(_phdr(FITS_KEY_REDROWS)) or "",
        mode=_as_str(_phdr(FITS_KEY_REDMODE)),
        tcu_method=_as_str(_phdr(FITS_KEY_REDTCUM)),
        pixels_replaced=_as_str(_phdr(FITS_KEY_REDPIXR)),
        outfname=_as_str(_phdr(FITS_KEY_REDONAM)),
    )
    calibration_info = CalibrationInfo(
        software=_as_str(_phdr(FITS_KEY_ZCSOFT)),
        file=_as_str(_phdr(FITS_KEY_ZCFILE)),
        status=_as_str(_phdr(FITS_KEY_ZCSTAT)),
        description=_as_str(_phdr(FITS_KEY_ZCDESC)),
    )

    data: dict[str, object] = {
        "telescope_name": str(telescope_name),
        "instrument": str(instrument),
        "type": str(measurement_type),
        "id": _as_int(measurement_id),
        "wavelength": _as_int(wavelength),
        "name": str(name),
        "datetime_start": str(date_beg),
        "datetime_end": date_end,
        "observer": _as_str(header.get("OBSERVER")) or "",
        "project": _as_str(header.get("PROJECT")) or "",
        "integration_time": _as_float(header.get("TEXPOSUR")),
        "solar_p0": solar_p0_val,
        # Extended top-level fields
        "instrument_post_focus": _as_str(_phdr(FITS_KEY_INSTPF)),
        "modulator_type": _as_str(_phdr(FITS_KEY_MODTYPE)),
        "sequence_length": _as_int(_phdr(FITS_KEY_SEQLEN)),
        "sub_sequence_length": _as_int(_phdr(FITS_KEY_SBSEQLN)),
        "sub_sequence_name": _as_str(_phdr(FITS_KEY_SBSEQNM)),
        "stokes_vector": _as_str(_phdr(FITS_KEY_STOKVEC)),
        "images": _as_str(_phdr(FITS_KEY_IMGLST)) or "",
        "image_type": _as_str(_phdr(FITS_KEY_IMGTYPE)),
        "image_type_x": _as_str(_phdr(FITS_KEY_IMGTYPX)),
        "image_type_y": _as_str(_phdr(FITS_KEY_IMGTYPY)),
        "guiding_status": _as_int(_phdr(FITS_KEY_GUIDST)),
        "pig_intensity": _as_int(_phdr(FITS_KEY_PIGINT)),
        "solar_disc_coordinates": _as_str(_phdr(FITS_KEY_SOLAR_XY)),
        "limbguider_status": _as_int(_phdr(FITS_KEY_LMGST)),
        "polcomp_status": _as_int(_phdr(FITS_KEY_PLCST)),
        "flatfield_status": _phdr(FITS_KEY_FFSTAT),
        "global_noise": _as_str(_phdr(FITS_KEY_GLBNOISE)),
        "global_mean": _as_str(_phdr(FITS_KEY_GLBMEAN)),
        # Sub-models
        "camera": camera,
        "spectrograph": spectrograph,
        "derotator": derotator,
        "tcu": tcu,
        "reduction": reduction,
        "calibration": calibration_info,
    }

    try:
        return MeasurementMetadata.model_validate(data)
    except Exception:
        logger.exception("Failed to build MeasurementMetadata from FITS header")
        return None


def _extract_solar_orientation(
    primary_header: fits.Header,
    data_header: fits.Header,
) -> Optional[SolarOrientationInfo]:
    """Reconstruct SolarOrientationInfo from stored FITS header values.

    Returns ``None`` if the required keys are absent (e.g. older files written
    before extended metadata support was added).
    """
    # Read slit angle and sun_p0 directly from the FITS headers.
    slit_angle = _as_float(
        _from_primary_or_data(primary_header, data_header, FITS_KEY_SLTANGL)
    )
    sun_p0 = _as_float(_from_primary_or_data(primary_header, data_header, "SOLAR_P0"))
    if slit_angle is None or sun_p0 is None:
        return None

    # needs_rotation is recoverable from the derotator coordinate system.
    coord_system_raw = _from_primary_or_data(
        primary_header, data_header, FITS_KEY_DRCSYS
    )
    coord_system = _as_int(coord_system_raw)
    try:
        from irsol_data_pipeline.core.slit_images.config import (
            DEROTATOR_COORDINATE_SYSTEMS,
        )

        needs_rotation = (
            DEROTATOR_COORDINATE_SYSTEMS.get(coord_system, False)
            if coord_system is not None
            else False
        )
    except Exception:
        needs_rotation = False

    return SolarOrientationInfo(
        sun_p0_deg=sun_p0,
        slit_angle_solar_deg=slit_angle,
        needs_rotation=needs_rotation,
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


def _as_int(value: object) -> Optional[int]:
    """Convert a FITS header value to int when possible.

    FITS integer-typed header cards sometimes come back as Python ``float``
    (e.g. ``1.0``) due to how astropy parses certain card formats.  The
    conversion uses ``round()`` so that near-integer floats (``1.0``,
    ``2.0``, …) are handled correctly.  Returns ``None`` for ``None`` and
    for string values that cannot be parsed as integers.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _as_str(value: object) -> Optional[str]:
    """Convert a FITS header value to a non-empty stripped string, or None.

    Returns ``None`` when ``value`` is ``None`` or reduces to an empty string
    after stripping whitespace.  Accepts any FITS header value type (str,
    int, float, or None).
    """
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _from_primary_or_data(
    primary: Optional[fits.Header],
    data: fits.Header,
    key: str,
) -> object:
    """Look up a header key preferring the primary HDU, falling back to a data
    HDU.

    Some fields (e.g. ``CAMTEMP``, ``SOLAR_P0``) are written exclusively to
    the primary HDU by :func:`write_stokes_fits`.  This helper transparently
    falls back to the data extension header so that callers need not repeat
    the fallback logic for every such field.
    """
    if primary is not None:
        value = primary.get(key)
        if value is not None:
            return value
    return data.get(key)
