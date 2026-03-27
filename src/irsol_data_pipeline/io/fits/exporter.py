"""FITS export helpers for processed Stokes measurements.

Serializes in-memory ``StokesParameters`` + ``MeasurementMetadata`` into a
multi-extension FITS product with WCS, observatory metadata, and optional
wavelength calibration header values.

Note: the implementation of this export is taken from https://github.com/irsol-locarno/fits-generator/blob/master/datapipeline.py
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Optional

import numpy as np
from astropy import units as u
from astropy.coordinates import EarthLocation, SkyCoord
from astropy.io import fits
from astropy.time import Time
from loguru import logger
from sunpy.coordinates import frames, sun
from sunpy.coordinates.sun import angular_radius

from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MeasurementMetadata,
    SolarOrientationInfo,
    StokesParameters,
)
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
from irsol_data_pipeline.version import __version__

IRSOL_LOCATION = EarthLocation(
    lat=46.176906 * u.Unit("deg"),
    lon=8.788521 * u.Unit("deg"),
    height=503.4 * u.Unit("m"),
)


def write_stokes_fits(
    output_path: Path,
    stokes: StokesParameters,
    info: MeasurementMetadata,
    calibration: Optional[CalibrationResult],
    solar_orientation: Optional[SolarOrientationInfo],
) -> Path:
    """Write processed Stokes data to a FITS file.

    Args:
        output_path: Where to write the `.fits` file.
        stokes: Stokes data to serialize.
        info: Measurement metadata used to derive FITS headers.
        calibration: Optional precomputed wavelength calibration.
        solar_orientation: Optional pre-computed solar orientation.
            :attr:`~irsol_data_pipeline.core.models.SolarOrientationInfo.slit_angle_solar_deg`
            is stored in the primary HDU header so it can be recovered on
            re-import without re-computing P0.

    Returns:
        The path written to.
    """
    with logger.contextualize(output_path=output_path):
        logger.debug(
            "Writing Stokes FITS",
            has_calibration=calibration is not None,
            shape_i=stokes.i.shape,
            shape_q=stokes.q.shape,
            shape_u=stokes.u.shape,
            shape_v=stokes.v.shape,
        )

        hdu_list = _build_fits_hdu_list(
            stokes=stokes,
            info=info,
            calibration=calibration,
            solar_orientation=solar_orientation,
        )
        hdu_list.writeto(output_path, output_verify="ignore", overwrite=True)
        logger.debug("Stokes FITS written")
    return output_path


def _build_fits_hdu_list(
    stokes: StokesParameters,
    info: MeasurementMetadata,
    calibration: Optional[CalibrationResult],
    solar_orientation: Optional[SolarOrientationInfo],
) -> fits.HDUList:
    """Build a FITS HDU list from Stokes data and raw info metadata.

    Calibration metadata is only included when an explicit calibration
    object is provided.
    """
    a1, a0, a1_err, a0_err = _calibration_values(calibration)
    return _build_hdu_list(stokes, info, a1, a0, a1_err, a0_err, solar_orientation)


def _calibration_values(
    calibration: Optional[CalibrationResult],
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Resolve wavelength calibration values for FITS headers."""
    if calibration is None:
        logger.debug("No wavelength calibration provided for FITS export")
        return None, None, None, None

    return (
        calibration.pixel_scale,
        calibration.wavelength_offset,
        calibration.pixel_scale_error,
        calibration.wavelength_offset_error,
    )


def _build_hdu_list(
    stokes: StokesParameters,
    metadata: MeasurementMetadata,
    a1: Optional[float],
    a0: Optional[float],
    a1_err: Optional[float],
    a0_err: Optional[float],
    solar_orientation: Optional[SolarOrientationInfo],
) -> fits.HDUList:
    """Build a complete multi-extension FITS HDU list."""

    # Reshape data: (spatial, spectral) -> (1, spatial, spectral) for 3D WCS
    def reshape(data: np.ndarray) -> np.ndarray:
        out = np.expand_dims(data, axis=2)
        out = np.swapaxes(out, 0, 1)
        return out

    si = reshape(stokes.i)
    sq = reshape(stokes.q)
    su = reshape(stokes.u)
    sv = reshape(stokes.v)

    hdu_primary = fits.PrimaryHDU()
    si_hdu = fits.ImageHDU(si)
    sq_hdu = fits.ImageHDU(sq)
    su_hdu = fits.ImageHDU(su)
    sv_hdu = fits.ImageHDU(sv)

    _fill_primary_header(hdu_primary.header, metadata, solar_orientation)
    _fill_data_header(
        si_hdu.header,
        metadata,
        stokes.i,
        a1,
        a0,
        a1_err,
        a0_err,
        "Stokes I",
        "I",
        si_hdu,
    )
    _fill_data_header(
        sq_hdu.header,
        metadata,
        stokes.q,
        a1,
        a0,
        a1_err,
        a0_err,
        "Stokes Q/I",
        "Q",
        si_hdu,
    )
    _fill_data_header(
        su_hdu.header,
        metadata,
        stokes.u,
        a1,
        a0,
        a1_err,
        a0_err,
        "Stokes U/I",
        "U",
        si_hdu,
    )
    _fill_data_header(
        sv_hdu.header,
        metadata,
        stokes.v,
        a1,
        a0,
        a1_err,
        a0_err,
        "Stokes V/I",
        "V",
        si_hdu,
    )

    # Filenames
    title = _make_title(metadata)
    for hdu in [hdu_primary, si_hdu, sq_hdu, su_hdu, sv_hdu]:
        hdu.header["FILENAME"] = title
        _add_software_metadata(
            header=hdu.header,
            software_version=__version__,
        )

    # Checksums
    for hdu in [si_hdu, sq_hdu, su_hdu, sv_hdu]:
        hdu.add_datasum()
        hdu.add_checksum()
    hdu_primary.add_checksum()

    return fits.HDUList([hdu_primary, si_hdu, sq_hdu, su_hdu, sv_hdu])


def _fill_primary_header(
    header: fits.Header,
    metadata: MeasurementMetadata,
    solar_orientation: Optional[SolarOrientationInfo],
) -> None:
    """Fill the primary HDU header."""
    header["EXTNAME"] = ("PRIMARY", "Name of HDU")
    header["SOLARNET"] = (1, "file is solarnet compliant")
    header["OBJNAME"] = ("Sun", None)
    header["TIMESYS"] = ("UTC", "Timesystem used in file")
    header["DATE"] = (
        dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "Creation UTC date of FITS header",
    )
    header["ORIGIN"] = (
        "IRSOL, Locarno, Switzerland",
        "Location where FITS file has been created",
    )
    header["WAVEUNIT"] = (-10, "WAVELNTH in units 10^WAVEUNIT m = Angstrom")
    header["WAVEREF"] = "air"
    header["WL_ATLAS"] = (
        "An Atlas of the Spectrum of the Solar Photosphere, L. Wallace, K. Hinkle, and W. Livingston, National Optical Astronomy Observatories",
        "Reference for wavelength calibration",
    )
    header["PIXSIZEX"] = (22.5, "[micrometer], CCD pixel size x")
    header["PIXSIZEY"] = (90, "[micrometer], CCD pixel size y")

    header["CAMTEMP"] = (metadata.camera.temperature, "Camera temp in celsius")
    header["SOLAR_P0"] = (metadata.solar_p0, "Sun-Earth angle")
    if header["SOLAR_P0"] is None:
        t = Time(metadata.datetime_start)
        header["SOLAR_P0"] = sun.P(t).value

    _fill_extended_metadata_primary_header(header, metadata, solar_orientation)


def _fill_extended_metadata_primary_header(
    header: fits.Header,
    metadata: MeasurementMetadata,
    solar_orientation: Optional[SolarOrientationInfo],
) -> None:
    """Write extended measurement metadata to the primary HDU header.

    All pipeline-specific fields that are not part of the standard FITS WCS
    vocabulary are written here using the keys defined in
    :mod:`~irsol_data_pipeline.io.fits.constants`.  The primary HDU is used
    so that the metadata is written once per file rather than being repeated
    in each of the four Stokes extension headers.
    """
    # Top-level measurement fields
    header[FITS_KEY_INSTPF] = (metadata.instrument_post_focus, "Post-focus instrument")
    header[FITS_KEY_MODTYPE] = (metadata.modulator_type, "Modulator type")
    header[FITS_KEY_SEQLEN] = (metadata.sequence_length, "Sequence length")
    header[FITS_KEY_SBSEQLN] = (metadata.sub_sequence_length, "Sub-sequence length")
    header[FITS_KEY_SBSEQNM] = (metadata.sub_sequence_name, "Sub-sequence name")
    header[FITS_KEY_STOKVEC] = (metadata.stokes_vector, "Stokes vector")
    images_str = " ".join(str(n) for n in metadata.images) if metadata.images else None
    header[FITS_KEY_IMGLST] = (images_str, "Image counts (space-separated)")
    header[FITS_KEY_IMGTYPE] = (metadata.image_type, "Image type")
    header[FITS_KEY_IMGTYPX] = (metadata.image_type_x, "Image type X axis")
    header[FITS_KEY_IMGTYPY] = (metadata.image_type_y, "Image type Y axis")
    header[FITS_KEY_GUIDST] = (metadata.guiding_status, "Guiding system status")
    header[FITS_KEY_PIGINT] = (metadata.pig_intensity, "PIG intensity level")
    header[FITS_KEY_SOLAR_XY] = (
        metadata.solar_disc_coordinates,
        "Solar disc coordinates [arcsec] 'X Y'",
    )
    header[FITS_KEY_LMGST] = (metadata.limbguider_status, "Limb guider status")
    header[FITS_KEY_PLCST] = (
        metadata.polcomp_status,
        "Polarization compensator status",
    )

    # Camera
    header[FITS_KEY_CAMPOS] = (metadata.camera.position, "Camera position")

    # Spectrograph
    header[FITS_KEY_SPALPH] = (metadata.spectrograph.alpha, "Spectrograph alpha angle")
    header[FITS_KEY_SPGRTWL] = (metadata.spectrograph.grtwl, "Grating wavelength")
    header[FITS_KEY_SPORD] = (metadata.spectrograph.order, "Diffraction order")
    header[FITS_KEY_SPSLIT] = (metadata.spectrograph.slit, "Slit width in mm")

    # Derotator
    header[FITS_KEY_DRCSYS] = (
        metadata.derotator.coordinate_system,
        "Derotator coordinate system (0=solar, 1=equatorial)",
    )
    header[FITS_KEY_DRANGL] = (
        metadata.derotator.position_angle,
        "[deg] Derotator position angle",
    )
    header[FITS_KEY_DROFFS] = (metadata.derotator.offset, "Derotator offset")

    # TCU
    header[FITS_KEY_TCUMODE] = (metadata.tcu.mode, "TCU mode")
    header[FITS_KEY_TCURTRN] = (metadata.tcu.retarder_name, "TCU retarder name")
    header[FITS_KEY_TCURTRP] = (
        metadata.tcu.retarder_wl_parameter,
        "TCU retarder wavelength parameters",
    )
    header[FITS_KEY_TCUPOSN] = (metadata.tcu.positions, "TCU positions")

    # Reduction
    header[FITS_KEY_REDSOFT] = (metadata.reduction.software, "Reduction software")
    header[FITS_KEY_REDSTAT] = (metadata.reduction.status, "Reduction status")
    header[FITS_KEY_REDFILE] = (metadata.reduction.file, "Reduction input file")
    header[FITS_KEY_REDNFIL] = (
        metadata.reduction.number_of_files,
        "Number of reduced files",
    )
    header[FITS_KEY_REDDCFL] = (
        metadata.reduction.file_dc_used,
        "Dark correction file used",
    )
    header[FITS_KEY_REDDCFT] = (metadata.reduction.dcfit, "Dark current fit method")
    header[FITS_KEY_REDDMOD] = (
        metadata.reduction.demodulation_matrix,
        "Demodulation matrix",
    )
    rows_str = (
        " ".join(str(r) for r in metadata.reduction.order_of_rows)
        if metadata.reduction.order_of_rows
        else None
    )
    header[FITS_KEY_REDROWS] = (rows_str, "Order of rows (space-separated ints)")
    header[FITS_KEY_REDMODE] = (metadata.reduction.mode, "Reduction mode")
    header[FITS_KEY_REDTCUM] = (metadata.reduction.tcu_method, "TCU reduction method")
    header[FITS_KEY_REDPIXR] = (
        metadata.reduction.pixels_replaced,
        "Pixels replaced count",
    )
    header[FITS_KEY_REDONAM] = (
        metadata.reduction.outfname,
        "Reduction output filename",
    )

    # CalibrationInfo (ZIMPOL calibration — not wavelength CalibrationResult)
    header[FITS_KEY_ZCSOFT] = (metadata.calibration.software, "ZIMPOL cal. software")
    header[FITS_KEY_ZCFILE] = (metadata.calibration.file, "ZIMPOL cal. file")
    header[FITS_KEY_ZCSTAT] = (metadata.calibration.status, "ZIMPOL cal. status")
    header[FITS_KEY_ZCDESC] = (
        metadata.calibration.description,
        "ZIMPOL cal. description",
    )

    # Top-level flags
    header[FITS_KEY_FFSTAT] = (
        metadata.flatfield_status,
        "Flat-field correction status",
    )
    header[FITS_KEY_GLBNOISE] = (metadata.global_noise, "Global noise levels")
    header[FITS_KEY_GLBMEAN] = (metadata.global_mean, "Global mean values")

    # Solar orientation — compute if not explicitly supplied
    slit_angle = _resolve_slit_angle(solar_orientation)
    header[FITS_KEY_SLTANGL] = (
        slit_angle,
        "[deg] Slit angle in solar reference frame",
    )


def _resolve_slit_angle(
    solar_orientation: Optional[SolarOrientationInfo],
) -> Optional[float]:
    """Return slit_angle_solar_deg from *solar_orientation* or None."""
    if solar_orientation is not None:
        return solar_orientation.slit_angle_solar_deg
    return None


def _fill_data_header(
    header: fits.Header,
    metadata: MeasurementMetadata,
    data: np.ndarray,
    a1: Optional[float],
    a0: Optional[float],
    a1_err: Optional[float],
    a0_err: Optional[float],
    ext_name: str,
    stokes_name: str,
    si_hdu: fits.ImageHDU,
) -> None:
    """Fill a data extension HDU header with WCS and metadata."""
    header["EXTNAME"] = (ext_name, "Name of HDU")
    header["BTYPE"] = (
        f"phys.polarization.stokes.{stokes_name}",
        "Unified Content Descriptors",
    )
    header["BUNIT"] = ("ADU" if stokes_name == "I" else "Fractional pol.", "Data unit")
    header["SOLARNET"] = (1, "file is solarnet compliant")
    header["OBJNAME"] = ("Sun", None)
    header["TIMESYS"] = ("UTC", "Timesystem used in file")
    header["DATE"] = (
        dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "Creation UTC date of FITS header",
    )
    header["OBS_HDU"] = (1, "This HDU contains observational data")
    header["ORIGIN"] = (
        "IRSOL, Locarno, Switzerland",
        "Location where FITS file has been created",
    )

    # Observation times
    obs_time = Time(metadata.datetime_start)
    header["DATE-OBS"] = (obs_time.fits, "Start date/time of observation")
    header["DATE-BEG"] = (obs_time.fits, "Start date/time of observation")
    if metadata.datetime_end is not None:
        end_time = Time(metadata.datetime_end)
        header["DATE-END"] = (end_time.fits, "End date/time of observation")
    else:
        header["DATE-END"] = (None, "End date/time of observation")

    # Telescope and instrument
    header["TELESCOP"] = (metadata.telescope_name, "Telescope")

    telescope = metadata.telescope_name
    if telescope in ("IRSOL", "Gregory IRSOL"):
        header["OBSGEO-X"], header["OBSGEO-Y"], header["OBSGEO-Z"] = (
            (4372553.12987083, "[m] IRSOL location (ITRS)"),
            (676011.48111147, "[m] IRSOL location (ITRS)"),
            (4579249.177649, "[m] IRSOL location (ITRS)"),
        )
        y_scaling = 1.3
        slit = metadata.spectrograph.slit
        if slit is not None and slit != -1:
            header["SLIT_WID"] = (
                round(7.9 * slit, 3),
                f"[arcsec] ({slit} mm), slit width",
            )
        else:
            # default value if not given in infos
            header["SLIT_WID"] = (0.474, "[arcsec] (0.06 mm), slit width")
    elif telescope == "GREGOR":
        y_scaling = 1.0
        header["OBSGEO-X"], header["OBSGEO-Y"], header["OBSGEO-Z"] = (
            (5390388.83418317, "[m] GREGOR location (ITRS)"),
            (-1597803.41550836, "[m] GREGOR location (ITRS)"),
            (3007217.8184646, "[m] GREGOR location (ITRS)"),
        )
        header["SLIT_WID"] = (0.29, "[arcsec] (0.07mm), slit width")
    else:
        logger.warning(
            "WARNING: Could not identify telescope, falling back to GREGOR",
            telescope=header["TELESCOP"],
        )
        header["OBSGEO-X"], header["OBSGEO-Y"], header["OBSGEO-Z"] = (
            (5390388.83418317, "[m] GREGOR location (ITRS)"),
            (-1597803.41550836, "[m] GREGOR location (ITRS)"),
            (3007217.8184646, "[m] GREGOR location (ITRS)"),
        )
        y_scaling = 1.0  # arcsec/pixel
        header["SLIT_WID"] = (
            0.29,
            "[arcsec] (0.07mm), slit width",
        )  # fixed slit width for GREGOR

    header["INSTRUME"] = (metadata.instrument, "Observing instrument")
    header["DATATYPE"] = (metadata.type, "Type of measurement")
    header["POINT_ID"] = (metadata.id, "Measurement ID")
    header["OBSERVER"] = (metadata.observer, None)
    header["PROJECT"] = (metadata.project, None)
    header["MEASNAME"] = (metadata.name, "Name of measurement")

    # Exposure info
    if metadata.images:
        nsumexp = sum(metadata.images)
    else:
        nsumexp = None
    header["NSUMEXP"] = (nsumexp, "Number of summed exposures")

    if metadata.integration_time is not None:
        header["TEXPOSUR"] = (metadata.integration_time, "[s] single exposure time")
    else:
        header["TEXPOSUR"] = (None, "[s] single exposure time")

    if metadata.integration_time is not None and nsumexp is not None:
        header["XPOSURE"] = (
            metadata.integration_time * nsumexp,
            "[s] total exposure time",
        )
    else:
        header["XPOSURE"] = (None, "[s] total exposure time")

    header["CAMERA"] = (metadata.camera.identity, "Camera identity")
    header["CCD"] = (metadata.camera.ccd, "Camera sensor identity")
    header["WAVELNTH"] = (metadata.wavelength, "Guideline Wavelength in Angstrom")

    # Data statistics
    _add_data_statistics(header, data)

    # WCS
    header["WCSNAME"] = "Helioprojective-cartesian"
    # TODO add CRDERn, stretch on error

    fabry_perrot = False  # TODO detect from infos if fabry perrot is used
    # with fabry perrot we have 2 spatial axis
    if fabry_perrot:
        x_scaling = 0.325  # zimpol camera intensity scaling
        header["CNAME1"] = metadata.image_type_x or "spatial"
        header["CRPIX1"] = (1.0, "Reference pixel of 0 point")
        fp_width = data.shape[1]  # TODO verify that we didn't swap axis
        header["CRPIX1"] = (
            (fp_width / 2) + 1.0,
            "Reference pixel of 0 point",
        )  # center of the camera
        header["CRDER1"] = (
            1.5,
            "[arcsec] Error on telescope tracking",
        )  # estimated error

        header["CNAME3"] = "spectral"
    else:
        # no FP, fake axis
        # no scaling needed as the axis is fake
        x_scaling = 1.0  # arcsec/pixel
        header["CNAME1"] = "spatial"
        header["CRDER1"] = (
            0.0,
            "[arcsec] Error on telescope tracking",
        )  # no error since it is fake

        header["CNAME3"] = metadata.image_type_x or "spectral"

    # TODO handle slit scanner at GREGOR
    # fake width axis (with FP it is real)
    # x axis
    header["CTYPE1"] = ("HPLN-TAN", "Coordinate along axis 1")
    header["CUNIT1"] = ("arcsec", "Unit along axis 1")
    header["CSYER1"] = (10.0, "[arcsec] Estimated error on telescope pointing")

    # slit length
    # y axis
    slit_height = data.shape[0]  # in pixels
    header["CNAME2"] = metadata.image_type_y or "spectral"
    header["CTYPE2"] = "HPLT-TAN", "Coordinate along axis 2"
    header["CUNIT2"] = "arcsec", "Unit along axis 2"
    header["CRPIX2"] = (slit_height / 2) + 1.0, "Reference pixel of 0 point"
    header["CRDER2"] = 1.5, "[arcsec] Error on telescope tracking"
    header["CSYER2"] = 10.0, "[arcsec] Estimated error on telescope pointing"

    # wavelength axis
    header["CTYPE3"] = "AWAV", "Coordinate along axis 3"
    header["CUNIT3"] = "angstrom", "Unit along axis 3"
    header["CRPIX3"] = 1.0, "Reference pixel of 0 point"

    if a1 is not None and a0 is not None:
        header["CDELT3"] = (a1, "Increment per pixel")
        header["CRDER3"] = (a1_err, "[angstrom] Error on wavelength fitting scale")
        header["CRVAL3"] = (a0, "[angstrom] Wavelength at reference pixel")
        header["CSYER3"] = (a0_err, "[angstrom] Error on wavelength fitting shift")
        header["WAVEMIN"] = (round(a0, 2), "Minimum wavelength in data")

        header["WAVEMAX"] = (
            round(a1 * float(si_hdu.header["NAXIS1"]) + a0, 2),
            "Maximum wavelength in data",
        )
        header["WAVECAL"] = (1, "Wavelength calibration done")
    else:
        header["CDELT3"] = (1.0, "Increment per pixel")
        header["CRVAL3"] = (0.0, "Value at reference pixel")
        header["CRDER3"] = (
            0.0,
            "Error on wavelength fitting",
        )  # no error as not fit happened
        header["WAVEMIN"] = (metadata.wavelength - 1.0, "Minimum wavelength in data")
        header["WAVEMAX"] = (metadata.wavelength + 1.0, "Maximum wavelength in data")

    # specify that we don't have any wavelength calibration based on movement
    # as per SVO 3.2
    header["SPECSYS"] = ("TOPOCENT", "Spectral reference frame")
    header["VELOSYS"] = (0.0, "[m/s] Reference velocity")

    # Polarization
    header["POLCCONV"] = (
        "(+HPLT,-HPLN,+HPRZ)",
        "Reference system for Stokes vectors",
    )
    derotator_angle = metadata.derotator.position_angle or 0.0
    derot_offset = metadata.derotator.offset or 0.0
    header["POLCANGL"] = (
        90 + derotator_angle + derot_offset,
        "[deg] angle between +Q and solar north",
    )

    # Slit rotation and coordinate transformation
    sun_p0 = sun.P(obs_time).value
    sun_p0_rad = -sun_p0 * (np.pi / 180)

    angle = derotator_angle * (np.pi / 180) + (np.pi / 2)  # 90 deg rotation
    if metadata.derotator.coordinate_system == 0:
        angle = angle - sun_p0

    # TODO handle slit scanner
    header["CDELT1"] = (x_scaling, "[arcsec/pixel] scaling")
    header["CDELT2"] = (y_scaling, "[arcsec/pixel] scaling")
    header["CD1_1"] = (x_scaling * np.cos(angle), "[arcsec/pixel] X scaling")
    header["CD1_2"] = (-y_scaling * np.sin(angle), "[arcsec/pixel] Y scaling")
    header["CD2_1"] = (x_scaling * np.sin(angle), "[arcsec/pixel] X scaling")
    header["CD2_2"] = (y_scaling * np.cos(angle), "[arcsec/pixel] Y scaling")

    # TODO spectral axis transformation

    # compute values from position of IRSOL
    # TODO adapt for GREGOR, actually doesn't change much (0.01 arcsec error)
    # Solar coordinates
    gcrs_coord = IRSOL_LOCATION.get_gcrs(obstime=obs_time)
    hgsr_coord = gcrs_coord.transform_to(
        frames.HeliographicStonyhurst(obstime=obs_time)
    )
    observer = SkyCoord(gcrs_coord)
    cr_coord = gcrs_coord.transform_to(
        frames.HeliographicCarrington(obstime=obs_time, observer=observer)
    )

    # values from position
    header["RSUN_REF"] = (695700000.0, "[m] Standard solar radius")
    header["DSUN_REF"] = (149597870700.0, "[m] Standard Sun-Earth distance")
    header["RSUN_OBS"] = (
        angular_radius(obs_time).to_value(u.Unit("arcsec")),
        "[arcsec] Angular radius of the Sun",
    )
    header["DSUN_OBS"] = (
        hgsr_coord.radius.to_value(u.Unit("m")),
        "[m] Sun-Earth distance",
    )
    header["CRLN_OBS"] = (
        cr_coord.lon.to_value(u.Unit("deg")),
        "[deg] Carrington longitude of disk center",
    )
    header["CRLT_OBS"] = (
        cr_coord.lat.to_value(u.Unit("deg")),
        "[deg] Carrington latitude of disk center",
    )

    # Slit position on solar disc
    if metadata.solar_disc_coordinates:
        try:
            coords = [float(x) for x in metadata.solar_disc_coordinates.split()]
            rotated = np.array(
                [
                    coords[0] * np.cos(sun_p0_rad) - coords[1] * np.sin(sun_p0_rad),
                    coords[0] * np.sin(sun_p0_rad) + coords[1] * np.cos(sun_p0_rad),
                ]
            )
            header["CRVAL1"] = (
                rotated[0],
                "[arcsec] HPLN coordinate at reference pixel",
            )
            header["CRVAL2"] = (
                rotated[1],
                "[arcsec] HPLT coordinate at reference pixel",
            )
        except (ValueError, IndexError):
            logger.warning(
                "Impossible to compute CRVAL1 and CRVAL2 due to incorrect solar disc coordinates",
                solar_disc_coordinates=metadata.solar_disc_coordinates,
            )
    else:
        logger.warning(
            "Setting CRVAL1 and CRVAL2 to 'None' as no solar disc coordinates were provided"
        )
        header["CRVAL1"] = (None, "[arcsec] HPLN coordinate at reference pixel")
        header["CRVAL2"] = (None, "[[arcsec] HPLT coordinate at reference pixel")


def _add_data_statistics(header: fits.Header, data: np.ndarray) -> None:
    """Add statistical summary of the data to the header."""
    mean = np.mean(data)
    header["DATAMIN"] = (float(np.min(data)), "Minimum data value")
    header["DATAMAX"] = (float(np.max(data)), "Maximum data value")
    header["DATAMEDN"] = (float(np.median(data)), "Median data value")
    header["DATAMEAN"] = (float(mean), "Mean data value")
    header["DATASTD"] = (float(np.std(data)), "Standard deviation of data values")

    for pstr in ("01", "02", "05", "10", "25", "50", "75", "90", "95", "98", "99"):
        perc = int(pstr)
        val = float(np.percentile(data, perc))
        header[f"DATAP{pstr}"] = (val, f"{perc}th percentile data value")
        if mean != 0:
            header[f"DATANP{pstr}"] = (
                val / mean,
                f"{perc}th percentile normalized data value",
            )
        else:
            header[f"DATANP{pstr}"] = (
                0.0,
                f"{perc}th percentile normalized data value",
            )


def _add_software_metadata(
    header: fits.Header,
    software_version: str,
) -> None:
    """Add software versioning information to a FITS header."""
    header["SWVER"] = (software_version, "irsol_data_pipeline package version")


def _make_title(metadata: MeasurementMetadata) -> str:
    """Generate a FITS filename from metadata."""
    try:
        time_str = metadata.datetime_start.strftime("%Y%m%d_%H%M%S")
        return f"{time_str}_{metadata.name}.fits"
    except Exception:
        logger.warning(
            "Impossible to determine title for measurement, using default measurement title"
        )
        return "measurement.fits"
