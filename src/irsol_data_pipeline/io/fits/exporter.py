"""FITS export helpers for processed Stokes measurements.

Serializes in-memory ``StokesParameters`` + ``MeasurementMetadata`` into a
multi-extension FITS product with WCS, observatory metadata, and optional
wavelength calibration header values.
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
from sunpy.coordinates import frames, sun
from sunpy.coordinates.sun import angular_radius

from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MeasurementMetadata,
    StokesParameters,
)

IRSOL_LOCATION = EarthLocation(
    lat=46.176906 * u.Unit("deg"),
    lon=8.788521 * u.Unit("deg"),
    height=503.4 * u.Unit("m"),
)

GREGOR_LOCATION = EarthLocation(
    lat=28.3014 * u.Unit("deg"),
    lon=-16.5097 * u.Unit("deg"),
    height=2390.0 * u.Unit("m"),
)


def write_stokes_fits(
    output_path: Path,
    stokes: StokesParameters,
    info: MeasurementMetadata,
    calibration: Optional[CalibrationResult] = None,
) -> Path:
    """Write processed Stokes data to a FITS file.

    Args:
        output_path: Where to write the `.fits` file.
        stokes: Stokes data to serialize.
        info: Measurement metadata used to derive FITS headers.
        calibration: Optional precomputed wavelength calibration.

    Returns:
        The path written to.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    hdu_list = _build_fits_hdu_list(
        stokes=stokes,
        info=info,
        calibration=calibration,
    )
    hdu_list.writeto(str(out), overwrite=True)
    return out


def _build_fits_hdu_list(
    stokes: StokesParameters,
    info: MeasurementMetadata,
    calibration: Optional[CalibrationResult] = None,
) -> fits.HDUList:
    """Build a FITS HDU list from Stokes data and raw info metadata.

    Calibration metadata is only included when an explicit calibration
    object is provided.
    """
    a1, a0, a1_err, a0_err = _calibration_values(calibration)
    return _build_hdu_list(stokes, info, a1, a0, a1_err, a0_err)


def _calibration_values(
    calibration: Optional[CalibrationResult],
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Resolve wavelength calibration values for FITS headers."""
    if calibration is None:
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

    _fill_primary_header(hdu_primary.header, metadata)
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

    # Checksums
    for hdu in [si_hdu, sq_hdu, su_hdu, sv_hdu]:
        hdu.add_datasum()
        hdu.add_checksum()
    hdu_primary.add_checksum()

    return fits.HDUList([hdu_primary, si_hdu, sq_hdu, su_hdu, sv_hdu])


def _fill_primary_header(header: fits.Header, metadata: MeasurementMetadata) -> None:
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

    if metadata.camera_temperature is not None:
        header["CAMTEMP"] = (metadata.camera_temperature, "Camera temp in celsius")
    if metadata.solar_p0 is not None:
        header["SOLAR_P0"] = (metadata.solar_p0, "Sun-Earth angle")


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

    # Telescope and instrument
    header["TELESCOP"] = (metadata.telescope_name, "Telescope")

    telescope = metadata.telescope_name
    if telescope in ("IRSOL", "Gregory IRSOL"):
        location = IRSOL_LOCATION
        y_scaling = 1.3
        gcrs = location.get_gcrs(obstime=obs_time)
        header["OBSGEO-X"] = (
            gcrs.cartesian.x.to_value(u.Unit("m")),
            "[m] IRSOL location (ITRS)",
        )
        header["OBSGEO-Y"] = (
            gcrs.cartesian.y.to_value(u.Unit("m")),
            "[m] IRSOL location (ITRS)",
        )
        header["OBSGEO-Z"] = (
            gcrs.cartesian.z.to_value(u.Unit("m")),
            "[m] IRSOL location (ITRS)",
        )
        slit = metadata.spectrograph_slit
        if slit and slit != "-1":
            header["SLIT_WID"] = (
                round(7.9 * float(slit), 3),
                f"[arcsec] ({slit} mm), slit width",
            )
        else:
            header["SLIT_WID"] = (0.474, "[arcsec] (0.06 mm), slit width")
    elif telescope == "GREGOR":
        location = GREGOR_LOCATION
        y_scaling = 1.0
        gcrs = location.get_gcrs(obstime=obs_time)
        header["OBSGEO-X"] = (
            gcrs.cartesian.x.to_value(u.Unit("m")),
            "[m] GREGOR location (ITRS)",
        )
        header["OBSGEO-Y"] = (
            gcrs.cartesian.y.to_value(u.Unit("m")),
            "[m] GREGOR location (ITRS)",
        )
        header["OBSGEO-Z"] = (
            gcrs.cartesian.z.to_value(u.Unit("m")),
            "[m] GREGOR location (ITRS)",
        )
        header["SLIT_WID"] = (0.29, "[arcsec] (0.07mm), slit width")
    else:
        location = IRSOL_LOCATION
        y_scaling = 1.3

    header["INSTRUME"] = (metadata.instrument, "Observing instrument")
    header["DATATYPE"] = (metadata.measurement_type, "Type of measurement")
    header["POINT_ID"] = (metadata.measurement_id, "Measurement ID")
    header["OBSERVER"] = (metadata.observer, None)
    header["PROJECT"] = (metadata.project, None)
    header["MEASNAME"] = (metadata.measurement_name, "Name of measurement")

    # Exposure info
    if metadata.images:
        try:
            nsumexp = sum(int(i) for i in metadata.images.split() if i)
            header["NSUMEXP"] = (nsumexp, "Number of summed exposures")
        except ValueError:
            pass
    if metadata.integration_time is not None:
        header["TEXPOSUR"] = (metadata.integration_time, "[s] single exposure time")
        nsumexp_value = header.get("NSUMEXP")
        if isinstance(nsumexp_value, (int, float)):
            header["XPOSURE"] = (
                metadata.integration_time * int(nsumexp_value),
                "[s] total exposure time",
            )

    header["CAMERA"] = (metadata.camera_identity, "Camera identity")
    header["CCD"] = (metadata.camera_ccd, "Camera sensor identity")
    header["WAVELNTH"] = (metadata.wavelength, "Guideline Wavelength in Angstrom")

    # Data statistics
    _add_data_statistics(header, data)

    # WCS
    header["WCSNAME"] = "Helioprojective-cartesian"
    slit_height = data.shape[0]

    header["CNAME1"] = "spatial"
    header["CTYPE1"] = ("HPLN-TAN", "Coordinate along axis 1")
    header["CUNIT1"] = ("arcsec", "Unit along axis 1")
    header["CRDER1"] = (0.0, "[arcsec] Error on telescope tracking")
    header["CSYER1"] = (10.0, "[arcsec] Estimated error on telescope pointing")

    header["CNAME2"] = "spatial"
    header["CTYPE2"] = ("HPLT-TAN", "Coordinate along axis 2")
    header["CUNIT2"] = ("arcsec", "Unit along axis 2")
    header["CRPIX2"] = ((slit_height / 2) + 1.0, "Reference pixel of 0 point")
    header["CRDER2"] = (1.5, "[arcsec] Error on telescope tracking")
    header["CSYER2"] = (10.0, "[arcsec] Estimated error on telescope pointing")

    header["CNAME3"] = "spectral"
    header["CTYPE3"] = ("AWAV", "Coordinate along axis 3")
    header["CUNIT3"] = ("angstrom", "Unit along axis 3")
    header["CRPIX3"] = (1.0, "Reference pixel of 0 point")

    if a1 is not None and a0 is not None:
        header["CDELT3"] = (a1, "Increment per pixel")
        header["CRVAL3"] = (a0, "[angstrom] Wavelength at reference pixel")
        if a1_err is not None:
            header["CRDER3"] = (a1_err, "[angstrom] Error on wavelength fitting scale")
        if a0_err is not None:
            header["CSYER3"] = (a0_err, "[angstrom] Error on wavelength fitting shift")
        header["WAVEMIN"] = (round(a0, 2), "Minimum wavelength in data")
        naxis1_value = si_hdu.header.get("NAXIS1", data.shape[1])
        naxis1 = (
            int(naxis1_value)
            if isinstance(naxis1_value, (int, float))
            else data.shape[1]
        )
        header["WAVEMAX"] = (round(a1 * naxis1 + a0, 2), "Maximum wavelength in data")
        header["WAVECAL"] = (1, "Wavelength calibration done")
    else:
        header["CDELT3"] = (1.0, "Increment per pixel")
        header["CRVAL3"] = (0.0, "Value at reference pixel")
        header["WAVEMIN"] = (metadata.wavelength - 1.0, "Minimum wavelength in data")
        header["WAVEMAX"] = (metadata.wavelength + 1.0, "Maximum wavelength in data")

    header["SPECSYS"] = ("TOPOCENT", "Spectral reference frame")
    header["VELOSYS"] = (0.0, "[m/s] Reference velocity")

    # Polarization
    if metadata.derotator_position_angle is not None:
        derot_offset = metadata.derotator_offset or 0.0
        header["POLCCONV"] = (
            "(+HPLT,-HPLN,+HPRZ)",
            "Reference system for Stokes vectors",
        )
        header["POLCANGL"] = (
            90 + metadata.derotator_position_angle + derot_offset,
            "[deg] angle between +Q and solar north",
        )

    # Slit rotation and coordinate transformation
    sun_p0 = sun.P(obs_time).value
    sun_p0_rad = -sun_p0 * (np.pi / 180)

    x_scaling = 1.0
    if metadata.derotator_position_angle is not None:
        angle = metadata.derotator_position_angle * (np.pi / 180) + (np.pi / 2)
        if metadata.derotator_coordinate_system == "0":
            angle = angle - sun_p0_rad
    else:
        angle = 0.0

    header["CDELT1"] = (x_scaling, "[arcsec/pixel] scaling")
    header["CDELT2"] = (y_scaling, "[arcsec/pixel] scaling")
    header["CD1_1"] = (x_scaling * np.cos(angle), "[arcsec/pixel] X scaling")
    header["CD1_2"] = (-y_scaling * np.sin(angle), "[arcsec/pixel] Y scaling")
    header["CD2_1"] = (x_scaling * np.sin(angle), "[arcsec/pixel] X scaling")
    header["CD2_2"] = (y_scaling * np.cos(angle), "[arcsec/pixel] Y scaling")

    # Solar coordinates
    gcrs_coord = location.get_gcrs(obstime=obs_time)
    hgsr_coord = gcrs_coord.transform_to(
        frames.HeliographicStonyhurst(obstime=obs_time)
    )
    observer = SkyCoord(gcrs_coord)
    cr_coord = gcrs_coord.transform_to(
        frames.HeliographicCarrington(obstime=obs_time, observer=observer)
    )

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
            pass


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


def _make_title(metadata: MeasurementMetadata) -> str:
    """Generate a FITS filename from metadata."""
    try:
        time_str = metadata.datetime_start.strftime("%Y%m%d_%H%M%S")
        return f"{time_str}_{metadata.measurement_name}.fits"
    except Exception:
        return "measurement.fits"
