import numpy as np
import pytest
from astropy.io import fits

from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MeasurementMetadata,
    SolarOrientationInfo,
    StokesParameters,
)
from irsol_data_pipeline.io.fits.exporter import write_stokes_fits

ALL_STOKES = ["Stokes I", "Stokes Q/I", "Stokes U/I", "Stokes V/I"]


@pytest.mark.parametrize(
    "with_calibration,with_solar_orientation",
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ],
)
def test_fits_header_keys_presence(
    tmp_path,
    sample_measurement_metadata: MeasurementMetadata,
    with_calibration,
    with_solar_orientation,
):
    output_path = tmp_path / f"test_{with_calibration}_{with_solar_orientation}.fits"
    stokes = StokesParameters(
        i=np.arange(20, dtype=float).reshape(4, 5) + 10.0,
        q=np.full((4, 5), 0.1),
        u=np.full((4, 5), 0.2),
        v=np.full((4, 5), -0.1),
    )
    calibration = (
        CalibrationResult(
            pixel_scale=0.012,
            wavelength_offset=6301.5,
            pixel_scale_error=0.001,
            wavelength_offset_error=0.01,
            reference_file="reference.npy",
        )
        if with_calibration
        else None
    )
    solar_orientation = (
        SolarOrientationInfo(
            sun_p0_deg=0.5,
            slit_angle_solar_deg=45,
            needs_rotation=False,
        )
        if with_solar_orientation
        else None
    )

    write_stokes_fits(
        output_path=output_path,
        stokes=stokes,
        info=sample_measurement_metadata,
        calibration=calibration,
        solar_orientation=solar_orientation,
    )

    with fits.open(output_path) as hdul:
        primary_header = hdul[0].header
        stokes_hdus = {h.header["EXTNAME"]: h for h in hdul[1:]}

        # Check solar orientation key in primary header
        if with_solar_orientation:
            assert primary_header["SLTANGL"] is not None
        else:
            assert primary_header["SLTANGL"] is None

        # Calibration keys in Stokes I header
        stokes_i = stokes_hdus["Stokes I"]
        if with_calibration:
            assert stokes_i.header.get("WAVECAL", 0) == 1
            assert stokes_i.header.get("CRVAL3") == 6301.5
            assert stokes_i.header.get("CDELT3") == 0.012
        else:
            assert stokes_i.header.get("WAVECAL", None) is None
            crval3 = stokes_i.header.get("CRVAL3")
            cdelt3 = stokes_i.header.get("CDELT3")
            assert crval3 == 0.0
            assert cdelt3 == 1.0

        # Check all Stokes HDUs have EXTNAME and correct shape
        for extname in ALL_STOKES:
            hdu = stokes_hdus[extname]
            assert hdu.header["EXTNAME"] == extname
            assert hdu.data.shape[-1] == 1
