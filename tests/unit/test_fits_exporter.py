"""Tests for FITS export helpers."""

from __future__ import annotations

import datetime
from typing import cast

import numpy as np
import pytest
from astropy.io import fits

from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MeasurementMetadata,
    StokesParameters,
)
from irsol_data_pipeline.io.fits.exporter import write_stokes_fits


@pytest.fixture
def measurement_metadata() -> MeasurementMetadata:
    return MeasurementMetadata(
        wavelength=6302,
        datetime_start=datetime.datetime(
            2024, 7, 13, 10, 22, tzinfo=datetime.timezone(datetime.timedelta(hours=1))
        ),
        datetime_end=datetime.datetime(
            2024, 7, 13, 10, 32, tzinfo=datetime.timezone(datetime.timedelta(hours=1))
        ),
        telescope_name="IRSOL",
        instrument="ZIMPOL",
        measurement_name="map_01",
        measurement_type="science",
        measurement_id="m1",
        observer="Test Observer",
        project="Test Project",
        camera_identity="Cam-1",
        camera_ccd="CCD-1",
        camera_temperature=None,
        integration_time=0.5,
        images="1 1",
        solar_p0=None,
        solar_disc_coordinates=None,
        derotator_position_angle=None,
        derotator_offset=None,
        derotator_coordinate_system=None,
        spectrograph_slit=None,
        reduction_outfname=None,
    )


def test_write_stokes_fits_writes_processed_measurement(
    tmp_path, measurement_metadata: MeasurementMetadata
):
    output_path = tmp_path / "6302_m1_corrected.fits"
    stokes = StokesParameters(
        i=np.arange(20, dtype=float).reshape(4, 5) + 10.0,
        q=np.full((4, 5), 0.1),
        u=np.full((4, 5), 0.2),
        v=np.full((4, 5), -0.1),
    )
    calibration = CalibrationResult(
        pixel_scale=0.012,
        wavelength_offset=6301.5,
        pixel_scale_error=0.001,
        wavelength_offset_error=0.01,
        reference_file="reference.npy",
    )

    result = write_stokes_fits(
        output_path=output_path,
        stokes=stokes,
        info=measurement_metadata,
        calibration=calibration,
    )

    assert result == output_path
    assert output_path.exists()

    with fits.open(output_path) as hdul:
        stokes_i_hdu = cast(fits.ImageHDU, hdul[1])
        assert len(hdul) == 5
        assert stokes_i_hdu.header["EXTNAME"] == "Stokes I"
        assert stokes_i_hdu.header["WAVECAL"] == 1
        assert stokes_i_hdu.header["WAVELNTH"] == 6302
        assert stokes_i_hdu.data.shape == (5, 4, 1)


def test_write_stokes_fits_omits_calibration_metadata_when_not_provided(
    tmp_path, measurement_metadata: MeasurementMetadata
):
    output_path = tmp_path / "6302_m2_corrected.fits"
    stokes = StokesParameters(
        i=np.arange(20, dtype=float).reshape(4, 5) + 10.0,
        q=np.full((4, 5), 0.1),
        u=np.full((4, 5), 0.2),
        v=np.full((4, 5), -0.1),
    )

    write_stokes_fits(
        output_path=output_path,
        stokes=stokes,
        info=measurement_metadata,
    )

    with fits.open(output_path) as hdul:
        stokes_i_hdu = cast(fits.ImageHDU, hdul[1])
        assert "WAVECAL" not in stokes_i_hdu.header
