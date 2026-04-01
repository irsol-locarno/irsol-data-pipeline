"""Tests for measurement_processor — focusing on
convert_measurement_to_fits."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np

from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MeasurementMetadata,
    StokesParameters,
)
from irsol_data_pipeline.core.solar_orientation import SolarOrientationInfo
from irsol_data_pipeline.pipeline.measurement_processor import (
    convert_measurement_to_fits,
    plot_original_profile,
)


def _make_stokes() -> StokesParameters:
    return StokesParameters(
        i=np.ones((10, 20)),
        q=np.zeros((10, 20)),
        u=np.zeros((10, 20)),
        v=np.zeros((10, 20)),
    )


def _make_calibration() -> CalibrationResult:
    return CalibrationResult(
        pixel_scale=0.01,
        wavelength_offset=6301.5,
        pixel_scale_error=0.001,
        wavelength_offset_error=0.01,
        reference_file="ref.npy",
    )


class TestConvertMeasurementToFits:
    """Tests for convert_measurement_to_fits."""

    def test_writes_converted_fits_and_profile_png(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """convert_measurement_to_fits calls fits_io.write with
        *_converted.fits and _plot_data with *_profile_converted.png."""
        measurement_path = tmp_path / "reduced" / "6302_m1.dat"
        measurement_path.parent.mkdir(parents=True)
        measurement_path.write_text("placeholder")
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        stokes = _make_stokes()
        calibration = _make_calibration()
        solar_orientation = SolarOrientationInfo(
            sun_p0_deg=0.5,
            slit_angle_solar_deg=45.0,
            needs_rotation=False,
        )

        with (
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.dat_io.read",
                return_value=(stokes, sample_measurement_metadata),
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.compute_solar_orientation",
                return_value=solar_orientation,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.calibrate_measurement",
                return_value=calibration,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.fits_io.write",
            ) as mock_fits_write,
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor._plot_data",
            ) as mock_plot,
        ):
            convert_measurement_to_fits(
                measurement_path=measurement_path,
                processed_dir=processed_dir,
            )

        mock_fits_write.assert_called_once()
        write_call = mock_fits_write.call_args
        # output_path is a positional arg
        output_path = (
            write_call.args[0]
            if write_call.args
            else write_call.kwargs.get("output_path")
        )
        assert output_path.name == "6302_m1_converted.fits"
        stokes_arg = (
            write_call.args[1]
            if len(write_call.args) > 1
            else write_call.kwargs.get("stokes")
        )
        assert stokes_arg is stokes
        metadata_arg = (
            write_call.args[2]
            if len(write_call.args) > 2
            else write_call.kwargs.get("info")
        )
        assert metadata_arg is sample_measurement_metadata
        assert write_call.kwargs.get("calibration") is calibration

        mock_plot.assert_called_once()
        plot_kwargs = mock_plot.call_args.kwargs
        assert plot_kwargs["filename_save"].name == "6302_m1_profile_converted.png"

    def test_fits_header_has_ffcorr_false(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """The converted FITS extra_header must contain FFCORR=False (bare bool
        or (value, comment) tuple)."""
        measurement_path = tmp_path / "6302_m1.dat"
        measurement_path.write_text("placeholder")
        processed_dir = tmp_path / "processed"

        stokes = _make_stokes()
        calibration = _make_calibration()

        with (
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.dat_io.read",
                return_value=(stokes, sample_measurement_metadata),
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.compute_solar_orientation",
                return_value=None,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.calibrate_measurement",
                return_value=calibration,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.fits_io.write",
            ) as mock_fits_write,
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor._plot_data",
            ),
        ):
            convert_measurement_to_fits(
                measurement_path=measurement_path,
                processed_dir=processed_dir,
            )

        extra_header = mock_fits_write.call_args.kwargs["extra_header"]
        from irsol_data_pipeline.io.fits.constants import FITS_KEY_FFCORR

        assert FITS_KEY_FFCORR in extra_header
        ffcorr_value = extra_header[FITS_KEY_FFCORR]
        # Value may be a bare bool or a (value, comment) tuple
        actual_value = (
            ffcorr_value[0] if isinstance(ffcorr_value, tuple) else ffcorr_value
        )
        assert actual_value is False

    def test_fits_header_does_not_have_fffile(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """The converted FITS must NOT include FFFILE (no flat-field was
        used)."""
        measurement_path = tmp_path / "6302_m1.dat"
        measurement_path.write_text("placeholder")
        processed_dir = tmp_path / "processed"

        stokes = _make_stokes()
        calibration = _make_calibration()

        with (
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.dat_io.read",
                return_value=(stokes, sample_measurement_metadata),
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.compute_solar_orientation",
                return_value=None,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.calibrate_measurement",
                return_value=calibration,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.fits_io.write",
            ) as mock_fits_write,
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor._plot_data",
            ),
        ):
            convert_measurement_to_fits(
                measurement_path=measurement_path,
                processed_dir=processed_dir,
            )

        extra_header = mock_fits_write.call_args.kwargs["extra_header"]
        from irsol_data_pipeline.io.fits.constants import FITS_KEY_FFFILE

        assert FITS_KEY_FFFILE not in extra_header

    def test_creates_processed_dir_if_missing(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """convert_measurement_to_fits creates the processed_dir (including
        parents) when it does not exist."""
        measurement_path = tmp_path / "6302_m1.dat"
        measurement_path.write_text("placeholder")
        processed_dir = tmp_path / "processed" / "does_not_exist"

        assert not processed_dir.exists()

        stokes = _make_stokes()
        calibration = _make_calibration()

        with (
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.dat_io.read",
                return_value=(stokes, sample_measurement_metadata),
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.compute_solar_orientation",
                return_value=None,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.calibrate_measurement",
                return_value=calibration,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.fits_io.write",
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor._plot_data",
            ),
        ):
            convert_measurement_to_fits(
                measurement_path=measurement_path,
                processed_dir=processed_dir,
            )

        assert processed_dir.exists()


class TestPlotOriginalProfile:
    """Tests for plot_original_profile."""

    def test_writes_profile_original_png(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """plot_original_profile calls _plot_data with
        *_profile_original.png."""
        measurement_path = tmp_path / "reduced" / "6302_m1.dat"
        measurement_path.parent.mkdir(parents=True)
        measurement_path.write_text("placeholder")
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        stokes = _make_stokes()
        calibration = _make_calibration()
        solar_orientation = SolarOrientationInfo(
            sun_p0_deg=0.5,
            slit_angle_solar_deg=45.0,
            needs_rotation=False,
        )

        with (
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.dat_io.read",
                return_value=(stokes, sample_measurement_metadata),
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.compute_solar_orientation",
                return_value=solar_orientation,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.calibrate_measurement",
                return_value=calibration,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor._plot_data",
            ) as mock_plot,
        ):
            plot_original_profile(
                measurement_path=measurement_path,
                processed_dir=processed_dir,
            )

        mock_plot.assert_called_once()
        plot_kwargs = mock_plot.call_args.kwargs
        assert plot_kwargs["filename_save"].name == "6302_m1_profile_original.png"
        assert plot_kwargs["stokes"] is stokes

    def test_does_not_write_fits_file(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """plot_original_profile does not write any FITS file."""
        measurement_path = tmp_path / "6302_m1.dat"
        measurement_path.write_text("placeholder")
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        stokes = _make_stokes()
        calibration = _make_calibration()

        with (
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.dat_io.read",
                return_value=(stokes, sample_measurement_metadata),
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.compute_solar_orientation",
                return_value=None,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.calibrate_measurement",
                return_value=calibration,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.fits_io.write",
            ) as mock_fits_write,
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor._plot_data",
            ),
        ):
            plot_original_profile(
                measurement_path=measurement_path,
                processed_dir=processed_dir,
            )

        mock_fits_write.assert_not_called()

    def test_creates_processed_dir_if_missing(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """plot_original_profile creates processed_dir when it does not
        exist."""
        measurement_path = tmp_path / "6302_m1.dat"
        measurement_path.write_text("placeholder")
        processed_dir = tmp_path / "processed" / "does_not_exist"

        assert not processed_dir.exists()

        stokes = _make_stokes()
        calibration = _make_calibration()

        with (
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.dat_io.read",
                return_value=(stokes, sample_measurement_metadata),
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.compute_solar_orientation",
                return_value=None,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor.calibrate_measurement",
                return_value=calibration,
            ),
            patch(
                "irsol_data_pipeline.pipeline.measurement_processor._plot_data",
            ),
        ):
            plot_original_profile(
                measurement_path=measurement_path,
                processed_dir=processed_dir,
            )

        assert processed_dir.exists()
