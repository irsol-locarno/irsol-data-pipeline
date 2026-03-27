"""Tests for plot-related CLI commands."""

from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pytest
from cyclopts.exceptions import ValidationError

from irsol_data_pipeline.cli import app
from irsol_data_pipeline.core.models import StokesParameters
from irsol_data_pipeline.io.fits.importer import ImportedFitsMeasurement


class TestCliPlot:
    def test_profile_renders_png_from_dat(self, tmp_path) -> None:
        input_path = tmp_path / "measurement.dat"
        input_path.write_text("placeholder")
        output_path = tmp_path / "profile.png"
        stokes = StokesParameters(
            i=np.ones((4, 5)),
            q=np.zeros((4, 5)),
            u=np.zeros((4, 5)),
            v=np.zeros((4, 5)),
        )
        mock_metadata_return_value = object()
        mock_solar_orientation_return_value = object()

        with (
            patch(
                "irsol_data_pipeline.io.dat.read", return_value=(stokes, np.array([]))
            ) as read_dat,
            patch("irsol_data_pipeline.plotting.profile.plot") as plot_profile,
            patch(
                "irsol_data_pipeline.core.models.MeasurementMetadata.from_info_array",
                return_value=mock_metadata_return_value,
            ),
            patch(
                "irsol_data_pipeline.core.solar_orientation.compute_solar_orientation",
                return_value=mock_solar_orientation_return_value,
            ),
            patch("irsol_data_pipeline.cli.common.has_display", return_value=True),
        ):
            result = app(
                [
                    "plot",
                    "profile",
                    str(input_path),
                    "--output-path",
                    str(output_path),
                    "--show",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        read_dat.assert_called_once_with(input_path.resolve())
        plot_profile.assert_called_once_with(
            stokes,
            filename_save=output_path.resolve(),
            show=True,
            a0=None,
            a1=None,
            solar_orientation=mock_solar_orientation_return_value,
            metadata=mock_metadata_return_value,
        )

    def test_profile_renders_png_from_fits(self, tmp_path) -> None:
        input_path = tmp_path / "measurement.fits"
        input_path.write_text("placeholder")
        output_path = tmp_path / "profile.png"
        stokes = StokesParameters(
            i=np.ones((4, 5)),
            q=np.zeros((4, 5)),
            u=np.zeros((4, 5)),
            v=np.zeros((4, 5)),
        )

        with (
            patch(
                "irsol_data_pipeline.io.fits.read",
                return_value=ImportedFitsMeasurement(
                    stokes=stokes, calibration=None, header=None
                ),
            ) as read_fits,
            patch("irsol_data_pipeline.plotting.profile.plot") as plot_profile,
            patch("irsol_data_pipeline.cli.common.has_display", return_value=True),
        ):
            result = app(
                [
                    "plot",
                    "profile",
                    str(input_path),
                    "--output-path",
                    str(output_path),
                    "--show",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        read_fits.assert_called_once_with(input_path.resolve())
        plot_profile.assert_called_once_with(
            stokes,
            filename_save=output_path.resolve(),
            show=True,
            a0=None,
            a1=None,
            metadata=None,
            solar_orientation=None,
        )

    def test_profile_requires_show_or_output_path(self, tmp_path) -> None:
        input_path = tmp_path / "measurement.dat"
        input_path.write_text("placeholder")

        with pytest.raises(ValidationError):
            app(
                ["plot", "profile", str(input_path)],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )
