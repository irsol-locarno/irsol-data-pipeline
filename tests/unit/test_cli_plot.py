"""Tests for plot-related CLI commands."""

from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pytest
from cyclopts.exceptions import ValidationError

from irsol_data_pipeline.cli import app
from irsol_data_pipeline.core.models import StokesParameters


class TestCliPlot:
    def test_profile_renders_png(self, tmp_path) -> None:
        input_path = tmp_path / "measurement.dat"
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
                "irsol_data_pipeline.io.dat.read", return_value=(stokes, np.array([]))
            ) as read_dat,
            patch("irsol_data_pipeline.plotting.profile.plot") as plot_profile,
            patch("irsol_data_pipeline.has_display", return_value=True),
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
        )

    def test_profile_supports_show_without_output_path(self, tmp_path) -> None:
        input_path = tmp_path / "measurement.dat"
        input_path.write_text("placeholder")
        stokes = StokesParameters(
            i=np.ones((4, 5)),
            q=np.zeros((4, 5)),
            u=np.zeros((4, 5)),
            v=np.zeros((4, 5)),
        )

        with (
            patch(
                "irsol_data_pipeline.io.dat.read", return_value=(stokes, np.array([]))
            ) as read_dat,
            patch("irsol_data_pipeline.plotting.profile.plot") as plot_profile,
            patch("irsol_data_pipeline.has_display", return_value=True),
        ):
            result = app(
                ["plot", "profile", str(input_path), "--show"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        read_dat.assert_called_once_with(input_path.resolve())
        plot_profile.assert_called_once_with(
            stokes,
            filename_save=None,
            show=True,
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
