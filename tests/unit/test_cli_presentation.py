"""Tests for CLI runtime presentation output."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from irsol_data_pipeline.cli import presentation


class TestCliPresentation:
    def test_build_runtime_presentation_includes_versions_and_runtime(self) -> None:
        version_by_distribution = {
            "spectroflat": "2.1.0",
            "numpy": "1.26.4",
            "pydantic": "2.10.6",
        }

        with (
            patch(
                "importlib.metadata.version",
                side_effect=version_by_distribution.__getitem__,
            ),
            patch("platform.system", return_value="Linux"),
            patch("platform.release", return_value="6.8.0"),
            patch("platform.machine", return_value="x86_64"),
            patch("platform.python_version", return_value="3.11.4"),
            patch(
                "irsol_data_pipeline.cli.presentation._detect_terminal_columns",
                return_value=200,
            ),
        ):
            result = presentation.build_runtime_presentation()

        assert "OS      : Linux 6.8.0 x86_64" in result
        assert "Python  : 3.11.4" in result
        assert "spectroflat : 2.1.0" in result
        assert "numpy       : 1.26.4" in result
        assert "pydantic    : 2.10.6" in result

    def test_build_runtime_presentation_uses_largest_fitting_title(
        self,
    ) -> None:
        with patch(
            "irsol_data_pipeline.cli.presentation._detect_terminal_columns",
            return_value=100,
        ):
            result = presentation.build_runtime_presentation()

        assert presentation.COMPACT_TITLE in result

    def test_print_runtime_presentation_writes_banner(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "irsol_data_pipeline.cli.presentation.build_runtime_presentation",
            return_value="banner",
        ):
            presentation.print_runtime_presentation()

        assert capsys.readouterr().out == "banner\n\n"
