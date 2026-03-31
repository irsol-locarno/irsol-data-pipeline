"""Tests for CLI runtime presentation output."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from irsol_data_pipeline.cli import presentation


class TestCliPresentation:
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
