"""Tests for the Cyclopts-based CLI application."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from cyclopts import App

from irsol_data_pipeline.cli import _meta, app


class TestCliApp:
    def test_meta_defaults_to_info(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.setup_logging") as mock_setup,
            patch("irsol_data_pipeline.cli.app") as mock_app,
        ):
            _meta("info")

        mock_setup.assert_called_once_with(level="INFO", force=True)
        mock_app.assert_called_once_with(("info",))

    def test_meta_verbose_one_gives_debug(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.setup_logging") as mock_setup,
            patch("irsol_data_pipeline.cli.app"),
        ):
            _meta("info", verbose=1)

        mock_setup.assert_called_once_with(level="DEBUG", force=True)

    def test_meta_verbose_two_gives_trace(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.setup_logging") as mock_setup,
            patch("irsol_data_pipeline.cli.app"),
        ):
            _meta("info", verbose=2)

        mock_setup.assert_called_once_with(level="TRACE", force=True)

    def test_meta_log_level_overrides(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.setup_logging") as mock_setup,
            patch("irsol_data_pipeline.cli.app"),
        ):
            _meta("info", log_level="WARNING")

        mock_setup.assert_called_once_with(level="WARNING", force=True)

    def test_meta_verbose_and_log_level_are_mutually_exclusive(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.setup_logging"),
            patch("irsol_data_pipeline.cli.app"),
        ):
            with pytest.raises(SystemExit) as exc_info:
                _meta("info", verbose=1, log_level="DEBUG")

        assert exc_info.value.code == 1

    def test_root_commands_are_registered(self) -> None:
        assert isinstance(app["prefect"], App)
        assert "info" in set(app)
        assert isinstance(app["plot"], App)

    def test_root_help_mentions_install_completion(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        app(
            ["--help"],
            exit_on_error=False,
            print_error=False,
            result_action="return_value",
        )

        assert "--install-completion" in capsys.readouterr().out

    def test_prefect_group_help(self, capsys: pytest.CaptureFixture[str]) -> None:
        app(
            ["prefect", "--help"],
            exit_on_error=False,
            print_error=False,
            result_action="return_value",
        )

        output = capsys.readouterr().out
        assert "start" in output
        assert "reset-database" in output
        assert "flows" in output
        assert "status" in output
        assert "configure" in output
        assert "variables" in output
