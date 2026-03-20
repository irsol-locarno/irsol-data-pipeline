"""Tests for the Cyclopts-based CLI application."""

from __future__ import annotations

import json
import subprocess
from unittest.mock import patch

import pytest
from cyclopts import App
from cyclopts.exceptions import ValidationError

from irsol_data_pipeline.cli.app import _meta, app
from irsol_data_pipeline.cli.prefect_command import start_prefect_server


class TestCliApp:
    def test_meta_defaults_to_info(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.app.setup_logging") as mock_setup,
            patch("irsol_data_pipeline.cli.app.app") as mock_app,
        ):
            _meta("info")

        mock_setup.assert_called_once_with(level="INFO", force=True)
        mock_app.assert_called_once_with(("info",))

    def test_meta_verbose_one_gives_debug(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.app.setup_logging") as mock_setup,
            patch("irsol_data_pipeline.cli.app.app"),
        ):
            _meta("info", verbose=1)

        mock_setup.assert_called_once_with(level="DEBUG", force=True)

    def test_meta_verbose_two_gives_trace(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.app.setup_logging") as mock_setup,
            patch("irsol_data_pipeline.cli.app.app"),
        ):
            _meta("info", verbose=2)

        mock_setup.assert_called_once_with(level="TRACE", force=True)

    def test_meta_log_level_overrides(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.app.setup_logging") as mock_setup,
            patch("irsol_data_pipeline.cli.app.app"),
        ):
            _meta("info", log_level="WARNING")

        mock_setup.assert_called_once_with(level="WARNING", force=True)

    def test_meta_verbose_and_log_level_are_mutually_exclusive(self) -> None:
        with (
            patch("irsol_data_pipeline.cli.app.setup_logging"),
            patch("irsol_data_pipeline.cli.app.app"),
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
        assert "variables" in output

    def test_prefect_flows_list_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        app(
            ["prefect", "flows", "list", "--format", "json", "--no-banner"],
            exit_on_error=False,
            print_error=False,
            result_action="return_value",
        )

        payload = json.loads(capsys.readouterr().out)

        assert [entry["group"] for entry in payload["flow_groups"]] == [
            "flat-field-correction",
            "slit-images",
            "maintenance",
        ]
        assert payload["flow_groups"][0]["flows"][0]["deployment_name"] == (
            "flat-field-correction-full"
        )

    def test_prefect_flows_serve_requires_selection(self) -> None:
        with pytest.raises(ValidationError):
            app(
                ["prefect", "flows", "serve", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

    def test_prefect_flows_serve_rejects_all_plus_explicit_group(self) -> None:
        with pytest.raises(ValidationError):
            app(
                [
                    "prefect",
                    "flows",
                    "serve",
                    "--all",
                    "flat-field-correction",
                    "--no-banner",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

    def test_info_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        value_by_name = {
            "data-root-path": ("/srv/data", "set"),
            "jsoc-email": ("operator@example.com", "set"),
            "cache-expiration-hours": ("672", "set"),
            "flow-run-expiration-hours": ("<unset>", "unset"),
        }

        with patch(
            "irsol_data_pipeline.cli.info.safe_read_prefect_variable",
            side_effect=value_by_name.__getitem__,
        ):
            app(
                ["info", "--format", "json", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert payload["version"]
        assert payload["prefect_variables"][0] == {
            "name": "data-root-path",
            "status": "set",
            "value": "/srv/data",
        }

    def test_prefect_variables_list_json(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        value_by_name = {
            "data-root-path": ("/srv/data", "set"),
            "jsoc-email": ("observer@example.com", "set"),
            "cache-expiration-hours": ("672", "set"),
            "flow-run-expiration-hours": ("<unset>", "unset"),
        }

        with patch(
            "irsol_data_pipeline.cli.variables.safe_read_prefect_variable",
            side_effect=value_by_name.__getitem__,
        ):
            app(
                ["prefect", "variables", "list", "--format", "json", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert payload["variables"][0]["value"] == "/srv/data"
        assert payload["variables"][1]["value"] == "observer@example.com"
        assert payload["variables"][3]["status"] == "unset"

    def test_prefect_variables_configure_returns_zero_when_skipping_all(
        self,
    ) -> None:
        with (
            patch("prefect.variables.Variable.get", return_value=None),
            patch("prefect.variables.Variable.set"),
            patch(
                "builtins.input",
                side_effect=["", "", "", "n", "", "n"],
            ),
            patch(
                "irsol_data_pipeline.cli.variables._render_variable_entries",
                return_value=None,
            ),
        ):
            result = app(
                ["prefect", "variables", "configure", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result == 0

    def test_prefect_variables_configure_returns_three_on_failure(self) -> None:
        with (
            patch("prefect.variables.Variable.get", return_value=None),
            patch(
                "prefect.variables.Variable.set",
                side_effect=RuntimeError("boom"),
            ),
            patch(
                "builtins.input",
                side_effect=[
                    "/srv/data",
                    "y",
                    "operator@example.com",
                    "y",
                    "",
                    "n",
                    "",
                    "n",
                ],
            ),
            patch(
                "irsol_data_pipeline.cli.variables._render_variable_entries",
                return_value=None,
            ),
        ):
            result = app(
                ["prefect", "variables", "configure", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result == 3

    def test_prefect_start_sets_local_prefect_config_before_server_start(self) -> None:
        with patch(
            "irsol_data_pipeline.cli.prefect_command.subprocess.run"
        ) as mock_run:
            mock_run.side_effect = [
                subprocess.CompletedProcess(args=[], returncode=0),
            ]

            with pytest.raises(SystemExit) as exc_info:
                start_prefect_server()

        assert exc_info.value.code == 0
        assert mock_run.call_args_list == [
            ((["prefect", "server", "start"],), {"check": False}),
        ]

    def test_prefect_start_propagates_server_exit_code(self) -> None:
        with patch(
            "irsol_data_pipeline.cli.prefect_command.subprocess.run"
        ) as mock_run:
            mock_run.side_effect = [
                subprocess.CompletedProcess(args=[], returncode=7),
            ]

            with pytest.raises(SystemExit) as exc_info:
                start_prefect_server()

        assert exc_info.value.code == 7
