"""Tests for the Cyclopts-based CLI application."""

from __future__ import annotations

import json
import subprocess
import sys
from unittest.mock import patch

import pytest
import requests
from cyclopts import App
from cyclopts.exceptions import ValidationError

from irsol_data_pipeline.cli import _meta, app
from irsol_data_pipeline.cli.commands.prefect_command import start_prefect_server
from irsol_data_pipeline.prefect.config import PREFECT_PROFILE_SETTINGS
from irsol_data_pipeline.prefect.variables import PrefectVariableName


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
        assert "variables" in output

    def test_prefect_status_json_when_dashboard_is_reachable(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "irsol_data_pipeline.cli.commands.prefect_command.status_command.requests.get"
        ) as mock_get:
            mock_get.return_value.ok = True
            mock_get.return_value.status_code = 200

            result = app(
                ["prefect", "status", "--format", "json"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert result == 0
        assert payload == {
            "dashboard_url": "http://127.0.0.1:4200",
            "detail": "Prefect dashboard is reachable on the expected port.",
            "healthcheck_url": "http://127.0.0.1:4200/api/health",
            "host": "127.0.0.1",
            "http_status": 200,
            "port": 4200,
            "reachable": True,
            "status": "running",
        }
        mock_get.assert_called_once_with(
            "http://127.0.0.1:4200/api/health",
            timeout=5.0,
        )

    def test_prefect_status_returns_one_when_dashboard_is_unreachable(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "irsol_data_pipeline.cli.commands.prefect_command.status_command.requests.get",
            side_effect=requests.ConnectionError("connection refused"),
        ):
            result = app(
                ["prefect", "status", "--format", "json"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert result == 1
        assert payload["status"] == "unreachable"
        assert payload["reachable"] is False
        assert payload["http_status"] is None
        assert "connection refused" in payload["detail"]

    def test_prefect_status_accepts_host_and_port_overrides(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "irsol_data_pipeline.cli.commands.prefect_command.status_command.requests.get"
        ) as mock_get:
            mock_get.return_value.ok = True
            mock_get.return_value.status_code = 200

            result = app(
                [
                    "prefect",
                    "status",
                    "--format",
                    "json",
                    "--host",
                    "prefect.internal",
                    "--port",
                    "4300",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert result == 0
        assert payload["dashboard_url"] == "http://prefect.internal:4300"
        assert payload["healthcheck_url"] == "http://prefect.internal:4300/api/health"
        assert payload["host"] == "prefect.internal"
        assert payload["port"] == 4300
        mock_get.assert_called_once_with(
            "http://prefect.internal:4300/api/health",
            timeout=5.0,
        )

    def test_prefect_status_deep_analysis_json(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with (
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.status_command.requests.get"
            ) as mock_get,
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.status_command._analyze_running_flows_and_tasks"
            ) as mock_analyze,
        ):
            mock_get.return_value.ok = True
            mock_get.return_value.status_code = 200
            mock_analyze.return_value.model_dump.return_value = {
                "detail": "Collected running flow and task details from Prefect SDK.",
                "flow_run_count": 1,
                "running_task_count": 2,
                "running_flows": [
                    {
                        "flow_id": "f-1",
                        "flow_name": "ff-correction-full",
                        "flow_run_id": "fr-1",
                        "flow_run_name": "ff-run-1",
                        "running_task_count": 2,
                        "running_task_names": ["process-measurement", "scan-day"],
                        "state": "Running",
                    }
                ],
            }

            result = app(
                [
                    "prefect",
                    "status",
                    "--format",
                    "json",
                    "--deep-analysis",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert result == 0
        assert payload["deep_analysis"]["flow_run_count"] == 1
        assert payload["deep_analysis"]["running_task_count"] == 2
        assert payload["deep_analysis"]["running_flows"][0] == {
            "flow_id": "f-1",
            "flow_name": "ff-correction-full",
            "flow_run_id": "fr-1",
            "flow_run_name": "ff-run-1",
            "running_task_count": 2,
            "running_task_names": ["process-measurement", "scan-day"],
            "state": "Running",
        }

    def test_prefect_status_deep_analysis_reports_failure(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with (
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.status_command.requests.get"
            ) as mock_get,
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.status_command._analyze_running_flows_and_tasks",
                side_effect=RuntimeError("sdk unavailable"),
            ),
        ):
            mock_get.return_value.ok = True
            mock_get.return_value.status_code = 200

            result = app(
                [
                    "prefect",
                    "status",
                    "--format",
                    "json",
                    "--deep-analysis",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert result == 0
        assert payload["deep_analysis"]["flow_run_count"] == 0
        assert payload["deep_analysis"]["running_task_count"] == 0
        assert payload["deep_analysis"]["running_flows"] == []
        assert "Failed to run deep analysis" in payload["deep_analysis"]["detail"]

    def test_prefect_flows_list_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        app(
            ["prefect", "flows", "list", "--format", "json"],
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
                ["prefect", "flows", "serve"],
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
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

    def test_info_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        value_by_name = {
            PrefectVariableName.DATA_ROOT_PATH: "/srv/data",
            PrefectVariableName.JSOC_EMAIL: "operator@example.com",
            PrefectVariableName.JSOC_DATA_DELAY_DAYS: "14",
            PrefectVariableName.CACHE_EXPIRATION_HOURS: "672",
            PrefectVariableName.FLOW_RUN_EXPIRATION_HOURS: "<unset>",
        }

        with patch(
            "irsol_data_pipeline.prefect.variables.get_variable",
            side_effect=value_by_name.__getitem__,
        ):
            app(
                ["info", "--format", "json"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert payload["version"]
        assert payload["prefect_variables"][0] == {
            "name": "data-root-path",
            "value": "/srv/data",
        }

    def test_prefect_variables_list_json(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        value_by_name = {
            PrefectVariableName.DATA_ROOT_PATH: "/srv/data",
            PrefectVariableName.JSOC_EMAIL: "observer@example.com",
            PrefectVariableName.JSOC_DATA_DELAY_DAYS: "10",
            PrefectVariableName.CACHE_EXPIRATION_HOURS: "672",
            PrefectVariableName.FLOW_RUN_EXPIRATION_HOURS: "<unset>",
        }

        with patch(
            "irsol_data_pipeline.prefect.variables.get_variable",
            side_effect=value_by_name.__getitem__,
        ):
            app(
                ["prefect", "variables", "list", "--format", "json"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert payload["variables"][0]["value"] == "/srv/data"
        assert payload["variables"][1]["value"] == "observer@example.com"
        assert payload["variables"][2]["value"] == "10"
        assert payload["variables"][4]["value"] == "<unset>"

    def test_prefect_variables_configure_returns_zero_when_skipping_all(
        self,
    ) -> None:
        with (
            patch("prefect.variables.Variable.get", return_value=None),
            patch("prefect.variables.Variable.set"),
            patch(
                "builtins.input",
                side_effect=["", "", "", "n", "", "n", "", "n"],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.variables_command._render_variable_entries",
                return_value=None,
            ),
        ):
            result = app(
                ["prefect", "variables", "configure"],
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
                    "",
                    "n",
                ],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.variables_command._render_variable_entries",
                return_value=None,
            ),
        ):
            result = app(
                ["prefect", "variables", "configure"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result == 3

    def test_prefect_start_sets_local_prefect_config_before_server_start(self) -> None:
        with (
            patch("prefect.settings.profiles.update_current_profile") as mock_update,
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.subprocess.run"
            ) as mock_run,
        ):
            mock_run.side_effect = [
                subprocess.CompletedProcess(args=[], returncode=0),
            ]

            with pytest.raises(SystemExit) as exc_info:
                start_prefect_server()

        assert exc_info.value.code == 0
        mock_update.assert_called_once_with(PREFECT_PROFILE_SETTINGS)
        assert mock_run.call_args_list == [
            (([sys.executable, "-m", "prefect", "server", "start"],), {"check": False}),
        ]

    def test_prefect_start_propagates_server_exit_code(self) -> None:
        with (
            patch("prefect.settings.profiles.update_current_profile") as mock_update,
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.subprocess.run"
            ) as mock_run,
        ):
            mock_run.side_effect = [
                subprocess.CompletedProcess(args=[], returncode=7),
            ]

            with pytest.raises(SystemExit) as exc_info:
                start_prefect_server()

        assert exc_info.value.code == 7
        mock_update.assert_called_once_with(PREFECT_PROFILE_SETTINGS)
