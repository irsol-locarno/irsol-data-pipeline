"""Tests for the prefect status command."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
import requests

from irsol_data_pipeline.cli import app


class TestPrefectStatusCommand:
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
