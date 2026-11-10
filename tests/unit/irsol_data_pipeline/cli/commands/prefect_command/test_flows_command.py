"""Tests for the prefect flows command."""

from __future__ import annotations

import json

import pytest
from cyclopts.exceptions import ValidationError

from irsol_data_pipeline.cli import app


class TestPrefectFlowsCommand:
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
            "web-assets-compatibility",
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


class TestMaintenanceDeploymentSchedules:
    def test_cache_cleanup_runs_outside_flow_run_cleanup_window(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``cache-cleanup`` must not overlap the flow-run deletion window.

        ``prefect-run-cleanup`` runs daily at 00:00 UTC and deletes ~1.3k
        flow runs for up to ~13 minutes, briefly locking the SQLite
        database. ``cache-cleanup`` used to start at 00:30 UTC, inside
        that window, and failed with retryable HTTP 503s.
        """
        import importlib

        monkeypatch.setenv("PREFECT_ENABLED", "1")
        from irsol_data_pipeline.prefect.flows.maintenance import (
            delete_old_cache_files as cache_module,
        )

        importlib.reload(cache_module)

        from irsol_data_pipeline.cli.commands.prefect_command.flows_command import (
            _build_maintenance_deployments,
        )

        deployments = _build_maintenance_deployments()
        crons = {
            deployment.name: deployment.schedules[0].schedule.cron
            for deployment in deployments
        }

        assert crons == {
            "prefect-run-cleanup": "0 0 * * *",
            "cache-cleanup": "0 3 * * *",
        }
