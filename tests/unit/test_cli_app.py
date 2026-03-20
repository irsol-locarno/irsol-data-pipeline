"""Tests for the Cyclopts-based CLI application."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from cyclopts import App
from cyclopts.exceptions import ValidationError

from irsol_data_pipeline.cli.app import app


class TestCliApp:
    def test_root_commands_are_registered(self) -> None:
        assert isinstance(app["flows"], App)
        assert isinstance(app["variables"], App)
        assert "info" in set(app)

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

    def test_flows_list_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        app(
            ["flows", "list", "--format", "json", "--no-banner"],
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

    def test_flows_serve_requires_selection(self) -> None:
        with pytest.raises(ValidationError):
            app(
                ["flows", "serve", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

    def test_flows_serve_rejects_all_plus_explicit_group(self) -> None:
        with pytest.raises(ValidationError):
            app(
                [
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

    def test_variables_list_json(self, capsys: pytest.CaptureFixture[str]) -> None:
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
                ["variables", "list", "--format", "json", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)

        assert payload["variables"][0]["value"] == "/srv/data"
        assert payload["variables"][1]["value"] == "observer@example.com"
        assert payload["variables"][3]["status"] == "unset"

    def test_variables_configure_returns_zero_when_skipping_all(
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
                ["variables", "configure", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result == 0

    def test_variables_configure_returns_three_on_failure(self) -> None:
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
                ["variables", "configure", "--no-banner"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result == 3
