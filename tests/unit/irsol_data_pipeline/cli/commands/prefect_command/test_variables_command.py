"""Tests for the prefect variables command."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from irsol_data_pipeline.cli import app
from irsol_data_pipeline.prefect.variables import PrefectVariableName


class TestPrefectVariablesCommand:
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
