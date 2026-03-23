"""Tests for the info CLI command."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from irsol_data_pipeline.cli import app
from irsol_data_pipeline.prefect.variables import PrefectVariableName


class TestInfoCommand:
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
