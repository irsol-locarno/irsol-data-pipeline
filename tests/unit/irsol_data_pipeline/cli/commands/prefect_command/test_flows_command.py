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
