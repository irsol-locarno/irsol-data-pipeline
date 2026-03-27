"""Tests for the prefect automations command."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from irsol_data_pipeline.cli import app
from irsol_data_pipeline.cli.commands.prefect_command.automations_command import (
    AutomationReportEntry,
    configure_automations,
    list_automations,
)
from irsol_data_pipeline.prefect.automations import AUTOMATIONS


class TestListAutomations:
    def test_list_automations_table_shows_deployed(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        mock_automation = MagicMock()
        mock_automation.name = "Crash zombie flows"
        mock_automation.description = "Some description"

        with (
            patch(
                "prefect.automations.Automation.read",
                return_value=mock_automation,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.automations_command._render_automation_entries"
            ) as mock_render,
        ):
            list_automations()

        mock_render.assert_called_once()
        entries: list[AutomationReportEntry] = mock_render.call_args[0][0]
        assert all(e.deployed for e in entries)

    def test_list_automations_table_shows_undeployed_on_server_error(self) -> None:
        with (
            patch(
                "prefect.automations.Automation.read",
                side_effect=RuntimeError("not found"),
            ),
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.automations_command._render_automation_entries"
            ) as mock_render,
        ):
            list_automations()

        entries: list[AutomationReportEntry] = mock_render.call_args[0][0]
        assert all(not e.deployed for e in entries)

    def test_list_automations_json_output(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        mock_automation = MagicMock()
        mock_automation.name = "Crash zombie flows"
        mock_automation.description = "desc"

        with patch(
            "prefect.automations.Automation.read",
            return_value=mock_automation,
        ):
            app(
                ["prefect", "automations", "list", "--format", "json"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)
        assert "automations" in payload
        assert len(payload["automations"]) == 2
        assert payload["automations"][0]["deployed"] is True

    def test_list_automations_json_undeployed(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "prefect.automations.Automation.read",
            side_effect=RuntimeError("not found"),
        ):
            app(
                ["prefect", "automations", "list", "--format", "json"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        payload = json.loads(capsys.readouterr().out)
        assert all(not a["deployed"] for a in payload["automations"])


class TestConfigureAutomations:
    def test_updates_automations_when_already_deployed(self) -> None:
        mock_existing = MagicMock()

        with (
            patch(
                "irsol_data_pipeline.prefect.automations.get_automation",
                return_value=mock_existing,
            ),
            patch("builtins.print"),
        ):
            result = configure_automations()

        assert result == 0
        assert mock_existing.update.call_count == len(AUTOMATIONS)

    def test_configure_automations_via_app(self) -> None:
        mock_existing = MagicMock()

        with (
            patch(
                "irsol_data_pipeline.prefect.automations.get_automation",
                return_value=mock_existing,
            ),
            patch("builtins.print"),
        ):
            result = app(
                ["prefect", "automations", "configure"],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result == 0
