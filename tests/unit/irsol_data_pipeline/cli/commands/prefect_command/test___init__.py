"""Tests for the prefect_command module (server start command)."""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import patch

import pytest

from irsol_data_pipeline.cli.commands.prefect_command import start_prefect_server
from irsol_data_pipeline.prefect.config import PREFECT_PROFILE_SETTINGS


class TestStartPrefectServer:
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
