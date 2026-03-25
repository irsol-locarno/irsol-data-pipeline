"""Tests for the prefect_command module (server start command)."""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import Mock, patch

import pytest

from irsol_data_pipeline.cli.commands.prefect_command import (
    _resolve_server_port_from_active_profile,
    start_prefect_server,
)
from irsol_data_pipeline.prefect.config import PREFECT_SERVER_PORT


class TestStartPrefectServer:
    def test_prefect_start_uses_active_profile_api_port(self) -> None:
        mock_profiles = Mock()
        mock_profiles.active_profile = Mock(
            settings={"PREFECT_API_URL": "http://127.0.0.1:4201/api"}
        )

        with (
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.subprocess.run"
            ) as mock_run,
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.PREFECT_API_URL",
                "PREFECT_API_URL",
            ),
        ):
            mock_run.side_effect = [
                subprocess.CompletedProcess(args=[], returncode=0),
            ]

            with pytest.raises(SystemExit) as exc_info:
                start_prefect_server()

        assert exc_info.value.code == 0
        assert mock_run.call_args_list == [
            (
                (
                    [
                        sys.executable,
                        "-m",
                        "prefect",
                        "server",
                        "start",
                        "--port",
                        "4201",
                    ],
                ),
                {"check": False},
            ),
        ]

    @pytest.mark.parametrize(
        ("api_url", "expected_port"),
        [
            (None, PREFECT_SERVER_PORT),
            ("", PREFECT_SERVER_PORT),
            ("http://127.0.0.1/api", PREFECT_SERVER_PORT),
            ("not-a-url", PREFECT_SERVER_PORT),
            ("http://127.0.0.1:4310/api", 4310),
        ],
    )
    def test_resolve_server_port_from_active_profile(
        self,
        api_url: str | None,
        expected_port: int,
    ) -> None:
        mock_profiles = Mock()
        mock_profiles.active_profile = Mock(settings={"PREFECT_API_URL": api_url})

        with (
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.PREFECT_API_URL",
                "PREFECT_API_URL",
            ),
        ):
            assert _resolve_server_port_from_active_profile() == expected_port

    def test_resolve_server_port_from_active_profile_without_active_profile(
        self,
    ) -> None:
        mock_profiles = Mock(active_profile=None)

        with patch(
            "irsol_data_pipeline.cli.commands.prefect_command.load_profiles",
            return_value=mock_profiles,
        ):
            assert _resolve_server_port_from_active_profile() == PREFECT_SERVER_PORT

    def test_prefect_start_propagates_server_exit_code(self) -> None:
        mock_profiles = Mock()
        mock_profiles.active_profile = Mock(
            settings={"PREFECT_API_URL": "http://127.0.0.1:4201/api"}
        )

        with (
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.subprocess.run"
            ) as mock_run,
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.prefect_command.PREFECT_API_URL",
                "PREFECT_API_URL",
            ),
        ):
            mock_run.side_effect = [
                subprocess.CompletedProcess(args=[], returncode=7),
            ]

            with pytest.raises(SystemExit) as exc_info:
                start_prefect_server()

        assert exc_info.value.code == 7
