"""Tests for the setup_command (user Prefect client setup)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from irsol_data_pipeline.cli.commands.setup_command import (
    DEFAULT_PREFECT_PROFILE_NAME,
    _prompt_server_host,
    _prompt_server_port,
    setup,
)
from irsol_data_pipeline.prefect.config import PREFECT_SERVER_HOST, PREFECT_SERVER_PORT


class TestPromptServerHost:
    def test_returns_default_on_empty_input(self) -> None:
        with patch("builtins.input", return_value=""):
            assert _prompt_server_host() == PREFECT_SERVER_HOST

    def test_returns_explicit_host(self) -> None:
        with patch("builtins.input", return_value="192.168.1.10"):
            assert _prompt_server_host() == "192.168.1.10"


class TestPromptServerPort:
    def test_returns_default_on_empty_input(self) -> None:
        with patch("builtins.input", return_value=""):
            assert _prompt_server_port() == PREFECT_SERVER_PORT

    def test_returns_explicit_valid_port(self) -> None:
        with patch("builtins.input", return_value="4201"):
            assert _prompt_server_port() == 4201

    def test_rejects_non_integer_then_accepts(self) -> None:
        with (
            patch("builtins.input", side_effect=["abc", "4202"]),
            patch("builtins.print"),
        ):
            assert _prompt_server_port() == 4202

    @pytest.mark.parametrize("bad_port", ["0", "65536", "-1"])
    def test_rejects_out_of_range_port_then_accepts(self, bad_port: str) -> None:
        with (
            patch("builtins.input", side_effect=[bad_port, "4200"]),
            patch("builtins.print"),
        ):
            assert _prompt_server_port() == 4200


class TestSetup:
    def _make_mock_profiles(self, *, profile_exists: bool) -> MagicMock:
        mock = MagicMock()
        mock.names = [DEFAULT_PREFECT_PROFILE_NAME] if profile_exists else []
        return mock

    def test_creates_new_profile_when_not_existing(self) -> None:
        mock_profiles = self._make_mock_profiles(profile_exists=False)

        with (
            patch("builtins.input", side_effect=["", ""]),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.save_profiles"
            ) as mock_save,
            patch("builtins.print"),
        ):
            result = setup()

        assert result == 0
        mock_profiles.add_profile.assert_called_once()
        mock_profiles.set_active.assert_called_once_with(DEFAULT_PREFECT_PROFILE_NAME)
        mock_save.assert_called_once_with(mock_profiles)

    def test_updates_existing_profile(self) -> None:
        mock_profiles = self._make_mock_profiles(profile_exists=True)

        with (
            patch("builtins.input", side_effect=["", ""]),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch("irsol_data_pipeline.cli.commands.setup_command.save_profiles"),
            patch("builtins.print"),
        ):
            result = setup()

        assert result == 0
        mock_profiles.update_profile.assert_called_once()
        mock_profiles.add_profile.assert_not_called()

    def test_does_not_write_database_settings(self) -> None:
        """Client setup must not write DB connection URL."""
        mock_profiles = self._make_mock_profiles(profile_exists=False)
        mock_profile_cls = MagicMock()

        with (
            patch("builtins.input", side_effect=["", ""]),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch("irsol_data_pipeline.cli.commands.setup_command.save_profiles"),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.Profile",
                mock_profile_cls,
            ),
            patch("builtins.print"),
        ):
            setup()

        _args, kwargs = mock_profile_cls.call_args
        settings: dict = kwargs["settings"]
        from prefect.settings import PREFECT_API_DATABASE_CONNECTION_URL

        assert PREFECT_API_DATABASE_CONNECTION_URL not in settings

    def test_writes_api_url_with_custom_host_and_port(self) -> None:
        mock_profiles = self._make_mock_profiles(profile_exists=False)
        mock_profile_cls = MagicMock()

        with (
            patch("builtins.input", side_effect=["192.168.1.5", "4210"]),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch("irsol_data_pipeline.cli.commands.setup_command.save_profiles"),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.Profile",
                mock_profile_cls,
            ),
            patch("builtins.print"),
        ):
            setup()

        _args, kwargs = mock_profile_cls.call_args
        settings: dict = kwargs["settings"]
        from prefect.settings import PREFECT_API_URL

        assert settings[PREFECT_API_URL] == "http://192.168.1.5:4210/api"
