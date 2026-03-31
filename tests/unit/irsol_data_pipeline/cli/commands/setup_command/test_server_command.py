"""Tests for the server_command (maintainer Prefect server profile setup)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from irsol_data_pipeline.cli.commands.setup_command.server_command import (
    DEFAULT_PREFECT_PROFILE_NAME,
    _build_sqlite_connection_url,
    _prompt_api_port,
    _prompt_database_path,
    setup_server,
)
from irsol_data_pipeline.prefect.config import PREFECT_SERVER_PORT


class TestBuildSqliteConnectionUrl:
    def test_returns_aiosqlite_scheme(self) -> None:
        url = _build_sqlite_connection_url(Path("/dati/.prefect/prefect.db"))
        assert url.startswith("sqlite+aiosqlite:///")

    def test_contains_resolved_path(self) -> None:
        url = _build_sqlite_connection_url(Path("/dati/.prefect/prefect.db"))
        assert "/dati/.prefect/prefect.db" in url


class TestPromptApiPort:
    def test_returns_default_on_empty_input(self) -> None:
        with patch("builtins.input", return_value=""):
            assert _prompt_api_port() == PREFECT_SERVER_PORT

    def test_returns_explicit_valid_port(self) -> None:
        with patch("builtins.input", return_value="4201"):
            assert _prompt_api_port() == 4201

    def test_rejects_non_integer_then_accepts(self) -> None:
        with (
            patch("builtins.input", side_effect=["abc", "4202"]),
            patch("builtins.print"),
        ):
            assert _prompt_api_port() == 4202

    @pytest.mark.parametrize("bad_port", ["0", "65536", "-1"])
    def test_rejects_out_of_range_port_then_accepts(self, bad_port: str) -> None:
        with (
            patch("builtins.input", side_effect=[bad_port, "4200"]),
            patch("builtins.print"),
        ):
            assert _prompt_api_port() == 4200


class TestPromptDatabasePath:
    def test_returns_default_when_confirmed(self) -> None:
        with patch("builtins.input", return_value="y"), patch("pathlib.Path.mkdir"):
            path = _prompt_database_path()
            from irsol_data_pipeline.cli.commands.setup_command.server_command import (
                DEFAULT_PREFECT_DATABASE_PATH,
            )

            assert path == DEFAULT_PREFECT_DATABASE_PATH

    def test_accepts_custom_path_when_default_declined(self, tmp_path: Path) -> None:
        custom_db = tmp_path / "custom.db"
        with patch("builtins.input", side_effect=["n", str(custom_db)]):
            path = _prompt_database_path()
        assert path == custom_db.resolve()

    def test_re_prompts_when_custom_path_empty(self, tmp_path: Path) -> None:
        custom_db = tmp_path / "my.db"
        with (
            patch("builtins.input", side_effect=["n", "", str(custom_db)]),
            patch("builtins.print"),
        ):
            path = _prompt_database_path()
        assert path == custom_db.resolve()


class TestSetupServer:
    def _make_mock_profiles(self, *, profile_exists: bool) -> MagicMock:
        mock = MagicMock()
        mock.names = [DEFAULT_PREFECT_PROFILE_NAME] if profile_exists else []
        return mock

    def test_creates_new_profile_when_not_existing(self, tmp_path: Path) -> None:
        mock_profiles = self._make_mock_profiles(profile_exists=False)

        with (
            patch("builtins.input", side_effect=["y", ""]),
            patch("pathlib.Path.mkdir"),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.server_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.server_command.save_profiles",
            ) as mock_save,
            patch("builtins.print"),
        ):
            result = setup_server()

        assert result == 0
        mock_profiles.add_profile.assert_called_once()
        mock_profiles.set_active.assert_called_once_with(DEFAULT_PREFECT_PROFILE_NAME)
        mock_save.assert_called_once_with(mock_profiles)

    def test_updates_existing_profile(self, tmp_path: Path) -> None:
        mock_profiles = self._make_mock_profiles(profile_exists=True)

        with (
            patch("builtins.input", side_effect=["y", ""]),
            patch("pathlib.Path.mkdir"),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.server_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.server_command.save_profiles",
            ),
            patch("builtins.print"),
        ):
            result = setup_server()

        assert result == 0
        mock_profiles.update_profile.assert_called_once()
        mock_profiles.add_profile.assert_not_called()

    def test_writes_expected_settings_keys(self, tmp_path: Path) -> None:
        mock_profiles = self._make_mock_profiles(profile_exists=False)
        captured: list[object] = []

        def capture_add(profile: object) -> None:
            captured.append(profile)

        mock_profiles.add_profile.side_effect = capture_add

        mock_profile_cls = MagicMock()

        with (
            patch("builtins.input", side_effect=["y", ""]),
            patch("pathlib.Path.mkdir"),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.server_command.load_profiles",
                return_value=mock_profiles,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.server_command.save_profiles",
            ),
            patch(
                "irsol_data_pipeline.cli.commands.setup_command.server_command.Profile",
                mock_profile_cls,
            ),
            patch("builtins.print"),
        ):
            setup_server()

        _args, kwargs = mock_profile_cls.call_args
        settings: dict = kwargs["settings"]
        from prefect.settings import (
            PREFECT_API_DATABASE_CONNECTION_URL,
            PREFECT_API_URL,
            PREFECT_SERVER_ANALYTICS_ENABLED,
        )

        assert PREFECT_API_DATABASE_CONNECTION_URL in settings
        assert PREFECT_API_URL in settings
        assert PREFECT_SERVER_ANALYTICS_ENABLED in settings
