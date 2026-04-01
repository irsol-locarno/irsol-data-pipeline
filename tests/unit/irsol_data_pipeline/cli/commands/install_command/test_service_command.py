"""Tests for the install service command (systemd unit file generation)."""

from __future__ import annotations

from pathlib import Path
from string import Template
from unittest.mock import MagicMock, patch

from irsol_data_pipeline.cli.commands.install_command.service_command import (
    _DEFAULT_SYSTEMD_DIR,
    _DEFAULT_USER,
    _FLOW_GROUP_DESCRIPTIONS,
    _FLOW_GROUP_SERVICE_NAMES,
    _SERVER_SERVICE_NAME,
    _detect_existing_services,
    _detect_idp_path,
    _generate_flow_runner_unit,
    _generate_server_unit,
    _is_service_registered,
    _prompt_flow_groups,
    _prompt_idp_path,
    _prompt_systemd_dir,
    _prompt_unix_user,
    _render_existing_services,
    _service_file_exists,
    _write_unit_file,
    install_service,
)


class TestDetectIdpPath:
    def test_returns_resolved_path_when_found(self) -> None:
        with patch("shutil.which", return_value="/usr/local/bin/idp"):
            result = _detect_idp_path()
        assert result == str(Path("/usr/local/bin/idp").resolve())

    def test_returns_fallback_when_not_found(self) -> None:
        with patch("shutil.which", return_value=None):
            result = _detect_idp_path()
        assert result == "idp"


class TestIsServiceRegistered:
    def test_returns_true_when_enabled(self) -> None:
        mock_result = MagicMock(returncode=0)
        with patch("subprocess.run", return_value=mock_result):
            assert _is_service_registered("test.service") is True

    def test_returns_false_when_not_enabled(self) -> None:
        mock_result = MagicMock(returncode=1)
        with patch("subprocess.run", return_value=mock_result):
            assert _is_service_registered("test.service") is False


class TestServiceFileExists:
    def test_returns_true_when_exists(self, tmp_path: Path) -> None:
        (tmp_path / "test.service").touch()
        assert _service_file_exists(tmp_path, "test.service") is True

    def test_returns_false_when_missing(self, tmp_path: Path) -> None:
        assert _service_file_exists(tmp_path, "test.service") is False


class TestDetectExistingServices:
    def test_returns_status_for_all_services(self, tmp_path: Path) -> None:
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command._is_service_registered",
            return_value=False,
        ):
            status = _detect_existing_services(console, tmp_path)
        expected_count = 1 + len(_FLOW_GROUP_SERVICE_NAMES)
        assert len(status) == expected_count

    def test_detects_existing_file(self, tmp_path: Path) -> None:
        (tmp_path / _SERVER_SERVICE_NAME).touch()
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command._is_service_registered",
            return_value=False,
        ):
            status = _detect_existing_services(console, tmp_path)
        assert status[_SERVER_SERVICE_NAME] is True


class TestRenderExistingServices:
    def test_prints_no_services_when_none_installed(self) -> None:
        console = MagicMock()
        status = {_SERVER_SERVICE_NAME: False}
        _render_existing_services(console, status)
        console.print.assert_called()

    def test_prints_table_when_services_installed(self) -> None:
        console = MagicMock()
        status = {_SERVER_SERVICE_NAME: True}
        _render_existing_services(console, status)
        assert console.print.call_count >= 2


class TestPromptUnixUser:
    def test_returns_custom_user(self) -> None:
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command.Prompt.ask",
            return_value="myuser",
        ):
            assert _prompt_unix_user(console) == "myuser"

    def test_default_user_for_root(self) -> None:
        console = MagicMock()
        with (
            patch("getpass.getuser", return_value="root"),
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command.Prompt.ask",
                return_value=_DEFAULT_USER,
            ) as mock_ask,
        ):
            _prompt_unix_user(console)
        _, kwargs = mock_ask.call_args
        assert kwargs["default"] == _DEFAULT_USER


class TestPromptIdpPath:
    def test_returns_prompted_path(self) -> None:
        console = MagicMock()
        with (
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command._detect_idp_path",
                return_value="/usr/bin/idp",
            ),
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command.Prompt.ask",
                return_value="/custom/bin/idp",
            ),
        ):
            assert _prompt_idp_path(console) == "/custom/bin/idp"


class TestPromptSystemdDir:
    def test_returns_default(self) -> None:
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command.Prompt.ask",
            return_value=str(_DEFAULT_SYSTEMD_DIR),
        ):
            result = _prompt_systemd_dir(console)
        assert result == _DEFAULT_SYSTEMD_DIR

    def test_returns_custom_path(self, tmp_path: Path) -> None:
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command.Prompt.ask",
            return_value=str(tmp_path),
        ):
            result = _prompt_systemd_dir(console)
        assert result == tmp_path


class TestPromptFlowGroups:
    def test_selects_all_groups(self) -> None:
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command.Confirm.ask",
            return_value=True,
        ):
            groups = _prompt_flow_groups(console)
        assert len(groups) == len(_FLOW_GROUP_SERVICE_NAMES)

    def test_selects_no_groups(self) -> None:
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command.Confirm.ask",
            return_value=False,
        ):
            groups = _prompt_flow_groups(console)
        assert groups == []


class TestGenerateServerUnit:
    def test_contains_user_and_exec_start(self) -> None:
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command._load_template",
            return_value=Template(
                "[Service]\nUser=${user}\nExecStart=${idp_executable_path} prefect start"
            ),
        ):
            content = _generate_server_unit("testuser", "/usr/bin/idp")
        assert "User=testuser" in content
        assert "ExecStart=/usr/bin/idp prefect start" in content

    def test_renders_from_real_template(self) -> None:
        content = _generate_server_unit("deploy-user", "/opt/bin/idp")
        assert "User=deploy-user" in content
        assert "ExecStart=/opt/bin/idp prefect start" in content
        assert "[Unit]" in content
        assert "[Install]" in content


class TestGenerateFlowRunnerUnit:
    def test_contains_flow_group_name(self) -> None:
        content = _generate_flow_runner_unit(
            "testuser",
            "/usr/bin/idp",
            "flat-field-correction",
        )
        assert "flat-field-correction" in content
        assert "User=testuser" in content
        assert "Requires=irsol-prefect-server.service" in content

    def test_contains_description(self) -> None:
        content = _generate_flow_runner_unit(
            "testuser",
            "/usr/bin/idp",
            "maintenance",
        )
        assert _FLOW_GROUP_DESCRIPTIONS["maintenance"] in content


class TestWriteUnitFile:
    def test_writes_new_file(self, tmp_path: Path) -> None:
        console = MagicMock()
        result = _write_unit_file(
            console,
            tmp_path,
            "test.service",
            "[Unit]\nDescription=Test",
            overwrite=False,
        )
        assert result is True
        assert (tmp_path / "test.service").read_text() == "[Unit]\nDescription=Test"

    def test_overwrites_when_flag_set(self, tmp_path: Path) -> None:
        target = tmp_path / "test.service"
        target.write_text("old content")
        console = MagicMock()
        result = _write_unit_file(
            console,
            tmp_path,
            "test.service",
            "new content",
            overwrite=True,
        )
        assert result is True
        assert target.read_text() == "new content"

    def test_skips_when_exists_and_declined(self, tmp_path: Path) -> None:
        target = tmp_path / "test.service"
        target.write_text("old content")
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command.Confirm.ask",
            return_value=False,
        ):
            result = _write_unit_file(
                console,
                tmp_path,
                "test.service",
                "new content",
                overwrite=False,
            )
        assert result is False
        assert target.read_text() == "old content"

    def test_overwrites_when_exists_and_confirmed(self, tmp_path: Path) -> None:
        target = tmp_path / "test.service"
        target.write_text("old content")
        console = MagicMock()
        with patch(
            "irsol_data_pipeline.cli.commands.install_command.service_command.Confirm.ask",
            return_value=True,
        ):
            result = _write_unit_file(
                console,
                tmp_path,
                "test.service",
                "new content",
                overwrite=False,
            )
        assert result is True
        assert target.read_text() == "new content"


class TestInstallService:
    def test_writes_server_and_flow_services(self, tmp_path: Path) -> None:
        with (
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command.Prompt.ask",
                side_effect=[
                    str(tmp_path),
                    "testuser",
                    "/usr/bin/idp",
                ],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command.Confirm.ask",
                return_value=True,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command._is_service_registered",
                return_value=False,
            ),
        ):
            result = install_service()

        assert result == 0
        assert (tmp_path / _SERVER_SERVICE_NAME).exists()
        for svc_name in _FLOW_GROUP_SERVICE_NAMES.values():
            assert (tmp_path / svc_name).exists()

    def test_no_services_selected_exits_cleanly(self, tmp_path: Path) -> None:
        with (
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command.Prompt.ask",
                side_effect=[
                    str(tmp_path),
                    "testuser",
                    "/usr/bin/idp",
                ],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command.Confirm.ask",
                return_value=False,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command._is_service_registered",
                return_value=False,
            ),
        ):
            result = install_service()

        assert result == 0
        assert not (tmp_path / _SERVER_SERVICE_NAME).exists()

    def test_generated_server_service_has_correct_content(self, tmp_path: Path) -> None:
        with (
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command.Prompt.ask",
                side_effect=[
                    str(tmp_path),
                    "deploy",
                    "/home/deploy/.local/bin/idp",
                ],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command.Confirm.ask",
                side_effect=[True, False, False, False, False],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.install_command.service_command._is_service_registered",
                return_value=False,
            ),
        ):
            install_service()

        content = (tmp_path / _SERVER_SERVICE_NAME).read_text()
        assert "User=deploy" in content
        assert "/home/deploy/.local/bin/idp prefect start" in content
