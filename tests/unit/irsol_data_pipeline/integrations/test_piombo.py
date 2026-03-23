"""Tests for the piombo SFTP integration."""

from __future__ import annotations

import pytest

from irsol_data_pipeline.integrations.piombo import SftpRemoteFileSystem


class TestSftpRemoteFileSystemValidation:
    def test_raises_when_hostname_missing(self) -> None:
        with pytest.raises(ValueError, match="all be provided together"):
            SftpRemoteFileSystem(hostname="", username="user", password="pass")

    def test_raises_when_username_missing(self) -> None:
        with pytest.raises(ValueError, match="all be provided together"):
            SftpRemoteFileSystem(hostname="host", username="", password="pass")

    def test_raises_when_password_missing(self) -> None:
        with pytest.raises(ValueError, match="all be provided together"):
            SftpRemoteFileSystem(hostname="host", username="user", password="")

    def test_raises_when_all_credentials_missing(self) -> None:
        with pytest.raises(ValueError, match="all be provided together"):
            SftpRemoteFileSystem(hostname="", username="", password="")

    def test_accepts_valid_credentials(self) -> None:
        fs = SftpRemoteFileSystem(hostname="host", username="user", password="pass")
        assert fs._hostname == "host"
        assert fs._username == "user"
        assert fs._password == "pass"
        assert fs._sftp_client is None

    @pytest.mark.parametrize(
        ("hostname", "username", "password"),
        [
            ("piombo7.usi.ch", "u", ""),
            ("piombo7.usi.ch", "", "p"),
            ("", "u", "p"),
        ],
    )
    def test_raises_on_partial_credentials(
        self, hostname: str, username: str, password: str
    ) -> None:
        with pytest.raises(ValueError, match="all be provided together"):
            SftpRemoteFileSystem(
                hostname=hostname, username=username, password=password
            )


class TestSftpRemoteFileSystemContextManager:
    def test_enter_returns_self(self) -> None:
        fs = SftpRemoteFileSystem(hostname="h", username="u", password="p")
        result = fs.__enter__()
        assert result is fs

    def test_exit_calls_close(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fs = SftpRemoteFileSystem(hostname="h", username="u", password="p")
        closed = []
        monkeypatch.setattr(fs, "close", lambda: closed.append(True))
        fs.__exit__(None, None, None)
        assert closed == [True]

    def test_close_is_idempotent(self) -> None:
        fs = SftpRemoteFileSystem(hostname="h", username="u", password="p")
        # close() on a never-connected instance should not raise
        fs.close()
        fs.close()
