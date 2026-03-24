"""Tests for the piombo SFTP integration."""

from __future__ import annotations

from pathlib import PurePosixPath

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
        assert fs._base_path is None

    def test_accepts_base_path(self) -> None:
        fs = SftpRemoteFileSystem(
            hostname="host",
            username="user",
            password="pass",
            base_path="/irsol_db/docs/web-site/assets",
        )
        assert fs._base_path == PurePosixPath("/irsol_db/docs/web-site/assets")

    def test_base_path_trailing_slash_is_normalised(self) -> None:
        fs = SftpRemoteFileSystem(
            hostname="host",
            username="user",
            password="pass",
            base_path="/irsol_db/docs/web-site/assets/",
        )
        assert fs._base_path == PurePosixPath("/irsol_db/docs/web-site/assets")

    def test_empty_base_path_leaves_no_prefix(self) -> None:
        fs = SftpRemoteFileSystem(
            hostname="host", username="user", password="pass", base_path=""
        )
        assert fs._base_path is None

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


class TestSftpRemoteFileSystemResolve:
    def test_resolve_without_base_path_returns_path_unchanged(self) -> None:
        fs = SftpRemoteFileSystem(hostname="h", username="u", password="p")
        assert fs._resolve("/absolute/path/file.jpg") == "/absolute/path/file.jpg"

    def test_resolve_with_base_path_prepends_it(self) -> None:
        fs = SftpRemoteFileSystem(
            hostname="h",
            username="u",
            password="p",
            base_path="/irsol_db/docs/web-site/assets",
        )
        assert (
            fs._resolve("img_quicklook/day")
            == "/irsol_db/docs/web-site/assets/img_quicklook/day"
        )

    def test_resolve_with_base_path_and_nested_subpath(self) -> None:
        fs = SftpRemoteFileSystem(
            hostname="h",
            username="u",
            password="p",
            base_path="/irsol_db/docs/web-site/assets",
        )
        assert (
            fs._resolve("img_quicklook/210211/image.jpg")
            == "/irsol_db/docs/web-site/assets/img_quicklook/210211/image.jpg"
        )

    @pytest.mark.parametrize(
        ("base_path", "sub_path", "expected"),
        [
            ("/base", "dir/file.jpg", "/base/dir/file.jpg"),
            ("/base/", "dir/file.jpg", "/base/dir/file.jpg"),
            ("/a/b/c", "d/e", "/a/b/c/d/e"),
        ],
    )
    def test_resolve_parametrized(
        self, base_path: str, sub_path: str, expected: str
    ) -> None:
        fs = SftpRemoteFileSystem(
            hostname="h", username="u", password="p", base_path=base_path
        )
        assert fs._resolve(sub_path) == expected
