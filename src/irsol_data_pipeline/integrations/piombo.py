"""Paramiko-based SFTP implementation of the RemoteFileSystem protocol.

This module provides :class:`SftpRemoteFileSystem`, a concrete adapter that
connects to a remote host via SSH/SFTP using the *Paramiko* library and
satisfies the
:class:`~irsol_data_pipeline.core.remote_filesystem.RemoteFileSystem`
protocol.

Usage example::

    with SftpRemoteFileSystem(
        hostname="piombo7.usi.ch",
        username="user",
        password="secret",
    ) as remote_fs:
        result = process_day_web_asset_compatibility(
            day=day,
            ...,
            remote_fs=remote_fs,
        )
"""

from __future__ import annotations

from pathlib import PurePosixPath
from types import TracebackType
from typing import Any

import paramiko
from loguru import logger

from irsol_data_pipeline.exceptions import WebAssetUploadError


class SftpRemoteFileSystem:
    """Paramiko SFTP adapter satisfying the ``RemoteFileSystem`` protocol.

    The underlying SSH connection is opened lazily on the first method call
    and kept alive for the lifetime of the object.  Use the class as a
    context manager to guarantee the connection is closed when it is no
    longer needed.

    The adapter loads the system host keys (``~/.ssh/known_hosts``) and uses
    :class:`paramiko.RejectPolicy`, so the remote host must already be present
    in the operator's known-hosts file before connecting.

    Args:
        hostname: SSH hostname of the remote server.
        username: SSH login username.
        password: SSH login password.

    Raises:
        ValueError: If any of *hostname*, *username*, or *password* is an
            empty string.

    Example::

        with SftpRemoteFileSystem("host", "user", "pass") as fs:
            fs.ensure_dir("/remote/path")
            fs.upload_file("/local/file.jpg", "/remote/path/file.jpg")
    """

    def __init__(self, hostname: str, username: str, password: str) -> None:
        provided = [
            bool(hostname.strip()),
            bool(username.strip()),
            bool(password.strip()),
        ]
        if not all(provided):
            raise ValueError(
                "SftpRemoteFileSystem requires hostname, username, and password "
                "to all be provided together"
            )

        self._hostname = hostname
        self._username = username
        self._password = password
        self._ssh_client: paramiko.SSHClient | None = None
        self._sftp_client: Any | None = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """Open the SSH and SFTP connections if they are not yet active."""
        if self._sftp_client is not None:
            return

        logger.debug(
            "Opening SFTP connection",
            hostname=self._hostname,
            username=self._username,
        )
        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.load_system_host_keys()
        self._ssh_client.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            self._ssh_client.connect(
                hostname=self._hostname,
                username=self._username,
                password=self._password,
            )
            self._sftp_client = self._ssh_client.open_sftp()
        except Exception as exc:
            self.close()
            raise WebAssetUploadError(
                f"Failed to open SFTP connection to {self._hostname}"
            ) from exc

    def close(self) -> None:
        """Close the SFTP and SSH connections.

        Safe to call multiple times; subsequent calls are no-ops.
        """
        if self._sftp_client is not None:
            try:
                self._sftp_client.close()
            except Exception:  # pragma: no cover
                pass
            self._sftp_client = None

        if self._ssh_client is not None:
            try:
                self._ssh_client.close()
            except Exception:  # pragma: no cover
                pass
            self._ssh_client = None

    def __enter__(self) -> SftpRemoteFileSystem:
        """Support use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the connection when leaving the ``with`` block."""
        self.close()

    # ------------------------------------------------------------------
    # RemoteFileSystem protocol implementation
    # ------------------------------------------------------------------

    def ensure_dir(self, remote_dir: str) -> None:
        """Create *remote_dir* and any missing parents on the remote host.

        Args:
            remote_dir: Absolute POSIX path to create.
        """
        self._connect()
        sftp = self._sftp_client

        if not remote_dir:
            return

        path_builder = (
            PurePosixPath("/") if remote_dir.startswith("/") else PurePosixPath()
        )
        for part in PurePosixPath(remote_dir).parts:
            if part == "/":
                continue
            path_builder = path_builder / part
            candidate = str(path_builder)
            try:
                sftp.stat(candidate)
            except OSError:
                sftp.mkdir(candidate)

    def file_exists(self, remote_path: str) -> bool:
        """Return whether *remote_path* exists on the remote host.

        Args:
            remote_path: Absolute POSIX path to test.

        Returns:
            ``True`` if the path exists, ``False`` otherwise.
        """
        self._connect()
        try:
            self._sftp_client.stat(remote_path)
            return True
        except OSError:
            return False

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload *local_path* to *remote_path* on the remote host.

        Args:
            local_path: Absolute local file path to upload.
            remote_path: Absolute POSIX destination path on the remote host.

        Raises:
            WebAssetUploadError: If the upload fails.
        """
        self._connect()
        try:
            self._sftp_client.put(local_path, remote_path)
        except Exception as exc:
            raise WebAssetUploadError(
                f"Failed to upload {local_path} to {self._hostname}:{remote_path}"
            ) from exc
