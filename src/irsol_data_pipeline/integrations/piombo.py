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
        password="secret",  # gitleaks:allow
        base_path="/irsol_db/docs/web-site/assets",
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

import paramiko
from loguru import logger

from irsol_data_pipeline.exceptions import WebAssetUploadError


class SftpRemoteFileSystem:
    """Paramiko SFTP adapter satisfying the ``RemoteFileSystem`` protocol.

    The underlying transport is opened lazily on the first method call and
    kept alive for the lifetime of the object.  Use the class as a context
    manager to guarantee the connection is closed when it is no longer needed.

    The connection is established via :class:`paramiko.Transport` with
    username/password authentication, mirroring the approach used in the
    quick-look ``deploy_images.py`` script.

    Args:
        hostname: SSH hostname of the remote server.
        username: SSH login username.
        password: SSH login password.
        base_path: Optional absolute POSIX path that is prepended to every
            path argument passed to :meth:`ensure_dir`, :meth:`file_exists`,
            and :meth:`upload_file`.  Callers therefore only need to supply
            sub-paths relative to this root.  Defaults to ``""`` (no
            prefix — callers must supply full absolute paths themselves).

    Raises:
        ValueError: If any of *hostname*, *username*, or *password* is an
            empty string.

    Example::

        with SftpRemoteFileSystem(
            "host", "user", "pass",
            base_path="/irsol_db/docs/web-site/assets",
        ) as fs:
            fs.ensure_dir("img_quicklook/210211")
            fs.upload_file("/local/file.jpg", "img_quicklook/210211/file.jpg")
    """

    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        base_path: str = "",
    ) -> None:
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
        self._base_path: PurePosixPath | None = (
            PurePosixPath(base_path) if base_path.strip() else None
        )
        self._transport: paramiko.Transport | None = None
        self._sftp_client: paramiko.SFTPClient | None = None

        self._logger = logger.bind(hostname=hostname, username=username)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------
    def _resolve(self, path: str) -> str:
        """Prepend *base_path* to *path* if a base path is configured.

        Args:
            path: Relative POSIX sub-path when *base_path* is set, or
                an absolute POSIX path when no *base_path* was configured.

        Returns:
            The fully resolved POSIX path string.
        """
        if self._base_path is None:
            return path
        return str(self._base_path / path)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """Open the SSH and SFTP connections if they are not yet active."""
        if self._sftp_client is not None:
            return

        self._logger.debug("Opening SFTP connection")
        try:
            self._transport = paramiko.Transport((self._hostname, 22))
            self._transport.connect(username=self._username, password=self._password)
            self._sftp_client = paramiko.SFTPClient.from_transport(self._transport)
        except Exception as exc:
            self.close()
            raise WebAssetUploadError(
                f"Failed to open SFTP connection to {self._hostname}"
            ) from exc
        self._logger.info("SFTP connection established")

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

        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:  # pragma: no cover
                pass
            self._transport = None

        self._logger.info("SFTP connection closed")

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
            remote_dir: POSIX path to create.  Relative to *base_path* when
                one was provided at construction time; absolute otherwise.
        """
        self._connect()

        resolved = self._resolve(remote_dir)
        if not resolved:
            return

        path_builder = (
            PurePosixPath("/") if resolved.startswith("/") else PurePosixPath()
        )
        for part in PurePosixPath(resolved).parts:
            if part == "/":
                continue
            path_builder = path_builder / part
            candidate = str(path_builder)
            try:
                self._sftp_client.stat(candidate)
            except OSError:
                self._logger.debug("Creating remote directory", directory=candidate)
                self._sftp_client.mkdir(candidate)

    def file_exists(self, remote_path: str) -> bool:
        """Return whether *remote_path* exists on the remote host.

        Args:
            remote_path: POSIX path to test.  Relative to *base_path* when
                one was provided at construction time; absolute otherwise.

        Returns:
            ``True`` if the path exists, ``False`` otherwise.
        """
        self._connect()
        resolved = self._resolve(remote_path)
        try:
            self._sftp_client.stat(resolved)
            self._logger.debug("Remote file exists", path=resolved)
            return True
        except OSError:
            self._logger.debug("Remote file does not exist", path=resolved)
            return False

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload *local_path* to *remote_path* on the remote host.

        Args:
            local_path: Absolute local file path to upload.
            remote_path: POSIX destination path on the remote host.  Relative
                to *base_path* when one was provided at construction time;
                absolute otherwise.

        Raises:
            WebAssetUploadError: If the upload fails.
        """
        self._connect()
        resolved = self._resolve(remote_path)
        try:
            self._logger.debug(
                "Uploading file to remote host",
                local_path=local_path,
                remote_path=resolved,
            )
            self._sftp_client.put(local_path, resolved)
            self._logger.debug(
                "File upload successful", local_path=local_path, remote_path=resolved
            )
        except Exception as exc:
            raise WebAssetUploadError(
                f"Failed to upload {local_path} to {self._hostname}:{resolved}"
            ) from exc
