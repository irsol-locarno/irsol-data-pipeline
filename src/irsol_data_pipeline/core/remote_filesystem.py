"""Protocol definition for remote file-system access.

This module defines the :class:`RemoteFileSystem` structural protocol.  The
``core`` and ``pipeline`` packages depend **only** on this interface; concrete
transport implementations (SFTP / Piombo) live in the ``integrations``
sub-package.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RemoteFileSystem(Protocol):
    """Structural protocol for a remote file-system used by the pipeline.

    Any class that implements :meth:`ensure_dir`, :meth:`file_exists`, and
    :meth:`upload_file` with the signatures below satisfies this protocol
    without requiring an explicit inheritance relationship.
    """

    def ensure_dir(self, remote_dir: str) -> None:
        """Ensure that *remote_dir* exists on the remote host.

        Creates the directory—and any missing parents—when it does not already
        exist.

        Args:
            remote_dir: Absolute POSIX path of the directory to create.
        """

    def file_exists(self, remote_path: str) -> bool:
        """Return whether *remote_path* already exists on the remote host.

        Args:
            remote_path: Absolute POSIX path to test.

        Returns:
            ``True`` if the path exists, ``False`` otherwise.
        """

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload a local file to the remote host.

        Args:
            local_path: Absolute local path of the file to upload.
            remote_path: Absolute POSIX destination path on the remote host.

        Raises:
            WebAssetUploadError: If the transfer fails.
        """
