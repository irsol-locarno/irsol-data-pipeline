"""Integrations sub-package.

Contains concrete adapters that satisfy the :class:`~irsol_data_pipeline.core.remote_filesystem.RemoteFileSystem`
protocol.  Each adapter encapsulates a specific third-party transport library
(e.g. Paramiko / SFTP) and can be instantiated with the required credentials
when a Prefect flow starts.

The ``core`` and ``pipeline`` packages depend **only** on the
:class:`~irsol_data_pipeline.core.remote_filesystem.RemoteFileSystem`
protocol; they must never import from this package directly.
"""
