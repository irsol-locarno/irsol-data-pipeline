"""Package version helpers."""

from __future__ import annotations

import importlib.metadata

_DISTRIBUTION_NAME = "irsol-data-pipeline"


def resolve_distribution_version(
    distribution_name: str, default_version: str = "0.0.0"
) -> str:
    """Resolve the installed version of a Python distribution.

    Args:
        distribution_name: Distribution name as registered in package metadata.
        default_version: Version string to return when metadata cannot be read.

    Returns:
        Installed version string, or the provided default version when metadata cannot be read.
    """
    try:
        return importlib.metadata.version(distribution_name)
    except importlib.metadata.PackageNotFoundError:
        return default_version


__version__ = resolve_distribution_version(_DISTRIBUTION_NAME)
