"""Package version helpers."""

from __future__ import annotations

import importlib.metadata

_DISTRIBUTION_NAME = "irsol-data-pipeline"
_DISTRIBUTION_NAME_CLI = "irsol-data-pipeline-cli"

_RELEVANT_DISTRIBUTIONS: tuple[str, ...] = (
    "astropy",
    "drms",
    "numpy",
    "pydantic",
    "spectroflat",
    "sunpy",
)


def resolve_distribution_version(
    distribution_name: str,
    default_version: str = "0.0.0",
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


def distribution_versions() -> dict[str, str]:
    """Get the versions of relevant distributions.

    Returns:
        Mapping of distribution names to their resolved versions.
    """
    return {
        distribution_name: resolve_distribution_version(distribution_name)
        for distribution_name in sorted(_RELEVANT_DISTRIBUTIONS)
    }


def _resolve_own_version(default_version: str = "0.0.0") -> str:
    """Resolve the version of this package.

    Tries ``irsol-data-pipeline`` first; if that distribution is not found
    (e.g. when only the ``irsol-data-pipeline-cli`` wheel is installed),
    falls back to ``irsol-data-pipeline-cli``, then to *default_version*.

    Args:
        default_version: Version string to return when neither distribution
            metadata can be read.

    Returns:
        Resolved version string.
    """
    try:
        return importlib.metadata.version(_DISTRIBUTION_NAME)
    except importlib.metadata.PackageNotFoundError:
        pass
    try:
        return importlib.metadata.version(_DISTRIBUTION_NAME_CLI)
    except importlib.metadata.PackageNotFoundError:
        return default_version


__version__ = _resolve_own_version()
__relevant_distribution_versions__ = tuple(
    (v, resolve_distribution_version(v)) for v in sorted(_RELEVANT_DISTRIBUTIONS)
)
