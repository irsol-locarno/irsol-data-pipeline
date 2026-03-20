"""Runtime presentation helpers for CLI entry points."""

from __future__ import annotations

import platform

from irsol_data_pipeline.version import __version__ as pipeline_version
from irsol_data_pipeline.version import (
    resolve_distribution_version,
)

TITLE_ART = r"""
    ____ ____  _____  ____   __               __        __                        _               __ _
   /  _// __ \/ ___/ / __ \ / /          ____/ /____ _ / /_ ____ _        ____   (_)____   ___   / /(_)____   ___
   / / / /_/ /\__ \ / / / // /   ______ / __  // __ `// __// __ `/______ / __ \ / // __ \ / _ \ / // // __ \ / _ \
 _/ / / _, _/___/ // /_/ // /___/_____// /_/ // /_/ // /_ / /_/ //_____// /_/ // // /_/ //  __// // // / / //  __/
/___//_/ |_|/____/ \____//_____/       \__,_/ \__,_/ \__/ \__,_/       / .___//_// .___/ \___//_//_//_/ /_/ \___/
                                                                      /_/       /_/
"""

_DISTRIBUTIONS: tuple[str, ...] = (
    "spectroflat",
    "numpy",
    "pydantic",
)


def _detect_operating_system() -> str:
    """Detect the current operating system description.

    Returns:
        Human-readable operating system description.
    """
    operating_system = platform.system()
    release = platform.release()
    machine = platform.machine()
    return " ".join(part for part in (operating_system, release, machine) if part)


def build_runtime_presentation() -> str:
    """Build the CLI runtime presentation banner.

    Returns:
        Multi-line banner containing package and runtime version information.
    """
    versions = {
        distribution_name: resolve_distribution_version(distribution_name)
        for distribution_name in _DISTRIBUTIONS
    }
    label_width = max(len(distribution_name) for distribution_name in _DISTRIBUTIONS)

    lines = [
        TITLE_ART,
        f"irsol-data-pipeline v{pipeline_version}",
        "",
        "Runtime",
        f"  {'OS':<8}: {_detect_operating_system()}",
        f"  {'Python':<8}: {platform.python_version()}",
        "",
        "Versions",
    ]
    lines.extend(
        f"  {distribution_name:<{label_width}} : {versions[distribution_name]}"
        for distribution_name in sorted(_DISTRIBUTIONS)
    )
    return "\n".join(lines)


def print_runtime_presentation() -> None:
    """Print the CLI runtime presentation banner to standard output."""
    print(build_runtime_presentation(), end="\n\n")
