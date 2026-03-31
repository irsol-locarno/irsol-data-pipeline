"""Runtime presentation helpers for CLI entry points."""

from __future__ import annotations

import platform
import shutil

from irsol_data_pipeline.version import (
    __relevant_distribution_versions__ as relevant_distribution_versions,
)
from irsol_data_pipeline.version import __version__ as pipeline_version

TITLE_ART = r"""
    ____ ____  _____  ____   __               __        __                        _               __ _
   /  _// __ \/ ___/ / __ \ / /          ____/ /____ _ / /_ ____ _        ____   (_)____   ___   / /(_)____   ___
   / / / /_/ /\__ \ / / / // /   ______ / __  // __ `// __// __ `/______ / __ \ / // __ \ / _ \ / // // __ \ / _ \
 _/ / / _, _/___/ // /_/ // /___/_____// /_/ // /_/ // /_ / /_/ //_____// /_/ // // /_/ //  __// // // / / //  __/
/___//_/ |_|/____/ \____//_____/       \__,_/ \__,_/ \__/ \__,_/       / .___//_// .___/ \___//_//_//_/ /_/ \___/
                                                                      /_/       /_/
"""

COMPACT_TITLE = r"""
             _  ___  ___  ___  _
            | || . \/ __>| . || |
            | ||   /\__ \| | || |_
   _        |_||_\_\<___/`___'|___|         _  _
 _| | ___ _| |_ ___  ___  ___ <_> ___  ___ | |<_>._ _  ___
/ . |<_> | | | <_> ||___|| . \| || . \/ ._>| || || ' |/ ._>
\___|<___| |_| <___|     |  _/|_||  _/\___.|_||_||_|_|\___.
                         |_|     |_|
"""

TITLE_VARIANTS: tuple[str, ...] = (
    TITLE_ART,
    COMPACT_TITLE,
)


def _detect_terminal_columns() -> int:
    """Detect terminal width in columns.

    Returns:
        Number of terminal columns, using a fallback when width is unavailable.
    """
    return shutil.get_terminal_size(fallback=(120, 24)).columns


def _title_width(title: str) -> int:
    """Compute the display width of a multi-line title.

    Args:
        title: ASCII art or plain-text title.

    Returns:
        Width of the longest non-empty line.
    """
    return max((len(line) for line in title.splitlines() if line.strip()), default=0)


def _select_title() -> str:
    """Select the title variant based on detected terminal width.

    Returns:
        Largest title variant that fits in the current terminal width.
        If none fits, an empty string is returned.
    """
    terminal_columns = _detect_terminal_columns()
    fitting_titles = [
        title for title in TITLE_VARIANTS if _title_width(title) <= terminal_columns
    ]
    if fitting_titles:
        return max(fitting_titles, key=_title_width)
    return ""


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
    label_width = max(
        len(distribution_name)
        for distribution_name, _ in relevant_distribution_versions
    )

    lines = [
        _select_title(),
        f"irsol-data-pipeline v{pipeline_version}",
        "",
        "Runtime",
        f"  {'OS':<8}: {_detect_operating_system()}",
        f"  {'Python':<8}: {platform.python_version()}",
        "",
        "Versions",
    ]
    lines.extend(
        f"  {distribution_name:<{label_width}} : {distribution_version}"
        for distribution_name, distribution_version in relevant_distribution_versions
    )
    return "\n".join(lines)


def print_runtime_presentation() -> None:
    """Print the CLI runtime presentation banner to standard output."""
    print(build_runtime_presentation(), end="\n\n")
