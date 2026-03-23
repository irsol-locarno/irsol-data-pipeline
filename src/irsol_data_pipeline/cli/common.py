"""Shared helpers for the Cyclopts-based CLI."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from typing import Any

from cyclopts.exceptions import ValidationError
from rich.console import Console

from irsol_data_pipeline import has_display
from irsol_data_pipeline.cli.metadata import OutputFormat
from irsol_data_pipeline.cli.presentation import build_runtime_presentation


def ensure_prefect_enabled() -> None:
    """Enable Prefect-backed behavior for CLI commands.

    The project uses conditional Prefect decorators in several modules.
    Setting this environment variable before importing those modules
    ensures the CLI always exercises the Prefect-enabled path.
    """

    os.environ.setdefault("PREFECT_ENABLED", "true")


def should_print_banner(output_format: OutputFormat, no_banner: bool) -> bool:
    """Determine whether to render the runtime banner.

    Args:
        output_format: Requested command output format.
        no_banner: Explicit user request to suppress the banner.

    Returns:
        True when the banner should be rendered.
    """

    return not no_banner and output_format != "json"


def print_banner(
    *, output_format: OutputFormat = "table", no_banner: bool = False
) -> None:
    """Print the runtime banner when appropriate.

    Args:
        output_format: Requested command output format.
        no_banner: Explicit user request to suppress the banner.
    """

    if should_print_banner(output_format=output_format, no_banner=no_banner):
        print(build_runtime_presentation(), end="\n\n")


def print_json(data: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> None:
    """Print stable JSON output for automation.

    Args:
        data: JSON-serializable mapping or sequence of mappings.
    """

    print(json.dumps(data, indent=2, sort_keys=True))


def get_console() -> Console:
    """Return the shared Rich console instance for operator output.

    Returns:
        Rich console for human-readable reports.
    """

    return Console()


def ensure_display_available() -> None:
    """Validate that a display is available for interactive figure display.

    Raises:
        ValidationError: If DISPLAY environment variable is not set.
    """

    if not has_display():
        raise ValidationError(
            "Cannot display figures: DISPLAY environment variable is not set. "
            "Either set DISPLAY (e.g., for X11: export DISPLAY=:0) or use "
            "--output-path to save the figure to disk instead of --show."
        )
