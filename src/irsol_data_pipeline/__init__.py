"""IRSOL Solar Observation Data Processing Pipeline."""

from __future__ import annotations

import os

from .version import __version__


def has_display() -> bool:
    """Check if a display is available for interactive visualization.

    Returns:
        True if DISPLAY environment variable is set and non-empty.
    """

    return bool(os.environ.get("DISPLAY"))


if "MPLBACKEND" not in os.environ and not has_display():
    import matplotlib

    matplotlib.use("Agg")


__all__ = ["__version__", "has_display"]
