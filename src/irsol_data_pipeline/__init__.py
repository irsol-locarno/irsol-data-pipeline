"""IRSOL Solar Observation Data Processing Pipeline."""

from __future__ import annotations

import os
import warnings

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

# Suppress warnings from sunpy, which can be verbose and not relevant to users of this package.
# The reason we get this type of warnings, is that sunpy is installed without some "optional" packages such as `mlp-animators` etc.
# For the purpose of this library, these extra features are not needed
from sunpy.util import SunpyUserWarning as _SunpyUserWarning  # noqa

warnings.filterwarnings("ignore", category=_SunpyUserWarning, module="sunpy")

__all__ = ["__version__", "has_display"]
