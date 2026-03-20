"""IRSOL Solar Observation Data Processing Pipeline."""

from __future__ import annotations

import os

import matplotlib

if "MPLBACKEND" not in os.environ and not os.environ.get("DISPLAY"):
    matplotlib.use("Agg")

from .version import __version__

__all__ = ["__version__"]
