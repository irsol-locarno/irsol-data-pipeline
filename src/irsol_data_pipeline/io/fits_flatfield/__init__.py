"""FITS-based serialization for ``FlatFieldCorrection`` objects."""

from __future__ import annotations

from .exporter import write_correction_data as write
from .importer import load_correction_data as read

__all__ = ["read", "write"]
