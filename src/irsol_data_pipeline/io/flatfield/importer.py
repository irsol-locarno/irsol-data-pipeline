"""Load ``FlatFieldCorrection`` payloads serialized as pickle files."""

from __future__ import annotations

import pickle
from pathlib import Path

from irsol_data_pipeline.core.models import FlatFieldCorrection


def load_correction_data(path: Path) -> FlatFieldCorrection:
    """Load a pickled correction data file.

    Args:
        path: Path to the pickle file.

    Returns:
        Deserialized FlatFieldCorrection.
    """
    with open(path, "rb") as f:
        try:
            flatfield_correction = pickle.load(f)
        except pickle.UnpicklingError as e:
            raise ValueError(f"Failed to unpickle FlatFieldCorrection from {path}: {e}")
    if not isinstance(flatfield_correction, FlatFieldCorrection):
        raise ValueError(
            f"Expected FlatFieldCorrection object in {path}, got {type(flatfield_correction)}"
        )
    return flatfield_correction
