"""Load ``FlatFieldCorrection`` payloads serialized as pickle files."""

from __future__ import annotations

import pickle
from pathlib import Path

from loguru import logger

from irsol_data_pipeline.core.models import FlatFieldCorrection
from irsol_data_pipeline.exceptions import FlatfieldCorrectionImportError


def load_correction_data(path: Path) -> FlatFieldCorrection:
    """Load a pickled correction data file.

    Args:
        path: Path to the pickle file.

    Returns:
        Deserialized FlatFieldCorrection.
    """
    with logger.contextualize(path=path):
        logger.debug("Loading flat-field correction pickle")
        with open(path, "rb") as f:
            try:
                flatfield_correction = pickle.load(f)
            except pickle.UnpicklingError as e:
                raise FlatfieldCorrectionImportError(
                    f"Failed to unpickle FlatFieldCorrection from {path}: {e}"
                )
        if not isinstance(flatfield_correction, FlatFieldCorrection):
            raise FlatfieldCorrectionImportError(
                f"Expected FlatFieldCorrection object in {path}, got {type(flatfield_correction)}"
            )
        logger.debug("Loaded flat-field correction pickle")
        return flatfield_correction
