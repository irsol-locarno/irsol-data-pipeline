import pickle
from pathlib import Path
from typing import Union

from irsol_data_pipeline.core.models import FlatFieldCorrection


def read_flatfield_correction(output_path: Union[Path, str]) -> FlatFieldCorrection:
    """Reads the FlatFieldCorrection from a file using pickle."""
    path = Path(output_path)
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
