"""Persist ``FlatFieldCorrection`` payloads as pickle files."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Union

from irsol_data_pipeline.core.models import FlatFieldCorrection


def write_correction_data(
    output_path: Union[Path, str],
    data: FlatFieldCorrection,
) -> Path:
    """Write a ``FlatFieldCorrection`` object to disk as pickle.

    Args:
        output_path: Where to write the pickle file.
        data: FlatFieldCorrection to persist.

    Returns:
        The path written to.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    return path
