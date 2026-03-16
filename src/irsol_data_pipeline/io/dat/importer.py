"""Reader for ZIMPOL IDL save files stored as ``.dat``/``.sav``."""

from __future__ import annotations

from pathlib import Path
from typing import Union

import numpy as np
from scipy.io import readsav

from irsol_data_pipeline.core.models import (
    StokesParameters,
)


def read_zimpol_dat(
    file_path: Union[Path, str],
) -> tuple[StokesParameters, np.ndarray]:
    """Read a ZIMPOL ``.dat``/``.sav`` file and return Stokes + raw ``info``.

    Args:
        file_path: Path to the .dat or .sav file.

    Returns:
        Tuple of (StokesParameters, info_array).
    """
    path = Path(file_path).resolve()
    if path.suffix.lower() in [".dat", ".sav"]:
        data = readsav(str(path), verbose=False, python_dict=True)
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}")

    si = np.array(data["si"])
    sq = np.array(data["sq"])
    su = np.array(data["su"])
    sv = np.array(data["sv"])
    info = np.array(data["info"])

    # If data is 3D (no TCU averaging), average to 2D
    if si.ndim == 3:
        si = np.mean(si, axis=0)
    if sv.ndim == 3:
        sv = np.mean(sv, axis=0)

    return StokesParameters(i=si, q=sq, u=su, v=sv), info
