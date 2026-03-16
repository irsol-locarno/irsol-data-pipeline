"""Reader for ZIMPOL IDL save files stored as ``.dat``/``.sav``."""

from __future__ import annotations

from pathlib import Path
from typing import Union

import numpy as np
from loguru import logger
from scipy.io import readsav

from irsol_data_pipeline.core.models import (
    StokesParameters,
)
from irsol_data_pipeline.exceptions import DatImportError


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
    with logger.contextualize(path=path):
        logger.debug("Loading ZIMPOL file")
        if path.suffix.lower() in [".dat", ".sav"]:
            data = readsav(str(path), verbose=False, python_dict=True)
        else:
            raise DatImportError(f"Unsupported file format: {path.suffix}")

        si = np.array(data["si"])
        sq = np.array(data["sq"])
        su = np.array(data["su"])
        sv = np.array(data["sv"])
        info = np.array(data["info"])

        # If data is 3D (no TCU averaging), average to 2D
        if si.ndim == 3:
            logger.debug("Averaging 3D Stokes I over axis 0", shape=si.shape)
            si = np.mean(si, axis=0)
        if sv.ndim == 3:
            logger.debug("Averaging 3D Stokes V over axis 0", shape=sv.shape)
            sv = np.mean(sv, axis=0)

        logger.debug(
            "Loaded ZIMPOL arrays",
            shape_i=si.shape,
            shape_q=sq.shape,
            shape_u=su.shape,
            shape_v=sv.shape,
            info_shape=info.shape,
        )

    return StokesParameters(i=si, q=sq, u=su, v=sv), info
