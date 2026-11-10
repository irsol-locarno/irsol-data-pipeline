"""Reader for ZIMPOL IDL save files stored as ``.dat``/``.sav``."""

from __future__ import annotations

from pathlib import Path

import numpy as np
from loguru import logger
from scipy.io import readsav

from irsol_data_pipeline.core.models import (
    MeasurementMetadata,
    StokesParameters,
)
from irsol_data_pipeline.exceptions import DatImportError


def read_zimpol_dat(
    file_path: Path | str,
) -> tuple[StokesParameters, MeasurementMetadata]:
    """Read a ZIMPOL ``.dat``/``.sav`` file and return Stokes +
    MeasurementMetadata.

    Args:
        file_path: Path to the .dat or .sav file.

    Returns:
        Tuple of (StokesParameters, MeasurementMetadata).
    """
    path = Path(file_path).resolve()
    with logger.contextualize(path=path):
        logger.debug("Loading ZIMPOL file")
        if path.suffix.lower() in [".dat", ".sav"]:
            data = readsav(str(path), verbose=False, python_dict=True)
        else:
            raise DatImportError(
                f"Unsupported file format: '{path.suffix}', expected '.dat' or '.sav'.",
            )

        si = np.array(data["si"])
        sq = np.array(data["sq"])
        su = np.array(data["su"])
        sv = np.array(data["sv"])
        info = np.array(data["info"])

        # If data is 3D (no TCU averaging), average to 2D.
        # All four channels must be handled: reductions that skip TCU
        # averaging store every Stokes parameter as (frames, y, x), and
        # downstream smile correction requires 2D arrays for Q and U too
        # (previously only I and V were averaged, crashing on Q/U).
        averaged = {}
        for name, channel in (("si", si), ("sq", sq), ("su", su), ("sv", sv)):
            result = channel
            if result.ndim == 3:  # noqa PLR2004 - magic numbers are ok in this case
                logger.debug(
                    f"Averaging 3D Stokes {name.upper()} over axis 0",
                    shape=result.shape,
                )
                result = np.mean(result, axis=0)
            averaged[name] = result
        si, sq, su, sv = averaged["si"], averaged["sq"], averaged["su"], averaged["sv"]

        logger.trace("Loading info file into MeasurementMetadata")
        metadata = MeasurementMetadata.from_info_array(info)

        logger.debug(
            "Loaded ZIMPOL arrays",
            shape_i=si.shape,
            shape_q=sq.shape,
            shape_u=su.shape,
            shape_v=sv.shape,
            info_shape=info.shape,
        )

    return StokesParameters(i=si, q=sq, u=su, v=sv), metadata
