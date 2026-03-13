"""Flat-field correction application using the spectroflat library."""

from __future__ import annotations

import numpy as np
from spectroflat.smile.interpolated_correction import SmileInterpolator
from spectroflat.smile import OffsetMap

from irsol_data_pipeline.core.types import StokesParameters


def apply_correction(
    stokes: StokesParameters,
    dust_flat: np.ndarray,
    offset_map: OffsetMap,
) -> StokesParameters:
    """Apply flat-field and smile correction to Stokes parameters.

    1. Divides Stokes I by the dust flat map (intensity correction).
    2. Applies smile correction to all four Stokes parameters.

    Args:
        stokes: Original Stokes parameters.
        dust_flat: Dust flat correction map from analysis.
        offset_map: Offset map for smile correction from analysis.

    Returns:
        Corrected StokesParameters.
    """
    # Handle extra dimension in dust_flat
    if dust_flat.ndim == 3:
        dust_flat = np.squeeze(dust_flat, axis=0)

    si_corrected = _desmile(stokes.i / dust_flat, offset_map)
    sq_corrected = _desmile(stokes.q, offset_map)
    su_corrected = _desmile(stokes.u, offset_map)
    sv_corrected = _desmile(stokes.v, offset_map)

    return StokesParameters(
        i=si_corrected,
        q=sq_corrected,
        u=su_corrected,
        v=sv_corrected,
    )


def _desmile(data: np.ndarray, offset_map: OffsetMap) -> np.ndarray:
    """Apply smile correction to a single 2D array."""
    interpolator = SmileInterpolator(offset_map, data, mod_state=0)
    desmiled = interpolator.run()
    result = desmiled.result
    if result is None:
        raise RuntimeError("Smile correction produced no result")
    return result
