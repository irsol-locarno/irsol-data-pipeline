"""Flat-field analysis using the spectroflat library."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
from loguru import logger
from qollib.strings import parse_shape
from spectroflat import Analyser, Config, SensorFlatConfig, SmileConfig
from spectroflat.smile import OffsetMap


def create_config_for_data(flat_field: np.ndarray) -> Config:
    """Create a spectroflat Config appropriate for the given flat-field data.

    Args:
        flat_field: Flat-field Stokes I array (2D or 3D).

    Returns:
        spectroflat Config object.
    """
    if flat_field.ndim == 2:
        ff_shape = flat_field.shape
    elif flat_field.ndim == 3:
        ff_shape = flat_field.shape[1:]
    else:
        raise ValueError(f"Flat field must be 2D or 3D, got shape {flat_field.shape}")

    roi = parse_shape(f"[1:{ff_shape[0] - 2},1:{ff_shape[1] - 2}]")

    config = Config(roi=roi, iterations=2)

    config.sensor_flat = SensorFlatConfig(
        spacial_degree=13,
        sigma_mask=2,
        fit_border=1,
        average_column_response_map=True,
        ignore_gradient=False,
        roi=roi,
    )

    config.smile = SmileConfig(
        line_distance=16,
        strong_smile_deg=2,
        max_dispersion_deg=5,
        line_prominence=0.1,
        height_sigma=0.04,
        smooth=True,
        emission_spectrum=False,
        state_aware=False,
        align_states=True,
        smile_deg=3,
        rotation_correction=0,
        detrend=True,
        roi=roi,
    )
    return config


def analyze_flatfield(
    flat_field_si: np.ndarray,
    reports_path: Optional[Path] = None,
    config: Optional[Config] = None,
) -> tuple[np.ndarray, OffsetMap, np.ndarray]:
    """Run spectroflat analysis on a flat-field Stokes I array.

    Args:
        flat_field_si: Raw Stokes I array from the flat-field file.
        reports_path: Directory for analysis reports. Uses a temp dir if None.
        config: spectroflat Config. Auto-created if None.

    Returns:
        Tuple of (dust_flat, offset_map, desmiled).
    """
    if config is None:
        config = create_config_for_data(flat_field_si)

    # Ensure data is 3D for spectroflat
    if flat_field_si.ndim == 2:
        ff_data = np.expand_dims(flat_field_si, axis=0)
    else:
        ff_data = flat_field_si

    if reports_path is not None:
        reports_path.mkdir(parents=True, exist_ok=True)
        rpath = str(reports_path)
    else:
        rpath = None

    logger.info("Starting flat-field analysis")
    analyser = Analyser(ff_data, config, rpath)  # type: ignore
    analyser.run()

    return analyser.dust_flat, analyser.offset_map, analyser.desmiled
