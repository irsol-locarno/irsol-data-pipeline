"""Coordinate transformations for slit image generation.

Pure math functions: mu calculation, Earth-to-Solar frame rotation,
slit endpoint computation. No I/O or plotting.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from astropy.time import Time
from loguru import logger
from sunpy.coordinates.sun import P, angular_radius

from irsol_data_pipeline.core.models import MeasurementMetadata
from irsol_data_pipeline.core.slit_images.config import (
    DEFAULT_TELESCOPE_SPEC,
    DEROTATOR_COORDINATE_SYSTEMS,
    TELESCOPE_SPECS,
)
from irsol_data_pipeline.core.slit_images.z3readbd import read_z3bd_header


@dataclass(frozen=True)
class SlitGeometry:
    """Computed slit geometry in the solar reference frame."""

    # Slit center in solar coordinates (arcsec)
    center_solar_x: float
    center_solar_y: float

    # Slit endpoints in solar coordinates (arcsec)
    slit_x_start: float
    slit_x_end: float
    slit_y_start: float
    slit_y_end: float

    # Observation angle for the slit in solar frame (radians)
    angle_solar: float

    # Derotator offset angle (radians), None if not available
    derotator_offset: float | None

    # Mu value (limb darkening)
    mu: float

    # Observation times
    start_time: datetime.datetime
    end_time: datetime.datetime

    # Display-friendly time strings
    start_time_str: str
    end_time_str: str

    # Telescope name
    telescope: str

    # Measurement name (e.g. "6302_m1")
    measurement_name: str

    # Observation name (e.g. "20250312")
    observation_name: str


def compute_mu(obstime: datetime.datetime, slit_center: tuple[float, float]) -> float:
    """Compute the mu (cos theta) value for a slit position on the solar disc.

    Args:
        obstime: Observation time (UTC, timezone-naive or aware).
        slit_center: (x, y) position in arcsec from disc center.

    Returns:
        Mu value. Positive if on-disc, negative if off-limb.
    """
    r0 = angular_radius(obstime).value
    distance_from_center = np.sqrt(slit_center[0] ** 2 + slit_center[1] ** 2)
    limb_distance = r0 - distance_from_center

    if limb_distance < 0.0:
        return -np.sqrt(1.0 - (1.0 - abs(limb_distance) / r0) ** 2)
    return np.sqrt(1.0 - (1.0 - abs(limb_distance) / r0) ** 2)


def compute_slit_geometry(
    metadata: MeasurementMetadata,
    use_limbguider: bool = False,
    offset_corrections: tuple[float, float] = (0.0, 0.0),
    angle_correction: float = 0.0,
) -> SlitGeometry:
    """Compute slit geometry in the solar reference frame from measurement
    metadata.

    Args:
        metadata: Parsed measurement metadata from a .dat file.
        use_limbguider: If True, attempt to use limbguider coordinates
            from the raw z3bd file.
        offset_corrections: (x, y) corrections in arcsec to apply to the
            slit center.
        angle_correction: Correction in degrees to apply to the derotator
            angle.

    Returns:
        SlitGeometry with all computed values.

    Raises:
        ValueError: If required metadata fields are missing.
    """
    # Get slit center coordinates
    image_center = _get_image_center(metadata, use_limbguider)
    if image_center is None:
        raise ValueError(
            f"No solar disc coordinates available for measurement {metadata.name}",
        )

    image_center = (
        image_center[0] + offset_corrections[0],
        image_center[1] + offset_corrections[1],
    )

    # Derotator angle
    derotator_angle_deg = metadata.derotator.position_angle
    if derotator_angle_deg is None:
        derotator_angle_deg = 0.0
    derotator_angle = (derotator_angle_deg + angle_correction) * np.pi / 180

    # Derotator offset
    derotator_offset: float | None = None
    if metadata.derotator.offset is not None:
        derotator_offset = float(metadata.derotator.offset) * np.pi / 180

    # Coordinate system
    coord_system = metadata.derotator.coordinate_system
    needs_rotation = (
        DEROTATOR_COORDINATE_SYSTEMS.get(coord_system, False)
        if coord_system is not None
        else False
    )

    # Observation times
    start_time = metadata.datetime_start.replace(tzinfo=None)
    end_time = (
        metadata.datetime_end.replace(tzinfo=None)
        if metadata.datetime_end
        else start_time
    )

    start_utc = Time(metadata.datetime_start, scale="utc")

    # Compute mu
    mu = compute_mu(start_time, image_center)

    # Sun P0 angle (position angle of solar north pole)
    sun_p0_deg = P(start_utc).value
    sun_p0_rad = -sun_p0_deg * np.pi / 180

    # Rotate slit center from Earth to Solar reference frame
    rot_matrix = np.array(
        [
            [np.cos(sun_p0_rad), -np.sin(sun_p0_rad)],
            [np.sin(sun_p0_rad), np.cos(sun_p0_rad)],
        ],
    )
    center_solar = rot_matrix.dot(image_center)
    center_solar_x, center_solar_y = float(center_solar[0]), float(center_solar[1])

    # Adjust derotator angle based on coordinate system
    angle2rotate = (derotator_angle - sun_p0_rad) if needs_rotation else derotator_angle

    # Get telescope specs
    telescope = metadata.telescope_name.lower()
    specs = TELESCOPE_SPECS.get(telescope, DEFAULT_TELESCOPE_SPEC)
    slit_radius = specs["slit_radius"]

    # Compute slit endpoints
    slit_x_start = center_solar_x - slit_radius * np.cos(angle2rotate)
    slit_x_end = center_solar_x + slit_radius * np.cos(angle2rotate)
    slit_y_start = center_solar_y - slit_radius * np.sin(angle2rotate)
    slit_y_end = center_solar_y + slit_radius * np.sin(angle2rotate)

    # Derive observation name from the day directory (e.g. "20250312")
    observation_name = ""
    raw_file = metadata.file or ""
    if raw_file:
        # Try to extract day name from the raw file path
        parts = Path(raw_file).parts
        for part in parts:
            if len(part) == 6 and part.isdigit():  # noqa: PLR2004 - magic values are ok in this case
                observation_name = part
                break

    return SlitGeometry(
        center_solar_x=center_solar_x,
        center_solar_y=center_solar_y,
        slit_x_start=float(slit_x_start),
        slit_x_end=float(slit_x_end),
        slit_y_start=float(slit_y_start),
        slit_y_end=float(slit_y_end),
        angle_solar=float(angle2rotate),
        derotator_offset=derotator_offset,
        mu=float(mu),
        start_time=start_time,
        end_time=end_time,
        start_time_str=metadata.datetime_start.strftime("%Y-%m-%d %H:%M:%S"),
        end_time_str=(
            metadata.datetime_end.strftime("%Y-%m-%d %H:%M:%S")
            if metadata.datetime_end
            else ""
        ),
        telescope=telescope,
        measurement_name=metadata.name,
        observation_name=observation_name,
    )


def _get_image_center(
    metadata: MeasurementMetadata,
    use_limbguider: bool,
) -> tuple[float, float] | None:
    """Get slit center coordinates, optionally from limbguider data.

    Args:
        metadata: Measurement metadata.
        use_limbguider: If True, attempt to read limbguider coordinates
            from the raw z3bd file referenced in the metadata.

    Returns:
        (x, y) in arcsec, or None if coordinates are unavailable.
    """
    sdc_center = None
    if metadata.solar_x is not None and metadata.solar_y is not None:
        sdc_center = (metadata.solar_x, metadata.solar_y)

    if not use_limbguider:
        return sdc_center

    # Try to read limbguider data from raw z3bd file
    if metadata.limbguider_status == 0 or metadata.limbguider_status is None:
        return sdc_center

    raw_file_path = metadata.file or metadata.get_raw("measurement.file")
    if not raw_file_path:
        logger.debug("No raw file path available for limbguider lookup")
        return sdc_center

    raw_path = Path(str(raw_file_path))
    if not raw_path.is_file():
        logger.debug("Raw z3bd file not found", path=raw_path)
        return sdc_center

    header = read_z3bd_header(raw_path)
    if header is None:
        logger.debug("Could not read z3bd header", path=raw_path)
        return sdc_center

    lg_cx = header.get("LG_CXM")
    lg_cy = header.get("LG_CYM")

    if lg_cx is not None and lg_cy is not None:
        cx, cy = float(lg_cx), float(lg_cy)
        if cx != 0.0 or cy != 0.0:
            logger.info("Using limbguider coordinates", cx=cx, cy=cy)
            return (cx, cy)
        logger.debug("Limbguider coordinates are zero, falling back to SDC")

    return sdc_center
