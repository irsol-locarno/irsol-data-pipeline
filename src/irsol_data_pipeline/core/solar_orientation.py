"""Solar orientation computation from ZIMPOL measurement metadata.

Provides :class:`SolarOrientationInfo` and
:func:`compute_solar_orientation` for determining the direction of
solar north relative to the spectrograph slit.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from astropy.time import Time
from sunpy.coordinates.sun import P

from irsol_data_pipeline.core.models import MeasurementMetadata
from irsol_data_pipeline.core.slit_images.config import DEROTATOR_COORDINATE_SYSTEMS


@dataclass(frozen=True)
class SolarOrientationInfo:
    """Solar orientation information computed from measurement metadata.

    Encapsulates all values needed to render a solar north indicator on
    a Stokes profile plot or other visualisations.

    The solar north direction in the plot frame (wavelength × spatial) is:

    .. code-block:: python

        import numpy as np
        angle_rad = np.radians(info.slit_angle_solar_deg)
        dx = np.cos(angle_rad)  # component along the wavelength axis
        dy = np.sin(angle_rad)  # component along the spatial axis
    """

    sun_p0_deg: float
    """Position angle of the solar north pole (P0) in degrees, as returned
    by :func:`sunpy.coordinates.sun.P`."""

    slit_angle_solar_deg: float
    """Angle of the slit direction in the solar reference frame, in degrees,
    measured counter-clockwise from the solar west direction.

    The solar north direction expressed in the (wavelength, spatial) plot
    frame is :math:`(\\cos\\theta,\\,\\sin\\theta)` where
    :math:`\\theta` = ``slit_angle_solar_deg`` in radians.
    """

    needs_rotation: bool
    """True when the derotator coordinate system is equatorial and a P0
    rotation was applied to bring the slit angle into the solar frame."""


def compute_solar_orientation(
    metadata: MeasurementMetadata,
) -> SolarOrientationInfo:
    """Compute solar orientation from ZIMPOL measurement metadata.

    Derives the angle of solar north in the slit reference frame by:

    1. Querying the sun's P0 angle (position angle of the solar north
       pole) via :func:`sunpy.coordinates.sun.P`.
    2. Reading the derotator position angle from *metadata*.
    3. Applying the P0 rotation when the derotator coordinate system is
       equatorial (``coordinate_system == 0``).

    Args:
        metadata: Parsed ZIMPOL measurement metadata.

    Returns:
        :class:`SolarOrientationInfo` with all orientation values
        populated.
    """
    start_utc = Time(metadata.datetime_start, scale="utc")

    # Sun P0 angle (position angle of the solar north pole).
    # Negated so the rotation matrix rotates from celestial to solar frame.
    sun_p0_deg: float = float(P(start_utc).value)
    sun_p0_rad: float = -sun_p0_deg * np.pi / 180.0

    # Derotator position angle (degrees → radians).
    derotator_angle_deg: float = float(metadata.derotator.position_angle or 0.0)
    derotator_angle_rad: float = derotator_angle_deg * np.pi / 180.0

    # Determine whether a P0 correction is required for this coordinate system.
    coord_system = metadata.derotator.coordinate_system
    needs_rotation: bool = (
        DEROTATOR_COORDINATE_SYSTEMS.get(coord_system, False)
        if coord_system is not None
        else False
    )

    # Slit angle in the solar reference frame.
    if needs_rotation:
        angle2rotate_rad = derotator_angle_rad - sun_p0_rad
    else:
        angle2rotate_rad = derotator_angle_rad

    return SolarOrientationInfo(
        sun_p0_deg=sun_p0_deg,
        slit_angle_solar_deg=float(np.degrees(angle2rotate_rad)),
        needs_rotation=needs_rotation,
    )
