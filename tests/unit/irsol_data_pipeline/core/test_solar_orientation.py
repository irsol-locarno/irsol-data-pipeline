"""Tests for solar orientation computation."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from pydantic import ValidationError

from irsol_data_pipeline.core.models import MeasurementMetadata, SolarOrientationInfo
from irsol_data_pipeline.core.solar_orientation import (
    compute_solar_orientation,
)
from tests.unit.utils import make_dat_array_info

_BASE_MEASUREMENT_INFO: dict[str, str] = {
    "measurement.file": "/data/5886_m1.z3bd",
    "measurement.telescope name": "Gregory IRSOL",
    "measurement.instrument": "ZIMPOL3",
    "measurement.name": "5886_m1",
    "measurement.datetime": "2025-06-15T10:00:00+00:00",
    "measurement.type": "SCIENCE",
    "measurement.id": "1",
    "measurement.wavelength": "5886",
}


def _make_metadata(overrides: dict[str, str] | None = None) -> MeasurementMetadata:
    """Build a minimal MeasurementMetadata from a base dict with optional
    overrides."""
    entries = dict(_BASE_MEASUREMENT_INFO)
    if overrides:
        entries.update(overrides)
    return MeasurementMetadata.from_info_array(make_dat_array_info(entries))


class TestSolarOrientationInfo:
    def test_is_frozen(self):
        info = SolarOrientationInfo(
            sun_p0_deg=5.0,
            slit_angle_solar_deg=45.0,
            needs_rotation=True,
        )
        with pytest.raises((AttributeError, TypeError, ValidationError)):
            info.sun_p0_deg = 10.0  # type: ignore[misc]

    def test_fields_stored(self):
        info = SolarOrientationInfo(
            sun_p0_deg=12.3,
            slit_angle_solar_deg=30.0,
            needs_rotation=False,
        )
        assert info.sun_p0_deg == pytest.approx(12.3)
        assert info.slit_angle_solar_deg == pytest.approx(30.0)
        assert info.needs_rotation is False


class TestComputeSolarOrientation:
    def test_no_derotator_info(self):
        """With no derotator metadata, angle defaults to zero in solar
        frame."""
        metadata = _make_metadata()
        with patch("irsol_data_pipeline.core.solar_orientation.P") as mock_p:
            mock_p.return_value.value = 10.0
            info = compute_solar_orientation(metadata)

        assert isinstance(info, SolarOrientationInfo)
        assert info.sun_p0_deg == pytest.approx(10.0)
        # No coordinate system set → needs_rotation defaults to False
        assert info.needs_rotation is False
        # No derotator angle → 0 degrees; no rotation applied
        assert info.slit_angle_solar_deg == pytest.approx(0.0)

    def test_equatorial_coordinate_system_applies_p0_rotation(self):
        """Equatorial coordinate system (0) should subtract P0 from the
        angle."""
        metadata = _make_metadata(
            {
                "measurement.derotator.coordinate_system": "0",
                "measurement.derotator.position_angle": "30.0",
            }
        )
        p0 = 15.0  # degrees
        with patch("irsol_data_pipeline.core.solar_orientation.P") as mock_p:
            mock_p.return_value.value = p0
            info = compute_solar_orientation(metadata)

        assert info.needs_rotation is True
        assert info.slit_angle_solar_deg == pytest.approx(45.0)

    def test_heliographic_coordinate_system_no_p0_rotation(self):
        """Heliographic coordinate system (1) should NOT apply P0 rotation."""
        metadata = _make_metadata(
            {
                "measurement.derotator.coordinate_system": "1",
                "measurement.derotator.position_angle": "30.0",
            }
        )
        with patch("irsol_data_pipeline.core.solar_orientation.P") as mock_p:
            mock_p.return_value.value = 20.0
            info = compute_solar_orientation(metadata)

        assert info.needs_rotation is False
        assert info.slit_angle_solar_deg == pytest.approx(30.0)

    def test_sun_p0_is_populated(self):
        """sun_p0_deg should reflect the value returned by sunpy P()."""
        metadata = _make_metadata()
        with patch("irsol_data_pipeline.core.solar_orientation.P") as mock_p:
            mock_p.return_value.value = 7.5
            info = compute_solar_orientation(metadata)

        assert info.sun_p0_deg == pytest.approx(7.5)

    @pytest.mark.parametrize(
        "coord_system,position_angle,p0,expected_angle",
        [
            # No coordinate system: use derotator angle directly
            (None, 0.0, 10.0, 0.0),
            # Heliographic (1): pass derotator angle through unchanged
            ("1", 45.0, 20.0, 45.0),
            # Equatorial (0): subtract sun_p0_rad from derotator_rad
            # angle2rotate_rad = (60 * pi/180) - (-5 * pi/180) = 65 * pi/180 = 65 deg
            ("0", 60.0, 5.0, 65.0),
        ],
    )
    def test_parametrized_angle_computation(
        self,
        coord_system: str | None,
        position_angle: float,
        p0: float,
        expected_angle: float,
    ):
        overrides: dict[str, str] = {
            "measurement.derotator.position_angle": str(position_angle),
        }
        if coord_system is not None:
            overrides["measurement.derotator.coordinate_system"] = coord_system

        metadata = _make_metadata(overrides)
        with patch("irsol_data_pipeline.core.solar_orientation.P") as mock_p:
            mock_p.return_value.value = p0
            info = compute_solar_orientation(metadata)

        assert info.slit_angle_solar_deg == pytest.approx(expected_angle, abs=1e-9)
