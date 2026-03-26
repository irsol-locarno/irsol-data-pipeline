"""Tests for solar orientation computation."""

from __future__ import annotations

import datetime
from unittest.mock import patch

import numpy as np
import pytest

from irsol_data_pipeline.core.models import MeasurementMetadata
from irsol_data_pipeline.core.solar_orientation import (
    SolarOrientationInfo,
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
    """Build a minimal MeasurementMetadata from a base dict with optional overrides."""
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
        with pytest.raises((AttributeError, TypeError)):
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
        """With no derotator metadata, angle defaults to zero in solar frame."""
        metadata = _make_metadata()
        with patch(
            "irsol_data_pipeline.core.solar_orientation.P"
        ) as mock_p:
            mock_p.return_value.value = 10.0
            info = compute_solar_orientation(metadata)

        assert isinstance(info, SolarOrientationInfo)
        assert info.sun_p0_deg == pytest.approx(10.0)
        # No coordinate system set → needs_rotation defaults to False
        assert info.needs_rotation is False
        # No derotator angle → 0 degrees; no rotation applied
        assert info.slit_angle_solar_deg == pytest.approx(0.0)

    def test_equatorial_coordinate_system_applies_p0_rotation(self):
        """Equatorial coordinate system (0) should subtract P0 from the angle."""
        metadata = _make_metadata(
            {
                "measurement.derotator.coordinate_system": "0",
                "measurement.derotator.position_angle": "30.0",
            }
        )
        p0 = 15.0  # degrees
        with patch(
            "irsol_data_pipeline.core.solar_orientation.P"
        ) as mock_p:
            mock_p.return_value.value = p0
            info = compute_solar_orientation(metadata)

        assert info.needs_rotation is True
        expected_angle = 30.0 - (-p0)  # derotator_deg - (-p0_deg * pi/180 converted back)
        # angle2rotate_rad = derotator_rad - sun_p0_rad
        # sun_p0_rad = -p0 * pi/180
        # angle2rotate_rad = (30 * pi/180) - (-15 * pi/180) = 45 * pi/180
        assert info.slit_angle_solar_deg == pytest.approx(45.0)

    def test_heliographic_coordinate_system_no_p0_rotation(self):
        """Heliographic coordinate system (1) should NOT apply P0 rotation."""
        metadata = _make_metadata(
            {
                "measurement.derotator.coordinate_system": "1",
                "measurement.derotator.position_angle": "30.0",
            }
        )
        with patch(
            "irsol_data_pipeline.core.solar_orientation.P"
        ) as mock_p:
            mock_p.return_value.value = 20.0
            info = compute_solar_orientation(metadata)

        assert info.needs_rotation is False
        assert info.slit_angle_solar_deg == pytest.approx(30.0)

    def test_sun_p0_is_populated(self):
        """sun_p0_deg should reflect the value returned by sunpy P()."""
        metadata = _make_metadata()
        with patch(
            "irsol_data_pipeline.core.solar_orientation.P"
        ) as mock_p:
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
        with patch(
            "irsol_data_pipeline.core.solar_orientation.P"
        ) as mock_p:
            mock_p.return_value.value = p0
            info = compute_solar_orientation(metadata)

        assert info.slit_angle_solar_deg == pytest.approx(expected_angle, abs=1e-9)


class TestDrawSolarNorthArrow:
    """Tests that _draw_solar_north_arrow can be called without error."""

    def test_draws_without_error(self):
        import matplotlib
        matplotlib.use("Agg")
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_agg import FigureCanvasAgg

        from irsol_data_pipeline.plotting.profile import _draw_solar_north_arrow

        fig = Figure()
        FigureCanvasAgg(fig)
        ax = fig.add_subplot(1, 1, 1)

        info = SolarOrientationInfo(
            sun_p0_deg=10.0,
            slit_angle_solar_deg=45.0,
            needs_rotation=False,
        )
        _draw_solar_north_arrow(ax, info)

    @pytest.mark.parametrize("angle_deg", [0.0, 45.0, 90.0, 135.0, 180.0, -45.0])
    def test_draws_various_angles(self, angle_deg: float):
        import matplotlib
        matplotlib.use("Agg")
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_agg import FigureCanvasAgg

        from irsol_data_pipeline.plotting.profile import _draw_solar_north_arrow

        fig = Figure()
        FigureCanvasAgg(fig)
        ax = fig.add_subplot(1, 1, 1)

        info = SolarOrientationInfo(
            sun_p0_deg=0.0,
            slit_angle_solar_deg=angle_deg,
            needs_rotation=False,
        )
        _draw_solar_north_arrow(ax, info)


class TestPlotWithMetadata:
    """Tests that plot() calls compute_solar_orientation when metadata is given."""

    def test_plot_calls_solar_north_when_metadata_provided(self, tmp_path):
        import matplotlib
        matplotlib.use("Agg")
        import numpy as np
        from irsol_data_pipeline.core.models import StokesParameters
        from irsol_data_pipeline.plotting.profile import plot

        stokes = StokesParameters(
            i=np.ones((10, 20)),
            q=np.zeros((10, 20)),
            u=np.zeros((10, 20)),
            v=np.zeros((10, 20)),
        )
        metadata = _make_metadata(
            {
                "measurement.derotator.coordinate_system": "1",
                "measurement.derotator.position_angle": "45.0",
            }
        )

        output = tmp_path / "test.png"
        with patch(
            "irsol_data_pipeline.plotting.profile.compute_solar_orientation"
        ) as mock_compute:
            mock_compute.return_value = SolarOrientationInfo(
                sun_p0_deg=5.0,
                slit_angle_solar_deg=45.0,
                needs_rotation=False,
            )
            plot(stokes, filename_save=output, metadata=metadata)

        mock_compute.assert_called_once_with(metadata)
        assert output.exists()

    def test_plot_no_arrow_without_metadata(self, tmp_path):
        import matplotlib
        matplotlib.use("Agg")
        import numpy as np
        from irsol_data_pipeline.core.models import StokesParameters
        from irsol_data_pipeline.plotting.profile import plot

        stokes = StokesParameters(
            i=np.ones((10, 20)),
            q=np.zeros((10, 20)),
            u=np.zeros((10, 20)),
            v=np.zeros((10, 20)),
        )

        output = tmp_path / "test_no_meta.png"
        with patch(
            "irsol_data_pipeline.plotting.profile.compute_solar_orientation"
        ) as mock_compute:
            plot(stokes, filename_save=output)

        mock_compute.assert_not_called()
        assert output.exists()
