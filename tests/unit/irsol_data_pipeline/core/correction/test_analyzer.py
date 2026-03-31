from pathlib import Path

import numpy as np
import pytest

from irsol_data_pipeline.core.correction.analyzer import create_config_for_data
from irsol_data_pipeline.exceptions import InvalidMeasurementDataException
from irsol_data_pipeline.io import dat as dat_io


class TestCorrectionConfig:
    @pytest.fixture(scope="module")
    def stokes_i(self, fixture_dir: Path) -> np.ndarray:
        return dat_io.read(fixture_dir / "5886_m14.dat")[0].i

    @pytest.mark.parametrize("expand_dims", [True, False])
    def test_create_config_for_data(self, stokes_i: np.ndarray, expand_dims: bool):
        if expand_dims:
            stokes_i = np.expand_dims(stokes_i, axis=0)

        config = create_config_for_data(stokes_i)
        assert config.sensor_flat.spacial_degree == 13
        assert config.sensor_flat.sigma_mask == 2
        assert config.sensor_flat.fit_border == 1
        assert config.sensor_flat.average_column_response_map is True
        assert config.sensor_flat.ignore_gradient is False

        assert config.smile.line_distance == 16
        assert config.smile.strong_smile_deg == 2
        assert config.smile.max_dispersion_deg == 5
        assert config.smile.line_prominence == 0.1
        assert config.smile.height_sigma == 0.04
        assert config.smile.smooth is True
        assert config.smile.emission_spectrum is False
        assert config.smile.state_aware is False
        assert config.smile.align_states is True
        assert config.smile.smile_deg == 3
        assert config.smile.rotation_correction == 0
        assert config.smile.detrend is True

    @pytest.mark.parametrize("shape", [(100,), (1, 100, 100, 100)])
    def test_create_config_for_data_fails_on_invalid_shape(
        self,
        shape: tuple[int, ...],
    ):
        stokes_i = np.empty(shape=shape)
        with pytest.raises(InvalidMeasurementDataException):
            create_config_for_data(stokes_i)
