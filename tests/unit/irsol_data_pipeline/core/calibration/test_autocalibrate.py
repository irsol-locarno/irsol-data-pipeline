from pathlib import Path

import pytest

from irsol_data_pipeline.core.calibration.autocalibrate import calibrate_measurement
from irsol_data_pipeline.core.models import StokesParameters
from irsol_data_pipeline.io import dat as dat_io


class TestAutocalibrate:
    @pytest.fixture
    def stokes(self, fixture_dir: Path) -> StokesParameters:
        return dat_io.read(fixture_dir / "5886_m14.dat")[0]

    def test_calibrate_measurement(self, stokes: StokesParameters):
        result = calibrate_measurement(stokes)
        assert pytest.approx(result.pixel_scale, rel=1e-5) == 0.023448466826962687
        assert pytest.approx(result.wavelength_offset, rel=1e-5) == 5872.047381861239
        assert (
            pytest.approx(result.pixel_scale_error, rel=1e-5) == 0.00017740790053598606
        )
        assert (
            pytest.approx(result.wavelength_offset_error, rel=1e-5)
            == 0.08647920052112987
        )
        assert result.reference_file == "ref_data5886_irsol.npy"
