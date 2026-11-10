from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

from irsol_data_pipeline.core.models import MeasurementMetadata, StokesParameters
from irsol_data_pipeline.exceptions import DatImportError
from irsol_data_pipeline.io import dat as dat_io


class TestImporter:
    @pytest.fixture(scope="module")
    def dat_path(self, fixture_dir: Path) -> Path:
        return fixture_dir / "5886_m14.dat"

    @pytest.fixture(scope="module")
    def invalid_path_extension(self, fixture_dir: Path) -> Path:
        return fixture_dir / "5886_m14.txt"

    @pytest.fixture(scope="module")
    def expected_stokes_shape(self) -> tuple[int, ...]:
        return (140, 1240)

    @pytest.fixture(scope="module")
    def expected_info_shape(self) -> tuple[int, ...]:
        return (65, 2)

    def test_read_valid_dat(
        self,
        dat_path: Path,
        expected_stokes_shape: tuple[int, ...],
        expected_info_shape: tuple[int, ...],
    ):
        stokes, metadata = dat_io.read(dat_path)
        assert isinstance(stokes, StokesParameters)
        assert isinstance(metadata, MeasurementMetadata)

        assert stokes.i.shape == expected_stokes_shape
        assert stokes.q.shape == expected_stokes_shape
        assert stokes.u.shape == expected_stokes_shape
        assert stokes.v.shape == expected_stokes_shape

    def test_read_invalid_dat(self, invalid_path_extension: Path):
        with pytest.raises(DatImportError):
            dat_io.read(invalid_path_extension)

    @pytest.mark.parametrize("shape", [(140, 200), (200, 800)])
    def test_read_with_ndims_3(self, shape: tuple[int, int]):
        """All four Stokes channels are averaged when stored 3D (no TCU
        averaging in the reduction)."""
        si = np.random.rand(3, *shape)
        sq = np.random.rand(3, *shape)
        su = np.random.rand(3, *shape)
        sv = np.random.rand(3, *shape)
        info_array = np.array([])
        metadata = object()

        expected_si = np.mean(si, axis=0)
        expected_sq = np.mean(sq, axis=0)
        expected_su = np.mean(su, axis=0)
        expected_sv = np.mean(sv, axis=0)

        mock_data = {"si": si, "sq": sq, "su": su, "sv": sv, "info": info_array}
        with (
            patch("irsol_data_pipeline.io.dat.importer.readsav") as mock_readsav,
            patch(
                "irsol_data_pipeline.core.models.MeasurementMetadata.from_info_array",
                return_value=metadata,
            ),
        ):
            # Mock data with 3D arrays
            mock_readsav.return_value = mock_data
            path = "/abs-path-to-dummy_path.dat"
            ret_stokes, ret_metadata = dat_io.read(path)

            # Check that the 3D arrays were averaged to 2D
            np.testing.assert_array_equal(ret_stokes.i, expected_si)
            np.testing.assert_array_equal(ret_stokes.q, expected_sq)
            np.testing.assert_array_equal(ret_stokes.u, expected_su)
            np.testing.assert_array_equal(ret_stokes.v, expected_sv)

            # Check that the returned stokes are all 2D
            for channel in (ret_stokes.i, ret_stokes.q, ret_stokes.u, ret_stokes.v):
                assert channel.ndim == 2

            # Check that the metadata returned is the one build from the info-array
            assert ret_metadata is metadata

            mock_readsav.assert_called_once_with(path, verbose=False, python_dict=True)
