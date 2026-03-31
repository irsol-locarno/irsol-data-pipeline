"""Unit tests for irsol_data_pipeline.core.slit_images.solar_data."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from irsol_data_pipeline.core.slit_images.solar_data import (
    _download_and_load_map,
    _fetch_fits_file,
)


class TestFetchFitsFile:
    def test_successful_download_writes_file(self, tmp_path: Path) -> None:
        target = tmp_path / "test.fits"
        fake_content = b"FITS" + b"\x00" * 100

        mock_response = MagicMock()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {"content-length": str(len(fake_content))}
        mock_response.iter_content.return_value = [fake_content]

        with patch(
            "irsol_data_pipeline.core.slit_images.solar_data.requests.get",
            return_value=mock_response,
        ):
            bytes_written = _fetch_fits_file.__wrapped__(
                "http://example.com/test.fits", target
            )

        assert bytes_written == len(fake_content)
        assert target.read_bytes() == fake_content

    def test_retries_on_http_error(self, tmp_path: Path) -> None:
        """The function should be retried on failure (tenacity decorator present)."""
        import requests as req_mod
        from tenacity import stop_after_attempt

        target = tmp_path / "test.fits"
        call_count = 0

        def _failing_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise req_mod.ConnectionError("network error")

        with patch(
            "irsol_data_pipeline.core.slit_images.solar_data.requests.get",
            side_effect=_failing_get,
        ):
            # Re-wrap with 2 attempts so the test runs quickly.
            from tenacity import retry, wait_none

            fast_retry = retry(
                stop=stop_after_attempt(2),
                wait=wait_none(),
                reraise=True,
            )
            wrapped = fast_retry(_fetch_fits_file.__wrapped__)
            with pytest.raises(req_mod.ConnectionError):
                wrapped("http://example.com/test.fits", target)

        assert call_count == 2

    def test_raises_after_all_retries_exhausted(self, tmp_path: Path) -> None:
        import requests as req_mod
        from tenacity import retry, stop_after_attempt, wait_none

        target = tmp_path / "test.fits"

        fast_retry = retry(
            stop=stop_after_attempt(3),
            wait=wait_none(),
            reraise=True,
        )
        wrapped = fast_retry(_fetch_fits_file.__wrapped__)

        with patch(
            "irsol_data_pipeline.core.slit_images.solar_data.requests.get",
            side_effect=req_mod.ConnectionError("down"),
        ):
            with pytest.raises(req_mod.ConnectionError):
                wrapped("http://example.com/test.fits", target)


class TestDownloadAndLoadMap:
    _SERIES = "aia.lev1_uv_24s"
    _WAVELENGTH = 1600
    _DATA_TIME = "2024-01-01T12:00:00Z"
    _URL = "http://jsoc.stanford.edu/data/test.fits"
    _METADATA: dict = {}

    def _make_fake_fits(self, tmp_path: Path) -> Path:
        import numpy as np
        from astropy.io import fits

        data = np.zeros((10, 10), dtype=np.float32)
        hdr = fits.Header()
        hdr["NAXIS"] = 2
        hdr["NAXIS1"] = 10
        hdr["NAXIS2"] = 10
        fpath = tmp_path / "fake.fits"
        fits.writeto(str(fpath), data, hdr, overwrite=True)
        return fpath

    def test_temp_file_deleted_after_successful_load(
        self, tmp_path: Path
    ) -> None:
        """Temporary FITS file must be removed after loading into a SunPy map."""
        fits_path = self._make_fake_fits(tmp_path)
        fits_bytes = fits_path.read_bytes()
        created_tmp: list[Path] = []

        original_ntf = __import__("tempfile").NamedTemporaryFile

        def _capturing_ntf(*args, **kwargs):
            ntf = original_ntf(*args, **kwargs)
            created_tmp.append(Path(ntf.name))
            return ntf

        mock_response = MagicMock()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {"content-length": str(len(fits_bytes))}
        mock_response.iter_content.return_value = [fits_bytes]

        with (
            patch(
                "irsol_data_pipeline.core.slit_images.solar_data.requests.get",
                return_value=mock_response,
            ),
            patch(
                "irsol_data_pipeline.core.slit_images.solar_data.tempfile.NamedTemporaryFile",
                side_effect=_capturing_ntf,
            ),
            patch(
                "irsol_data_pipeline.core.slit_images.solar_data.sunpy.map.Map",
                return_value=MagicMock(),
            ),
        ):
            _download_and_load_map(
                self._SERIES,
                self._WAVELENGTH,
                self._DATA_TIME,
                self._URL,
                self._METADATA,
                cache_dir=None,
            )

        assert created_tmp, "Expected a temporary file to be created"
        for tmp in created_tmp:
            assert not tmp.exists(), f"Temp file {tmp} was not deleted"

    def test_temp_file_deleted_when_fits_read_fails(
        self, tmp_path: Path
    ) -> None:
        """Temp file must be removed even if astropy FITS loading fails."""
        fits_bytes = b"NOT_FITS"
        created_tmp: list[Path] = []

        original_ntf = __import__("tempfile").NamedTemporaryFile

        def _capturing_ntf(*args, **kwargs):
            ntf = original_ntf(*args, **kwargs)
            created_tmp.append(Path(ntf.name))
            return ntf

        mock_response = MagicMock()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {"content-length": str(len(fits_bytes))}
        mock_response.iter_content.return_value = [fits_bytes]

        with (
            patch(
                "irsol_data_pipeline.core.slit_images.solar_data.requests.get",
                return_value=mock_response,
            ),
            patch(
                "irsol_data_pipeline.core.slit_images.solar_data.tempfile.NamedTemporaryFile",
                side_effect=_capturing_ntf,
            ),
        ):
            result = _download_and_load_map(
                self._SERIES,
                self._WAVELENGTH,
                self._DATA_TIME,
                self._URL,
                self._METADATA,
                cache_dir=None,
            )

        assert result is None
        assert created_tmp, "Expected a temporary file to be created"
        for tmp in created_tmp:
            assert not tmp.exists(), f"Temp file {tmp} was not deleted"

    def test_temp_file_deleted_when_download_fails(
        self, tmp_path: Path
    ) -> None:
        """Temp file must be removed when the download itself fails."""
        import requests as req_mod

        created_tmp: list[Path] = []
        original_ntf = __import__("tempfile").NamedTemporaryFile

        def _capturing_ntf(*args, **kwargs):
            ntf = original_ntf(*args, **kwargs)
            created_tmp.append(Path(ntf.name))
            return ntf

        # Patch _fetch_fits_file to fail immediately (no real network needed).
        with (
            patch(
                "irsol_data_pipeline.core.slit_images.solar_data.tempfile.NamedTemporaryFile",
                side_effect=_capturing_ntf,
            ),
            patch(
                "irsol_data_pipeline.core.slit_images.solar_data._fetch_fits_file",
                side_effect=req_mod.ConnectionError("down"),
            ),
        ):
            result = _download_and_load_map(
                self._SERIES,
                self._WAVELENGTH,
                self._DATA_TIME,
                self._URL,
                self._METADATA,
                cache_dir=None,
            )

        assert result is None
        assert created_tmp, "Expected a temporary file to be created"
        for tmp in created_tmp:
            assert not tmp.exists(), f"Temp file {tmp} was not deleted"

    def test_cached_file_not_deleted(self, tmp_path: Path) -> None:
        """A pre-existing cache file must not be removed after loading."""
        fits_path = self._make_fake_fits(tmp_path)
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        safe_time = self._DATA_TIME.replace("/", "-").replace(":", "-").replace(
            " ", "_"
        )
        cached = cache_dir / f"{self._SERIES}_{self._WAVELENGTH}_{safe_time}.fits"
        cached.write_bytes(fits_path.read_bytes())

        with patch(
            "irsol_data_pipeline.core.slit_images.solar_data.sunpy.map.Map",
            return_value=MagicMock(),
        ):
            _download_and_load_map(
                self._SERIES,
                self._WAVELENGTH,
                self._DATA_TIME,
                self._URL,
                self._METADATA,
                cache_dir=cache_dir,
            )

        assert cached.exists(), "Cache file must not be deleted"
