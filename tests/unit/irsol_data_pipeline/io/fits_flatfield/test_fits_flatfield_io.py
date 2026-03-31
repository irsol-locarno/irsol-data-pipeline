"""Tests for the FITS-based flat-field correction IO (fits_flatfield)."""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from astropy.io import fits

from irsol_data_pipeline.core.models import FlatFieldCorrection
from irsol_data_pipeline.exceptions import (
    FlatfieldCorrectionExportError,
    FlatfieldCorrectionImportError,
)
from irsol_data_pipeline.io.fits_flatfield.exporter import (
    _OFFSET_MAP_SUFFIX,
    write_correction_data,
)
from irsol_data_pipeline.io.fits_flatfield.importer import load_correction_data


def _make_correction(
    wavelength: int = 6302,
    offset_map: object = None,
    path_name: str = "ff_test.dat",
) -> FlatFieldCorrection:
    return FlatFieldCorrection(
        source_flatfield_path=Path(f"/data/{path_name}"),
        dust_flat=np.ones((50, 200), dtype=np.float64),
        offset_map=offset_map,
        desmiled=np.full((50, 200), 2.0, dtype=np.float64),
        timestamp=datetime.datetime(2024, 7, 13, 10, 0, tzinfo=datetime.timezone.utc),
        wavelength=wavelength,
    )


class TestFitsFlatfieldExporter:
    def test_writes_main_fits_file(self, tmp_path: Path):
        correction = _make_correction()
        out_path = tmp_path / "ff_test_correction_cache.fits"
        result = write_correction_data(out_path, correction)
        assert result == out_path.resolve()
        assert out_path.exists()

    def test_main_fits_has_expected_extensions(self, tmp_path: Path):
        correction = _make_correction()
        out_path = tmp_path / "ff_test_correction_cache.fits"
        write_correction_data(out_path, correction)
        with fits.open(str(out_path)) as hdul:
            ext_names = [hdu.name for hdu in hdul]
        assert "DUSTFLAT" in ext_names
        assert "DESMILED" in ext_names

    def test_main_fits_header_metadata(self, tmp_path: Path):
        correction = _make_correction(wavelength=5886)
        out_path = tmp_path / "ff_test_correction_cache.fits"
        write_correction_data(out_path, correction)
        with fits.open(str(out_path)) as hdul:
            hdr = hdul[0].header
        assert int(hdr["WAVELEN"]) == 5886
        assert str(hdr["SRCFFPTH"]) == str(correction.source_flatfield_path)
        ts_back = datetime.datetime.fromisoformat(str(hdr["TIMESTMP"]))
        assert ts_back == correction.timestamp

    def test_no_offset_map_file_when_offset_map_is_none(self, tmp_path: Path):
        correction = _make_correction(offset_map=None)
        out_path = tmp_path / "ff_test_correction_cache.fits"
        write_correction_data(out_path, correction)
        companion = tmp_path / f"ff_test_correction_cache{_OFFSET_MAP_SUFFIX}"
        assert not companion.exists()
        with fits.open(str(out_path)) as hdul:
            assert "OMAPFILE" not in hdul[0].header

    def test_offset_map_companion_file_created(self, tmp_path: Path):
        mock_offset_map = MagicMock()
        correction = _make_correction(offset_map=mock_offset_map)
        out_path = tmp_path / "ff_test_correction_cache.fits"
        write_correction_data(out_path, correction)
        companion = tmp_path / f"ff_test_correction_cache{_OFFSET_MAP_SUFFIX}"
        mock_offset_map.dump.assert_called_once_with(str(companion))
        with fits.open(str(out_path)) as hdul:
            assert hdul[0].header["OMAPFILE"] == companion.name

    def test_overwrites_existing_file(self, tmp_path: Path):
        correction = _make_correction()
        out_path = tmp_path / "ff_test_correction_cache.fits"
        write_correction_data(out_path, correction)
        correction2 = _make_correction(wavelength=4078)
        write_correction_data(out_path, correction2)
        with fits.open(str(out_path)) as hdul:
            assert int(hdul[0].header["WAVELEN"]) == 4078

    def test_creates_parent_directories(self, tmp_path: Path):
        correction = _make_correction()
        out_path = tmp_path / "deep" / "nested" / "ff_test_correction_cache.fits"
        write_correction_data(out_path, correction)
        assert out_path.exists()

    def test_raises_export_error_on_write_failure(self, tmp_path: Path):
        correction = _make_correction()
        with patch("astropy.io.fits.HDUList.writeto", side_effect=OSError("disk full")):
            with pytest.raises(FlatfieldCorrectionExportError, match="disk full"):
                write_correction_data(tmp_path / "out.fits", correction)


class TestFitsFlatfieldImporter:
    def test_roundtrip_without_offset_map(self, tmp_path: Path):
        original = _make_correction(wavelength=6302, offset_map=None)
        out_path = tmp_path / "ff_test_correction_cache.fits"
        write_correction_data(out_path, original)

        loaded = load_correction_data(out_path)

        assert loaded.wavelength == original.wavelength
        assert loaded.source_flatfield_path == original.source_flatfield_path
        assert loaded.timestamp == original.timestamp
        np.testing.assert_array_almost_equal(loaded.dust_flat, original.dust_flat)
        np.testing.assert_array_almost_equal(loaded.desmiled, original.desmiled)
        assert loaded.offset_map is None

    def test_roundtrip_with_offset_map(self, tmp_path: Path):
        mock_offset_map = MagicMock()
        original = _make_correction(wavelength=5886, offset_map=mock_offset_map)
        out_path = tmp_path / "ff_test_correction_cache.fits"

        write_correction_data(out_path, original)
        companion = tmp_path / f"ff_test_correction_cache{_OFFSET_MAP_SUFFIX}"

        mock_loaded_om = MagicMock()
        with patch(
            "irsol_data_pipeline.io.fits_flatfield.importer.OffsetMap.from_file",
            return_value=mock_loaded_om,
        ) as mock_from_file:
            loaded = load_correction_data(out_path)
            mock_from_file.assert_called_once_with(str(companion))

        assert loaded.offset_map is mock_loaded_om

    def test_raises_import_error_on_missing_file(self, tmp_path: Path):
        with pytest.raises(FlatfieldCorrectionImportError):
            load_correction_data(tmp_path / "nonexistent.fits")

    def test_raises_import_error_when_offset_map_file_missing(self, tmp_path: Path):
        # Write a main FITS file with OMAPFILE header pointing to a non-existent file
        companion_name = f"ff_test_correction_cache{_OFFSET_MAP_SUFFIX}"
        primary_hdr = fits.Header()
        primary_hdr["CONTENT"] = "Flatfield correction"
        primary_hdr["SRCFFPTH"] = "/data/ff_test.dat"
        primary_hdr["WAVELEN"] = 6302
        primary_hdr["TIMESTMP"] = datetime.datetime(
            2024, 7, 13, 10, 0, tzinfo=datetime.timezone.utc
        ).isoformat()
        primary_hdr["OMAPFILE"] = companion_name
        hdul = fits.HDUList(
            [
                fits.PrimaryHDU(header=primary_hdr),
                fits.ImageHDU(data=np.ones((50, 200)), name="DUSTFLAT"),
                fits.ImageHDU(data=np.ones((50, 200)), name="DESMILED"),
            ]
        )
        out_path = tmp_path / "ff_test_correction_cache.fits"
        hdul.writeto(str(out_path))

        # Companion file is intentionally absent
        with pytest.raises(FlatfieldCorrectionImportError, match="OffsetMap"):
            load_correction_data(out_path)

    def test_raises_import_error_on_corrupt_fits(self, tmp_path: Path):
        corrupt = tmp_path / "corrupt.fits"
        corrupt.write_bytes(b"not a fits file")
        with pytest.raises(FlatfieldCorrectionImportError):
            load_correction_data(corrupt)

    def test_string_path_accepted(self, tmp_path: Path):
        original = _make_correction()
        out_path = tmp_path / "ff_test_correction_cache.fits"
        write_correction_data(str(out_path), original)
        loaded = load_correction_data(str(out_path))
        assert loaded.wavelength == original.wavelength
