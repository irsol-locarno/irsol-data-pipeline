"""Unit tests for the web_asset_compatibility package."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image

from irsol_data_pipeline.core.web_asset_compatibility.conversion import (
    convert_png_to_jpeg,
)


class TestConvertPngToJpeg:
    def _make_png(self, tmp_path: Path) -> Path:
        png_path = tmp_path / "source.png"
        img = Image.new("RGB", (4, 4), color=(128, 64, 32))
        img.save(png_path, format="PNG")
        return png_path

    def test_creates_jpeg_at_target_path(self, tmp_path: Path) -> None:
        png_path = self._make_png(tmp_path)
        target = tmp_path / "out" / "result.jpg"
        convert_png_to_jpeg(png_path, target, jpeg_quality=80)
        assert target.exists()

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        png_path = self._make_png(tmp_path)
        target = tmp_path / "nested" / "deep" / "result.jpg"
        convert_png_to_jpeg(png_path, target, jpeg_quality=80)
        assert target.parent.is_dir()

    def test_output_is_readable_jpeg(self, tmp_path: Path) -> None:
        png_path = self._make_png(tmp_path)
        target = tmp_path / "result.jpg"
        convert_png_to_jpeg(png_path, target, jpeg_quality=80)
        img = Image.open(target)
        assert img.format == "JPEG"

    def test_rgba_png_converted_without_error(self, tmp_path: Path) -> None:
        """RGBA PNGs must be flattened to RGB before saving as JPEG."""
        rgba_path = tmp_path / "rgba.png"
        img = Image.new("RGBA", (4, 4), color=(128, 64, 32, 200))
        img.save(rgba_path, format="PNG")
        target = tmp_path / "out.jpg"
        convert_png_to_jpeg(rgba_path, target, jpeg_quality=75)
        assert target.exists()

    def test_invalid_quality_raises(self, tmp_path: Path) -> None:
        png_path = self._make_png(tmp_path)
        target = tmp_path / "out.jpg"
        with pytest.raises(ValueError):
            convert_png_to_jpeg(png_path, target, jpeg_quality=0)

    def test_calls_image_save_with_correct_params(self, tmp_path: Path) -> None:
        png_path = self._make_png(tmp_path)
        target = tmp_path / "out.jpg"

        mock_img = MagicMock()
        mock_open = MagicMock(return_value=MagicMock())
        mock_open.return_value.convert.return_value = mock_img

        with patch(
            "irsol_data_pipeline.core.web_asset_compatibility.conversion.Image.open",
            mock_open,
        ):
            convert_png_to_jpeg(png_path, target, jpeg_quality=85)

        mock_img.save.assert_called_once_with(
            target,
            format="JPEG",
            quality=85,
            optimize=True,
            progressive=False,
        )


class TestConvertPngToJpegWithFixtures:
    """Byte-level fidelity tests using pre-generated fixture images.

    The fixture directory contains a reference ``sample.png`` and the
    corresponding ``sample.jpg`` produced by :func:`convert_png_to_jpeg`
    at quality 80.  Re-running the conversion must yield the exact same
    bytes on disk, demonstrating that the function is fully deterministic.
    """

    _JPEG_QUALITY = 80

    def test_png_to_jpeg_bytes_match_fixture(
        self, fixture_dir: Path, tmp_path: Path
    ) -> None:
        """Converting the fixture PNG produces bytes identical to the fixture
        JPEG."""
        png_fixture = fixture_dir / "web_asset_compatibility" / "sample.png"
        jpg_fixture = fixture_dir / "web_asset_compatibility" / "sample.jpg"

        output_jpg = tmp_path / "converted.jpg"
        convert_png_to_jpeg(png_fixture, output_jpg, jpeg_quality=self._JPEG_QUALITY)

        assert output_jpg.read_bytes() == jpg_fixture.read_bytes()
