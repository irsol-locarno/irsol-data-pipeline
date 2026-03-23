"""PNG to JPEG conversion utilities for web-asset deployment."""

from __future__ import annotations

from pathlib import Path

from loguru import logger
from PIL import Image


def _normalize_jpeg_quality(jpeg_quality: int) -> int:
    """Validate JPEG quality for conversion.

    Args:
        jpeg_quality: Requested JPEG quality.

    Returns:
        A valid JPEG quality value.

    Raises:
        ValueError: If the quality is outside Pillow's practical range.
    """

    if 1 <= jpeg_quality <= 95:
        return jpeg_quality
    raise ValueError("jpeg_quality must be in [1, 95]")


def convert_png_to_jpeg(
    source_path: Path,
    target_path: Path,
    jpeg_quality: int,
) -> None:
    """Convert one PNG image to a JPEG file with explicit quality settings.

    Args:
        source_path: Source PNG path.
        target_path: Destination JPG path.
        jpeg_quality: JPEG quality in [1, 95].
    """
    logger.debug(
        "Converting PNG to JPEG",
        source_path=source_path,
        target_path=target_path,
        jpeg_quality=jpeg_quality,
    )
    quality = _normalize_jpeg_quality(jpeg_quality)
    image_data = Image.open(source_path).convert("RGB")

    target_path.parent.mkdir(parents=True, exist_ok=True)

    image_data.save(
        target_path,
        format="JPEG",
        quality=quality,
        optimize=True,
        progressive=False,
    )
