"""Discovery of web-asset sources from processed pipeline outputs."""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from irsol_data_pipeline.core.config import (
    PROFILE_CORRECTED_PNG_SUFFIX,
    SLIT_PREVIEW_PNG_SUFFIX,
)
from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.core.web_asset_compatibility.models import (
    WebAssetKind,
    WebAssetSource,
)


def _extract_measurement_name(filename: str, suffix: str) -> str:
    """Extract the measurement base name by stripping a known suffix.

    Args:
        filename: File name to parse.
        suffix: Expected suffix that must be removed.

    Returns:
        Canonical measurement name.

    Raises:
        ValueError: If filename does not end with the expected suffix.
    """

    if not filename.endswith(suffix):
        raise ValueError("filename does not end with expected suffix")
    return filename[: -len(suffix)]


def discover_day_web_asset_sources(
    day: ObservationDay,
    quicklook_root: Path,
    context_root: Path,
) -> list[WebAssetSource]:
    """Discover deployable PNG sources for one observation day.

    Args:
        day: Observation day context.
        quicklook_root: Root output directory for quicklook JPG files.
        context_root: Root output directory for context JPG files.

    Returns:
        Sorted list of deployable sources.
    """

    sources: list[WebAssetSource] = []

    with logger.contextualize(stage="quick-look"):
        if day.processed_dir.is_dir():
            for source_path in sorted(
                day.processed_dir.glob(f"*{PROFILE_CORRECTED_PNG_SUFFIX}")
            ):
                logger.trace("Found quick-look source", source_path=source_path)
                measurement_name = _extract_measurement_name(
                    source_path.name,
                    PROFILE_CORRECTED_PNG_SUFFIX,
                )
                target_path = quicklook_root / day.name / f"{measurement_name}.jpg"
                sources.append(
                    WebAssetSource(
                        kind=WebAssetKind.QUICK_LOOK,
                        observation_name=day.name,
                        measurement_name=measurement_name,
                        source_path=source_path,
                        target_path=target_path,
                    )
                )

    with logger.contextualize(stage="context"):
        if day.processed_dir.is_dir():
            for source_path in sorted(
                day.processed_dir.glob(f"*{SLIT_PREVIEW_PNG_SUFFIX}")
            ):
                logger.trace("Found context source", source_path=source_path)
                measurement_name = _extract_measurement_name(
                    source_path.name,
                    SLIT_PREVIEW_PNG_SUFFIX,
                )
                target_path = context_root / day.name / f"{measurement_name}.jpg"
                sources.append(
                    WebAssetSource(
                        kind=WebAssetKind.CONTEXT,
                        observation_name=day.name,
                        measurement_name=measurement_name,
                        source_path=source_path,
                        target_path=target_path,
                    )
                )

    logger.info("Collected web asset sources", day=day.name, count=len(sources))
    return sorted(
        sources,
        key=lambda source: (
            source.observation_name,
            source.kind.value,
            source.measurement_name,
        ),
    )
