"""Discovery of web-asset sources from processed pipeline outputs."""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from irsol_data_pipeline.core.config import (
    PROFILE_CORRECTED_PNG_SUFFIX,
    PROFILE_ORIGINAL_PNG_SUFFIX,
    SLIT_PREVIEW_PNG_SUFFIX,
)
from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.core.web_asset_compatibility.models import (
    WebAssetKind,
    WebAssetSource,
)

# Maps each asset kind to an ordered list of PNG suffixes to check; the first
# existing file wins.  The priority list for QUICK_LOOK prefers the corrected
# profile (successful run) and falls back to the original (uncorrected) profile
# so that measurements which failed flat-field correction still get a
# quick-look asset.
_KIND_SUFFIX_PRIORITY: list[tuple[WebAssetKind, list[str]]] = [
    (
        WebAssetKind.QUICK_LOOK,
        [PROFILE_CORRECTED_PNG_SUFFIX, PROFILE_ORIGINAL_PNG_SUFFIX],
    ),
    (WebAssetKind.CONTEXT, [SLIT_PREVIEW_PNG_SUFFIX]),
]


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


def discover_measurement_names(processed_dir: Path) -> list[str]:
    """Discover unique measurement names from a processed output directory.

    Scans for PNG files matching any known output suffix and extracts the
    measurement base names. Files that do not match any known suffix are
    ignored.

    Args:
        processed_dir: Directory containing processed pipeline outputs.

    Returns:
        Sorted list of unique measurement names found in the directory.
        Returns an empty list when the directory does not exist.
    """
    if not processed_dir.is_dir():
        return []

    names: set[str] = set()
    for _kind, suffixes in _KIND_SUFFIX_PRIORITY:
        for suffix in suffixes:
            names.update(
                _extract_measurement_name(path.name, suffix)
                for path in processed_dir.glob(f"*{suffix}")
            )

    return sorted(names)


def discover_assets_for_measurement(
    measurement_name: str,
    observation_name: str,
    processed_dir: Path,
) -> list[WebAssetSource]:
    """Discover web assets available for a single measurement.

    Checks for the existence of each known PNG output file for the given
    measurement name and returns a :class:`~irsol_data_pipeline.core.web_asset_compatibility.models.WebAssetSource`
    for each file that is present.

    For asset kinds with multiple candidate suffixes (e.g. ``QUICK_LOOK``),
    the first existing file in priority order is used and the remaining
    candidates are skipped, ensuring exactly one source per kind.

    Args:
        measurement_name: Canonical measurement name (e.g. ``"5876_m01"``).
        observation_name: Observation day folder name (YYMMDD).
        processed_dir: Directory containing processed pipeline outputs.

    Returns:
        List of web assets found for the measurement (may be empty).
    """
    sources: list[WebAssetSource] = []
    for kind, suffixes in _KIND_SUFFIX_PRIORITY:
        for suffix in suffixes:
            source_path = processed_dir / f"{measurement_name}{suffix}"
            if source_path.exists():
                logger.trace(
                    "Found web asset source",
                    kind=kind.value,
                    source_path=source_path,
                )
                sources.append(
                    WebAssetSource(
                        kind=kind,
                        observation_name=observation_name,
                        measurement_name=measurement_name,
                        source_path=source_path,
                    ),
                )
                break  # first match wins for this kind
    return sources


def discover_day_web_asset_sources(day: ObservationDay) -> list[WebAssetSource]:
    """Discover deployable PNG sources for one observation day.

    Iterates over all unique measurement names found in the processed
    directory and, for each measurement, identifies the available web assets
    (quicklook profile plot and context slit preview).

    Args:
        day: Observation day context.

    Returns:
        Sorted list of deployable sources, ordered by observation name, asset
        kind, and measurement name.
    """
    with logger.contextualize(day=day.name):
        sources: list[WebAssetSource] = []

        measurement_names = discover_measurement_names(day.processed_dir)
        if not measurement_names:
            logger.info("No measurements found")
        for measurement_name in measurement_names:
            with logger.contextualize(measurement=measurement_name):
                assets = discover_assets_for_measurement(
                    measurement_name=measurement_name,
                    observation_name=day.name,
                    processed_dir=day.processed_dir,
                )
                sources.extend(assets)

        logger.info("Collected web asset sources", day=day.name, count=len(sources))
        return sorted(
            sources,
            key=lambda source: (
                source.observation_name,
                source.kind.value,
                source.measurement_name,
            ),
        )
