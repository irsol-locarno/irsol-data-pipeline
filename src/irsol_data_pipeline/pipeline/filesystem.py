"""Filesystem discovery utilities.

Functions for scanning the dataset hierarchy and discovering observation
folders, measurement files, and flat-field files.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from loguru import logger

from irsol_data_pipeline.core.config import (
    CACHE_DIRNAME,
    CORRECTED_FITS_SUFFIX,
    ERROR_JSON_SUFFIX,
    FLATFIELD_CORRECTION_DATA_SUFFIX,
    METADATA_JSON_SUFFIX,
    PROCESSED_DIRNAME,
    PROFILE_CORRECTED_PNG_SUFFIX,
    PROFILE_ORIGINAL_PNG_SUFFIX,
    RAW_DIRNAME,
    REDUCED_DIRNAME,
    SLIT_PREVIEW_ERROR_JSON_SUFFIX,
    SLIT_PREVIEW_PNG_SUFFIX,
)
from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.prefect.decorators import task

# Pattern: <wavelength>_m<id>.dat  (e.g. 6302_m1.dat)
OBSERVATION_PATTERN = re.compile(r"^(\d+)_m(\d+)\.dat$")

# Pattern: ff<wavelength>_m<id>.dat  (e.g. ff6302_m1.dat)
FLATFIELD_PATTERN = re.compile(r"^ff(\d+)_m(\d+)\.dat$")

# Patterns to ignore: cal*, dark*
IGNORED_PREFIXES = ("cal", "dark")


ProcessedOutputKind = Literal[
    "corrected_fits",
    "error_json",
    "metadata_json",
    "flatfield_correction_data",
    "profile_corrected_png",
    "profile_original_png",
    "slit_preview_png",
    "slit_preview_error_json",
]

ObservationDayPredicate = Callable[[ObservationDay], bool]

_PROCESSED_SUFFIX_BY_KIND: dict[ProcessedOutputKind, str] = {
    "corrected_fits": CORRECTED_FITS_SUFFIX,
    "error_json": ERROR_JSON_SUFFIX,
    "metadata_json": METADATA_JSON_SUFFIX,
    "flatfield_correction_data": FLATFIELD_CORRECTION_DATA_SUFFIX,
    "profile_corrected_png": PROFILE_CORRECTED_PNG_SUFFIX,
    "profile_original_png": PROFILE_ORIGINAL_PNG_SUFFIX,
    "slit_preview_png": SLIT_PREVIEW_PNG_SUFFIX,
    "slit_preview_error_json": SLIT_PREVIEW_ERROR_JSON_SUFFIX,
}


def raw_dir_for_day(day_path: Path) -> Path:
    """Return the canonical raw directory for an observation day path."""
    return day_path / RAW_DIRNAME


def reduced_dir_for_day(day_path: Path) -> Path:
    """Return the canonical reduced directory for an observation day path."""
    return day_path / REDUCED_DIRNAME


def processed_dir_for_day(day_path: Path) -> Path:
    """Return the canonical processed directory for an observation day path."""
    return day_path / PROCESSED_DIRNAME


def processed_cache_dir_for_day(day_path: Path) -> Path:
    """Return the canonical processed cache directory for an observation day
    path."""
    return processed_dir_for_day(day_path) / CACHE_DIRNAME


def processed_dir_for_measurement(measurement_path: Path) -> Path:
    """Return the default processed directory for a measurement path."""
    return measurement_path.parent.parent / PROCESSED_DIRNAME


def processed_output_path(
    processed_dir: Path,
    source_name: str,
    kind: ProcessedOutputKind,
) -> Path:
    """Build a canonical processed output path for a source measurement
    name."""
    stem = get_processed_stem(source_name)
    return processed_dir / f"{stem}{_PROCESSED_SUFFIX_BY_KIND[kind]}"


def flatfield_correction_cache_path(flatfield_path: Path) -> Path:
    """Return the flat-field correction cache path for a flat-field .dat
    path."""
    cache_filename = f"{flatfield_path.stem}_correction_cache.fits"
    day_path = flatfield_path.parent.parent
    return processed_cache_dir_for_day(day_path) / "flat-field-cache" / cache_filename


def sdo_cache_dir_path(day_path: Path) -> Path:
    """Return the SDO FITS cache directory for an observation day path."""
    return processed_cache_dir_for_day(day_path) / "sdo"


def delete_empty_dirs(path: Path):
    """Recursively deletes empty directories, including the one pointed to by
    the input argument, if empty."""
    if not path.is_dir():
        return
    for child in path.iterdir():
        if child.is_dir():
            delete_empty_dirs(child)
    # After deleting empty subdirs, check if current dir is empty
    if not any(path.iterdir()):
        logger.debug("Deleting empty directory", path=path)
        path.rmdir()


@task(task_run_name="dataset/discover-days/{root.name}")
def discover_observation_days(
    root: Path,
    predicate: ObservationDayPredicate | None = None,
) -> list[ObservationDay]:
    """Scan the dataset root and discover all observation day folders.

    Expects a hierarchy of ``<root>/<year>/<day>/`` where each day
    has a ``reduced/`` subfolder.

    Args:
        root: The dataset root directory.
        predicate: Optional filter returning ``True`` for days that should be
            included.

    Returns:
        Sorted list of ObservationDay objects.
    """
    with logger.contextualize(root=root):
        logger.info("Scanning observation days")
        days: list[ObservationDay] = []

        if not root.is_dir():
            logger.debug("Observation root does not exist")
            return days

        for year_dir in sorted(root.iterdir()):
            with logger.contextualize(year=year_dir):
                if not year_dir.is_dir():
                    logger.warning("Skipping non-directory in root")
                    continue
                logger.info("Scanning year")
                for day_dir in sorted(year_dir.iterdir()):
                    with logger.contextualize(day=day_dir.name):
                        if not day_dir.is_dir():
                            logger.warning("Skipping non-directory")
                            continue
                        reduced = day_dir / REDUCED_DIRNAME
                        if not reduced.is_dir():
                            logger.warning(
                                "Skipping day without reduced directory",
                            )
                            continue
                        days.append(
                            ObservationDay(
                                path=day_dir,
                                raw_dir=day_dir / RAW_DIRNAME,
                                reduced_dir=reduced,
                                processed_dir=day_dir / PROCESSED_DIRNAME,
                            )
                        )

        if predicate is not None:
            days = [day for day in days if predicate(day)]

        days_sorted = sorted(days, key=lambda d: d.path)
        logger.info("Discovered observation days", count=len(days_sorted))
        return days_sorted


def discover_measurement_files(reduced_dir: Path) -> list[Path]:
    """Find all observation .dat files in a reduced directory.

    Excludes flat-field, calibration, and dark files.

    Args:
        reduced_dir: Path to the reduced/ folder of an observation day.

    Returns:
        Sorted list of measurement file paths.
    """
    with logger.contextualize(reduced_dir=reduced_dir):
        logger.debug("Scanning measurement files")
        if not reduced_dir.is_dir():
            logger.debug("Reduced directory does not exist")
            return []

        files: list[Path] = []
        for p in sorted(reduced_dir.iterdir()):
            if not p.is_file() or not p.name.endswith(".dat"):
                continue
            # Skip flat-field files
            if FLATFIELD_PATTERN.match(p.name):
                continue
            # Skip calibration and dark files
            if any(p.name.lower().startswith(prefix) for prefix in IGNORED_PREFIXES):
                continue
            # Must match observation pattern
            if OBSERVATION_PATTERN.match(p.name):
                files.append(p)

        files_sorted = sorted(files)
        logger.debug(
            "Discovered measurement files",
            count=len(files_sorted),
        )
        return files_sorted


def discover_flatfield_files(reduced_dir: Path) -> list[Path]:
    """Find all flat-field .dat files in a reduced directory.

    Args:
        reduced_dir: Path to the reduced/ folder.

    Returns:
        Sorted list of flat-field file paths.
    """
    with logger.contextualize(reduced_dir=reduced_dir):
        logger.debug("Scanning flat-field files")
        if not reduced_dir.is_dir():
            logger.debug("Reduced directory does not exist")
            return []

        files: list[Path] = []
        for p in sorted(reduced_dir.iterdir()):
            if p.is_file() and FLATFIELD_PATTERN.match(p.name):
                files.append(p)

        files_sorted = sorted(files)
        logger.debug(
            "Discovered flat-field files",
            count=len(files_sorted),
        )
        return files_sorted


def get_processed_stem(source_name: str) -> str:
    """Derive a processed file stem from a source file name.

    Example: ``6302_m1.dat`` -> ``6302_m1``

    Args:
        source_name: The source .dat filename.

    Returns:
        Base stem for processed output files.
    """
    return Path(source_name).stem


def is_measurement_flat_field_processed(processed_dir: Path, source_name: str) -> bool:
    """Check whether a measurement has already been processed.

    A measurement is considered processed if either a canonical
    ``*_corrected.fits`` file or an ``*_error.json`` file exists in
    the processed directory.

    Args:
        processed_dir: Path to the processed/ folder.
        source_name: Source .dat filename.

    Returns:
        True if already processed.
    """
    with logger.contextualize(processed_dir=processed_dir, source_name=source_name):
        corrected_fits = processed_output_path(
            processed_dir,
            source_name,
            kind="corrected_fits",
        )
        error = processed_output_path(
            processed_dir,
            source_name,
            kind="error_json",
        )
        is_processed = corrected_fits.exists() or error.exists()
        logger.debug(
            "Checked processed state",
            has_corrected_fits=corrected_fits.exists(),
            has_error_json=error.exists(),
            is_processed=is_processed,
        )
        return is_processed


def is_measurement_slit_preview_generated(
    processed_dir: Path, source_name: str
) -> bool:
    """Check whether a slit preview has already been generated.

    A slit preview is considered generated if either a
    ``*_slit_preview.png`` or ``*_slit_preview_error.json`` file exists.

    Args:
        processed_dir: Path to the processed/ folder.
        source_name: Source .dat filename.

    Returns:
        True if slit preview already exists.
    """
    preview = processed_output_path(processed_dir, source_name, kind="slit_preview_png")
    error = processed_output_path(
        processed_dir, source_name, kind="slit_preview_error_json"
    )
    return preview.exists() or error.exists()
