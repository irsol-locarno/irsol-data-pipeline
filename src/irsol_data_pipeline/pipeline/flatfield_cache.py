"""Flat-field correction cache.

Stores computed flat-field corrections grouped by wavelength, and
retrieves the closest correction in time for a given measurement.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional

from loguru import logger

from irsol_data_pipeline.core.config import DEFAULT_MAX_DELTA
from irsol_data_pipeline.core.correction.analyzer import analyze_flatfield
from irsol_data_pipeline.core.models import FlatFieldCorrection, MeasurementMetadata
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.io import fits_flatfield as flatfield_io
from irsol_data_pipeline.pipeline.filesystem import flatfield_correction_cache_path
from irsol_data_pipeline.prefect.decorators import task
from irsol_data_pipeline.prefect.utils import create_prefect_json_report


class FlatFieldCache:
    """Cache for computed flat-field corrections.

    Stores corrections grouped by wavelength and provides lookup by
    closest timestamp.
    """

    def __init__(self, max_delta: datetime.timedelta = DEFAULT_MAX_DELTA):
        self.max_delta = max_delta
        self._corrections: dict[int, list[FlatFieldCorrection]] = {}

    def add_correction(self, correction: FlatFieldCorrection) -> None:
        """Add a computed correction to the cache.

        Args:
            correction: The flat-field correction to cache.
        """
        wl = correction.wavelength
        if wl not in self._corrections:
            self._corrections[wl] = []
        self._corrections[wl].append(correction)

    def find_best_correction(
        self,
        wavelength: int,
        timestamp: datetime.datetime,
        max_delta: Optional[datetime.timedelta] = None,
    ) -> Optional[FlatFieldCorrection]:
        """Find the closest flat-field correction for a given wavelength and
        time.

        Args:
            wavelength: Target wavelength in Angstrom.
            timestamp: Measurement timestamp.
            max_delta: Maximum allowed time difference. Uses instance default if None.

        Returns:
            The closest FlatFieldCorrection, or None if none within threshold.
        """
        max_delta = max_delta or self.max_delta

        candidates = self._corrections.get(wavelength, [])
        if not candidates:
            logger.warning(
                "No flat-field corrections available for wavelength",
                wavelength=wavelength,
            )
            return None

        best: Optional[FlatFieldCorrection] = None
        best_delta: Optional[datetime.timedelta] = None

        for correction in candidates:
            delta = abs(correction.timestamp - timestamp)
            if delta > max_delta:
                continue
            if best_delta is None or delta < best_delta:
                best = correction
                best_delta = delta

        return best

    @property
    def wavelengths(self) -> list[int]:
        """List of wavelengths with cached corrections."""
        return sorted(self._corrections.keys())

    def __len__(self) -> int:
        return sum(len(v) for v in self._corrections.values())


@task(task_run_name="ff-correction/analyze-flatfield/{path.name}")
def _analyze_flatfield(path: Path) -> FlatFieldCorrection:
    """Helper function to analyze a single flat-field file for parallel
    processing."""

    stokes, info = dat_io.read(path)
    metadata = MeasurementMetadata.from_info_array(info)

    with NamedTemporaryFile(suffix=".json") as f:
        with open(f.name, "w") as json_file:
            json.dump(metadata.model_dump(), json_file, default=str)
        create_prefect_json_report(
            path=Path(f.name), title="Flatfield metadata", key=f"ff-{path.name}"
        )

    dust_flat, offset_map, desmiled = analyze_flatfield(stokes.i)
    correction = FlatFieldCorrection(
        source_flatfield_path=path,
        dust_flat=dust_flat,
        offset_map=offset_map,
        desmiled=desmiled,
        timestamp=metadata.datetime_start,
        wavelength=metadata.wavelength,
    )
    return correction


@task(task_run_name="ff-correction/build-cache")
def build_flatfield_cache(
    flatfield_paths: list[Path],
    max_delta: datetime.timedelta = DEFAULT_MAX_DELTA,
    allow_cached_data: bool = True,
    cache_dir: Optional[Path] = None,
) -> FlatFieldCache:
    """Build a FlatFieldCache by analyzing all flat-field files.

    Args:
        flatfield_paths: List of flat-field .dat file paths.
        max_delta: Maximum time delta for matching.
        allow_cached_data: If True, allows using cached analysis results if available.
        cache_dir: Optional directory override for storing correction cache FITS
            files. When provided, cached files are placed directly under this
            directory instead of the default day-structure-derived path. Useful
            when processing individual measurements outside the standard dataset
            hierarchy.

    Returns:
        Populated FlatFieldCache.
    """
    cache = FlatFieldCache(max_delta=max_delta)

    def _resolve_cache_path(flatfield_path: Path) -> Path:
        """Resolve the cache file path for a flat-field file.

        Args:
            flatfield_path: Path to the flat-field ``.dat`` source file.

        Returns:
            Destination path for the correction cache FITS file.
        """
        if cache_dir is not None:
            return cache_dir / f"{flatfield_path.stem}_correction_cache.fits"
        return flatfield_correction_cache_path(flatfield_path)

    # identify if the flatfields have already been computed and cached, and if so, load them instead of recomputing
    remaining_flatfields = []
    for flatfield_path in flatfield_paths:
        with logger.contextualize(file=flatfield_path.name):
            cache_path = _resolve_cache_path(flatfield_path)
            if allow_cached_data and cache_path.is_file():
                try:
                    correction = flatfield_io.read(cache_path)
                    cache.add_correction(correction)
                    logger.debug("Loaded cached flat-field correction")
                    continue
                except FileNotFoundError:
                    logger.debug(
                        "Flat field correction cache does not exist, will analyze and cache results",
                    )
                except Exception as err:
                    logger.warning(
                        "Failed to load cached correction, will re-analyze",
                        error=str(err),
                    )
            remaining_flatfields.append(flatfield_path)

    if not remaining_flatfields:
        logger.info("All flat-field corrections loaded from cache, no analysis needed")
        return cache

    logger.debug(
        "Starting flat-field analysis",
        found_in_cache=len(flatfield_paths) - len(remaining_flatfields),
        to_compute=len(remaining_flatfields),
    )
    for ff_path in remaining_flatfields:
        with logger.contextualize(file=ff_path.name):
            try:
                correction = _analyze_flatfield(ff_path)
                dest = _resolve_cache_path(ff_path)
                dest.parent.mkdir(parents=True, exist_ok=True)
                flatfield_io.write(dest, correction)
            except Exception:
                logger.exception("Failed to analyze flat-field")
            else:
                cache.add_correction(correction)
                logger.success(
                    "Cached flat-field correction",
                    wavelength=correction.wavelength,
                )

    return cache
