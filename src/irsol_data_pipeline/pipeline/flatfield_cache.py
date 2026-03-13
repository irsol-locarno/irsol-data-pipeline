"""Flat-field correction cache.

Stores computed flat-field corrections grouped by wavelength,
and retrieves the closest correction in time for a given measurement.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
import os

from irsol_data_pipeline.orchestration.decorators import prefect_enabled
from irsol_data_pipeline.core.flatfield import FlatFieldCorrection
from irsol_data_pipeline.correction.analyzer import analyze_flatfield
from irsol_data_pipeline.io.dat_reader import load_flatfield, read_flatfield_si
from concurrent.futures import ProcessPoolExecutor, as_completed, Future
from irsol_data_pipeline.io.flatfield_correction_reader import read_flatfield_correction
from irsol_data_pipeline.io.flatfield_correction_writer import (
    write_flatfield_correction,
)

# Default maximum time delta between measurement and flat-field
DEFAULT_MAX_DELTA = datetime.timedelta(hours=2)


class FlatFieldCache(BaseModel):
    """Cache for computed flat-field corrections.

    Stores corrections grouped by wavelength and provides
    lookup by closest timestamp.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    max_delta: datetime.timedelta = Field(default_factory=lambda: DEFAULT_MAX_DELTA)
    _corrections: dict[int, list[FlatFieldCorrection]] = PrivateAttr(
        default_factory=dict
    )

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
        """Find the closest flat-field correction for a given wavelength and time.

        Args:
            wavelength: Target wavelength in Angstrom.
            timestamp: Measurement timestamp.
            max_delta: Maximum allowed time difference. Uses instance default if None.

        Returns:
            The closest FlatFieldCorrection, or None if none within threshold.
        """
        if max_delta is None:
            max_delta = self.max_delta

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


def _analyze_flatfield(path: Path, reports_dir: Optional[Path]) -> FlatFieldCorrection:
    """Helper function to analyze a single flat-field file for parallel processing."""
    ff = load_flatfield(path)
    ff_si = read_flatfield_si(path)
    dust_flat, offset_map, desmiled = analyze_flatfield(ff_si, reports_path=reports_dir)
    correction = FlatFieldCorrection(
        source_flatfield_path=path,
        dust_flat=dust_flat,
        offset_map=offset_map,
        desmiled=desmiled,
        timestamp=ff.metadata.datetime_start,
        wavelength=ff.metadata.wavelength,
    )
    return correction


def _flatfield_correction_cache_path(
    flatfield_path: Path,
) -> Path:
    """Determine the cache file path for a given flat-field file."""
    cache_filename = f"{flatfield_path.stem}_correction_cache.pkl"
    return flatfield_path.parent.parent / "processed" / "_cache" / cache_filename


def build_flatfield_cache(
    flatfield_paths: list[Path],
    max_delta: datetime.timedelta = DEFAULT_MAX_DELTA,
    reports_dir: Optional[Path] = None,
    allow_cached_data: bool = True,
    max_workers: Optional[int] = None,
) -> FlatFieldCache:
    """Build a FlatFieldCache by analyzing all flat-field files.

    Args:
        flatfield_paths: List of flat-field .dat file paths.
        max_delta: Maximum time delta for matching.
        reports_dir: Optional directory for spectroflat reports.
        allow_cached_data: If True, allows using cached analysis results if available.
        max_workers: Number of parallel workers to use. Defaults to number of CPU cores minus one.

    Returns:
        Populated FlatFieldCache.
    """
    cache = FlatFieldCache(max_delta=max_delta)

    # identify if the flatfields have already been computed and cached, and if so, load them instead of recomputing
    remaining_flatfields = []
    for flatfield_path in flatfield_paths:
        cache_path = _flatfield_correction_cache_path(flatfield_path)
        if allow_cached_data and cache_path.is_file():
            try:
                correction = read_flatfield_correction(cache_path)
                cache.add_correction(correction)
                logger.debug(
                    "Loaded cached flat-field correction", file=flatfield_path.name
                )
                continue
            except Exception:
                logger.warning(
                    "Failed to load cached correction, will re-analyze",
                    file=flatfield_path.name,
                )
        remaining_flatfields.append(flatfield_path)

    if not remaining_flatfields:
        logger.info("All flat-field corrections loaded from cache, no analysis needed")
        return cache

    max_workers = max_workers or max(
        1, min(len(flatfield_paths), (os.cpu_count() or 1) - 1)
    )
    if prefect_enabled() or max_workers == 1:
        # Sequential computation in same process/thread
        logger.debug(
            "Startng sequential flat-field analysis",
            found_in_cache=len(flatfield_paths) - len(remaining_flatfields),
            to_compute=len(remaining_flatfields),
        )
        for ff_path in remaining_flatfields:
            try:
                correction = _analyze_flatfield(ff_path, reports_dir)
                write_flatfield_correction(
                    correction, _flatfield_correction_cache_path(ff_path)
                )
                cache.add_correction(correction)
                logger.success(
                    "Cached flat-field correction",
                    file=ff_path.name,
                    wavelength=correction.wavelength,
                )
            except Exception:
                logger.exception("Failed to analyze flat-field", file=ff_path.name)
    else:
        logger.debug(
            "Starting parallel flat-field analysis",
            found_in_cache=len(flatfield_paths) - len(remaining_flatfields),
            to_compute=len(remaining_flatfields),
            max_workers=max_workers,
        )

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures_to_ff_path: dict[Future, Path] = {
                executor.submit(_analyze_flatfield, ff_path, reports_dir): ff_path
                for ff_path in remaining_flatfields
            }

            for future in as_completed(futures_to_ff_path):
                ff_path = futures_to_ff_path[future]
                try:
                    correction = future.result()
                    write_flatfield_correction(
                        correction, _flatfield_correction_cache_path(ff_path)
                    )

                    cache.add_correction(correction)
                    logger.success(
                        "Cached flat-field correction",
                        file=ff_path.name,
                        wavelength=correction.wavelength,
                    )
                except Exception:
                    logger.exception("Failed to analyze flat-field", file=ff_path.name)

    return cache
