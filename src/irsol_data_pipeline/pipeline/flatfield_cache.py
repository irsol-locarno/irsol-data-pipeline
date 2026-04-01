"""Flat-field correction cache.

Stores computed flat-field corrections grouped by wavelength, and
retrieves the closest correction in time for a given measurement.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from tempfile import NamedTemporaryFile

from loguru import logger

from irsol_data_pipeline.core.config import DEFAULT_MAX_ANGLE_DELTA, DEFAULT_MAX_DELTA
from irsol_data_pipeline.core.correction.analyzer import analyze_flatfield
from irsol_data_pipeline.core.models import FlatFieldCorrection
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.io import fits_flatfield as flatfield_io
from irsol_data_pipeline.pipeline.filesystem import flatfield_correction_cache_path
from irsol_data_pipeline.prefect.decorators import task
from irsol_data_pipeline.prefect.utils import create_prefect_json_report

_HALF_CIRCLE_DEGREES = 180


class FlatFieldCache:
    """Cache for computed flat-field corrections.

    Stores corrections grouped by wavelength and provides lookup by
    closest timestamp.
    """

    def __init__(
        self,
        max_delta: datetime.timedelta = DEFAULT_MAX_DELTA,
        max_angle_delta: float = DEFAULT_MAX_ANGLE_DELTA,
    ):
        self.max_delta = max_delta
        self.max_angle_delta = max_angle_delta
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
        max_delta: datetime.timedelta | None = None,
        position_angle: float | None = None,
        max_angle_delta: float | None = None,
    ) -> FlatFieldCorrection | None:
        """Find the closest flat-field correction for a given wavelength, time,
        and optional derotator position angle.

        A candidate correction is eligible only when:

        1. Its wavelength matches exactly.
        2. The absolute time difference with *timestamp* does not exceed
           *max_delta*.
        3. If both *position_angle* and the candidate's
           ``correction.position_angle`` are known, the circular angular
           difference between them does not exceed *max_angle_delta*.
           When either angle is ``None`` the angle check is skipped and the
           candidate is not filtered out.

        Among all eligible candidates the one closest in time is returned.

        Args:
            wavelength: Target wavelength in Angstrom.
            timestamp: Measurement timestamp.
            max_delta: Maximum allowed time difference. Uses instance default
                if ``None``.
            position_angle: Derotator position angle of the measurement in
                degrees, or ``None`` if unknown.
            max_angle_delta: Maximum allowed angular difference in degrees.
                Uses instance default if ``None``.

        Returns:
            The closest eligible FlatFieldCorrection, or ``None`` if no
            candidate satisfies all constraints.
        """
        max_delta = max_delta or self.max_delta
        effective_max_angle_delta = (
            max_angle_delta if max_angle_delta is not None else self.max_angle_delta
        )

        candidates = self._corrections.get(wavelength, [])
        if not candidates:
            logger.warning(
                "No flat-field corrections available for wavelength",
                wavelength=wavelength,
            )
            return None

        best: FlatFieldCorrection | None = None
        best_delta: datetime.timedelta | None = None

        for correction in candidates:
            delta = abs(correction.timestamp - timestamp)
            if delta > max_delta:
                continue

            if position_angle is not None and correction.position_angle is not None:
                raw_diff = abs(correction.position_angle - position_angle) % 360
                angle_diff = (
                    raw_diff if raw_diff <= _HALF_CIRCLE_DEGREES else 360 - raw_diff
                )
                if angle_diff > effective_max_angle_delta:
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
    stokes, metadata = dat_io.read(path)

    with NamedTemporaryFile(suffix=".json") as f:
        with Path(f.name).open("w") as json_file:
            json.dump(metadata.model_dump(), json_file, default=str)
        create_prefect_json_report(
            path=Path(f.name),
            title="Flatfield metadata",
            key=f"ff-{path.name}",
        )

    dust_flat, offset_map, desmiled = analyze_flatfield(stokes.i)
    correction = FlatFieldCorrection(
        source_flatfield_path=path,
        dust_flat=dust_flat,
        offset_map=offset_map,
        desmiled=desmiled,
        timestamp=metadata.datetime_start,
        wavelength=metadata.wavelength,
        position_angle=metadata.derotator.position_angle,
    )
    return correction


@task(task_run_name="ff-correction/build-cache")
def build_flatfield_cache(
    flatfield_paths: list[Path],
    max_delta: datetime.timedelta = DEFAULT_MAX_DELTA,
    max_angle_delta: float = DEFAULT_MAX_ANGLE_DELTA,
    allow_cached_data: bool = True,
    cache_dir: Path | None = None,
) -> FlatFieldCache:
    """Build a FlatFieldCache by analyzing all flat-field files.

    Args:
        flatfield_paths: List of flat-field .dat file paths.
        max_delta: Maximum time delta for matching.
        max_angle_delta: Maximum allowed angular difference in degrees between
            the derotator position angles of a measurement and a flat-field.
            Flat-fields whose angle differs by more than this value are excluded
            from association.
        allow_cached_data: If True, allows using cached analysis results if available.
        cache_dir: Optional directory override for storing correction cache FITS
            files. When provided, cached files are placed directly under this
            directory instead of the default day-structure-derived path. Useful
            when processing individual measurements outside the standard dataset
            hierarchy.

    Returns:
        Populated FlatFieldCache.
    """
    cache = FlatFieldCache(max_delta=max_delta, max_angle_delta=max_angle_delta)

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
