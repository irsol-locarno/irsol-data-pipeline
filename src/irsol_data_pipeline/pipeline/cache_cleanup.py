"""Cache-file cleanup logic for observation day directories.

Contains the pure filesystem operations for discovering and removing stale
``.pkl`` files from the per-day cache directories (``processed/_cache`` and
``processed/_sdo_cache``).  All orchestration concerns live in the
``orchestration/flows/maintenance`` layer.
"""

from __future__ import annotations

import datetime
from pathlib import Path

from loguru import logger

from irsol_data_pipeline.core.models import CacheCleanupDayResult, ObservationDay
from irsol_data_pipeline.pipeline.filesystem import (
    processed_cache_dir_for_day,
    sdo_cache_dir_for_day,
)


def _cache_directories_for_day(day_path: Path) -> list[Path]:
    """Return existing cache directories for an observation day.

    Args:
        day_path: Observation day path.

    Returns:
        Subset of ``[processed/_cache, processed/_sdo_cache]`` that exist on
        disk.
    """
    candidates = [
        processed_cache_dir_for_day(day_path),
        sdo_cache_dir_for_day(day_path),
    ]
    return [d for d in candidates if d.is_dir()]


def cleanup_day_cache_files(
    day: ObservationDay,
    hours: float,
) -> CacheCleanupDayResult:
    """Delete stale ``.pkl`` cache files for a single observation day.

    Files in ``processed/_cache`` and ``processed/_sdo_cache`` whose
    last-modified time is older than *hours* are removed.  Non-``.pkl``
    files are always left untouched.

    Args:
        day: Observation day to clean up.
        hours: Retention window in hours.  Files older than
            ``now - hours`` are deleted.

    Returns:
        Cleanup summary for the day.
    """
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        hours=hours
    )

    with logger.contextualize(day=day.name, hours=hours, cutoff=cutoff.isoformat()):
        checked = 0
        deleted = 0
        skipped_recent = 0
        failed = 0

        cache_dirs = _cache_directories_for_day(day.path)
        logger.info(
            "Starting day cache cleanup",
            cache_directories=[str(d) for d in cache_dirs],
        )

        for cache_dir in cache_dirs:
            logger.debug("Scanning cache directory", cache_dir=cache_dir)
            pkl_files = sorted(cache_dir.glob("*.pkl"))
            logger.debug(
                "Found .pkl files in cache directory",
                cache_dir=cache_dir,
                count=len(pkl_files),
            )
            for cache_file in pkl_files:
                checked += 1
                modified_at = datetime.datetime.fromtimestamp(
                    cache_file.stat().st_mtime,
                    tz=datetime.timezone.utc,
                )
                logger.trace(
                    "Checking cache file age",
                    file=cache_file.name,
                    modified_at=modified_at.isoformat(),
                    is_stale=modified_at < cutoff,
                )
                if modified_at >= cutoff:
                    skipped_recent += 1
                    logger.debug(
                        "Keeping recent cache file",
                        file=cache_file.name,
                        modified_at=modified_at.isoformat(),
                    )
                    continue
                try:
                    cache_file.unlink()
                    deleted += 1
                    logger.info(
                        "Deleted old cache file",
                        file=cache_file.name,
                        modified_at=modified_at.isoformat(),
                    )
                except OSError:
                    failed += 1
                    logger.exception(
                        "Failed deleting cache file",
                        file=str(cache_file),
                    )

        result = CacheCleanupDayResult(
            day_name=day.name,
            checked_files=checked,
            deleted_files=deleted,
            skipped_recent_files=skipped_recent,
            failed_files=failed,
        )
        logger.info(
            "Completed day cache cleanup",
            checked_files=result.checked_files,
            deleted_files=result.deleted_files,
            skipped_recent_files=result.skipped_recent_files,
            failed_files=result.failed_files,
        )
        return result
