"""Maintenance flows for deleting old cache files.

Thin Prefect orchestration wrappers.  All filesystem and deletion logic lives
in :mod:`irsol_data_pipeline.pipeline.cache_cleanup`.

Two flows are exposed:

- :func:`delete_old_cache_files` — top-level flow that scans the dataset root
  and dispatches one subflow per observation day.
- :func:`delete_old_day_cache_files` — per-day subflow; can also be triggered
  manually for a single day.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from irsol_data_pipeline.core.models import CacheCleanupDayResult, ObservationDay
from irsol_data_pipeline.orchestration.decorators import flow, task
from irsol_data_pipeline.orchestration.patch_logging import setup_logging
from irsol_data_pipeline.pipeline.cache_cleanup import cleanup_day_cache_files
from irsol_data_pipeline.pipeline.filesystem import (
    discover_observation_days,
    processed_dir_for_day,
    raw_dir_for_day,
    reduced_dir_for_day,
)

DEFAULT_CACHE_RETENTION_HOURS = 24 * 7 * 4


@task(task_run_name="maintenance-cache/discover-days/{root.name}")
def scan_observation_days_task(root: Path) -> list[ObservationDay]:
    """Task: discover all observation days under the dataset root.

    Args:
        root: Dataset root.

    Returns:
        Sorted list of :class:`~irsol_data_pipeline.core.models.ObservationDay`
        objects.
    """
    days = discover_observation_days(root)
    logger.info(
        "Discovered observation days for cache cleanup", root=root, count=len(days)
    )
    return days


@flow(
    name="maintenance-cache-cleanup-daily",
    flow_run_name="maintenance/cache-cleanup/daily/{day_path.name}",
    description="Delete old .pkl cache files for one observation day",
)
def delete_old_day_cache_files(
    day_path: Path,
    hours: float = DEFAULT_CACHE_RETENTION_HOURS,
) -> CacheCleanupDayResult:
    """Delete stale cache files for a single observation day.

    Args:
        day_path: Observation day path.
        hours: Cache retention window in hours.

    Returns:
        Cleanup summary for the day.
    """
    setup_logging()
    path = Path(day_path)
    day = ObservationDay(
        path=path,
        raw_dir=raw_dir_for_day(path),
        reduced_dir=reduced_dir_for_day(path),
        processed_dir=processed_dir_for_day(path),
    )
    return cleanup_day_cache_files(day=day, hours=hours)


@flow(
    name="maintenance-cache-cleanup",
    flow_run_name="maintenance/cache-cleanup/{hours}h",
    description=(
        "Delete old .pkl cache files from processed/_cache and processed/_sdo_cache"
    ),
)
def delete_old_cache_files(
    root: str,
    hours: float = DEFAULT_CACHE_RETENTION_HOURS,
) -> list[CacheCleanupDayResult]:
    """Delete stale cache files across all observation days.

    Args:
        root: Dataset root path.
        hours: Cache retention window in hours.

    Returns:
        Per-day cleanup summaries.
    """
    setup_logging()

    root_path = Path(root)
    logger.info("Starting cache cleanup", root=root_path, hours=hours)

    days = scan_observation_days_task(root_path)
    if not days:
        logger.info("No observation days found for cache cleanup", root=root_path)
        return []

    results = [
        delete_old_day_cache_files(day_path=day.path, hours=hours) for day in days
    ]

    logger.success(
        "Cache cleanup completed",
        day_count=len(results),
        checked_files=sum(r.checked_files for r in results),
        deleted_files=sum(r.deleted_files for r in results),
        skipped_recent_files=sum(r.skipped_recent_files for r in results),
        failed_files=sum(r.failed_files for r in results),
    )
    return results
