"""Maintenance flows for deleting old cache files.

Thin Prefect orchestration wrappers.  All filesystem and deletion logic lives
in :mod:`irsol_data_pipeline.pipeline.cache_cleanup`.

Two flows are exposed:

- :func:`delete_old_cache_files` — top-level flow that scans one or more
  dataset roots and dispatches one subflow per observation day.
- :func:`delete_old_day_cache_files` — per-day subflow; can also be triggered
  manually for a single day.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
from prefect import unmapped
from prefect.task_runners import ThreadPoolTaskRunner

from irsol_data_pipeline.core.models import CacheCleanupDayResult, ObservationDay
from irsol_data_pipeline.pipeline.cache_cleanup import (
    build_cache_cleanup_report,
    cleanup_day_cache_files,
)
from irsol_data_pipeline.pipeline.filesystem import (
    discover_observation_days,
    processed_dir_for_day,
    raw_dir_for_day,
    reduced_dir_for_day,
)
from irsol_data_pipeline.prefect.decorators import flow, task
from irsol_data_pipeline.prefect.patch_logging import PrefectLogLevel, setup_logging
from irsol_data_pipeline.prefect.utils import create_prefect_markdown_report
from irsol_data_pipeline.prefect.variables import (
    PrefectVariableName,
    get_variable,
    resolve_dataset_roots,
)


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
        "Discovered observation days for cache cleanup",
        root=root,
        count=len(days),
    )
    return days


@flow(
    name="maintenance-cache-cleanup-daily",
    flow_run_name="maintenance/cache-cleanup/daily/{day_path.name}",
    description="Delete old .fits cache files for one observation day",
)
def delete_old_day_cache_files(
    day_path: Path,
    hours: float = 0.0,
    log_level: PrefectLogLevel = PrefectLogLevel.INFO,
) -> CacheCleanupDayResult:
    """Delete stale cache files for a single observation day.

    Args:
        day_path: Observation day path.
        hours: Optional cache retention window in hours. If unset (0),
            the Prefect Variable ``cache-expiration-hours`` is used.
        log_level: Logging level for the Prefect flow.

    Returns:
        Cleanup summary for the day.
    """
    setup_logging(level=log_level)
    hours = hours or float(
        get_variable(PrefectVariableName.CACHE_EXPIRATION_HOURS, default="672"),
    )
    path = Path(day_path)
    day = ObservationDay(
        path=path,
        raw_dir=raw_dir_for_day(path),
        reduced_dir=reduced_dir_for_day(path),
        processed_dir=processed_dir_for_day(path),
    )
    return cleanup_day_cache_files(day=day, hours=hours)


@task(task_run_name="maintenance-cache/cache-cleanup/daily/{day_path.name}")
def delete_old_day_cache_files_task(
    day_path: Path,
    hours: float,
    log_level: PrefectLogLevel = PrefectLogLevel.INFO,
) -> CacheCleanupDayResult:
    """Task wrapper for :func:`delete_old_day_cache_files`."""
    return delete_old_day_cache_files(
        day_path=day_path, hours=hours, log_level=log_level
    )


@flow(
    name="maintenance-cache-cleanup",
    flow_run_name="maintenance/cache-cleanup",
    task_runner=ThreadPoolTaskRunner(max_workers=4),
    description=("Delete old cache files from processed/_cache"),
)
def delete_old_cache_files(
    roots: tuple[str, ...] = tuple(),
    hours: float = 0.0,
    log_level: PrefectLogLevel = PrefectLogLevel.INFO,
) -> list[CacheCleanupDayResult]:
    """Delete stale cache files across all observation days.

    Args:
        roots: Dataset root path(s).
            If not set, the default path(s) from the Prefect Variable
            ``data-root-path`` are used.
        hours: Optional cache retention window in hours. If unset (0),
            the Prefect Variable ``cache-expiration-hours`` is used.
        log_level: Logging level for the Prefect flow.

    Returns:
        Per-day cleanup summaries.
    """
    setup_logging(level=log_level)

    hours = hours or float(
        get_variable(PrefectVariableName.CACHE_EXPIRATION_HOURS, default="672"),
    )

    root_paths = resolve_dataset_roots(roots)
    logger.info(
        "Starting cache cleanup",
        roots=[str(p) for p in root_paths],
        root_count=len(root_paths),
        hours=hours,
    )

    all_days: list[ObservationDay] = []
    all_results: list[CacheCleanupDayResult] = []
    for root_path in root_paths:
        days = scan_observation_days_task(root_path)
        if not days:
            logger.info("No observation days found for cache cleanup", root=root_path)
            continue

        results = delete_old_day_cache_files_task.map(
            day_path=[day.path for day in days],
            hours=unmapped(hours),
            log_level=unmapped(log_level),
        ).result()
        all_results.extend(results)

        report = build_cache_cleanup_report(
            root=root_path, results=results, hours=hours
        )
        create_prefect_markdown_report(
            content=report,
            description=f"Cache cleanup summary: deleted and retained temporary files per observation day for {root_path}",
            key=f"cache-cleanup-report-{root_path.name}",
        )

    logger.success(
        "Cache cleanup completed",
        day_count=len(all_days),
        checked_files=sum(r.checked_files for r in all_results),
        deleted_files=sum(r.deleted_files for r in all_results),
        deleted_bytes=sum(r.deleted_bytes for r in all_results),
        skipped_recent_files=sum(r.skipped_recent_files for r in all_results),
        skipped_bytes=sum(r.skipped_bytes for r in all_results),
        failed_files=sum(r.failed_files for r in all_results),
    )
    return all_results
