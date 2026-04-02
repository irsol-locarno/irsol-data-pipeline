"""Prefect 3.x orchestration flows for the flat-field correction pipeline.

Two flows:
1. process_unprocessed_measurements (ff-correction-full) — Scans one or more
   dataset roots and processes all days with pending measurements.
2. process_daily_unprocessed_measurements (ff-correction-daily) — Processes a single
   observation day.

Naming convention: ff-correction/<scope>[/<context>]
  Flows:  ff-correction-full / ff-correction-daily
  Tasks:  ff-correction/<verb>-<noun>/<context>

The prefect layer contains NO scientific logic — it only
calls pipeline functions and handles flow/task coordination.
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path

from loguru import logger
from prefect import flow, task, unmapped
from prefect.task_runners import ThreadPoolTaskRunner

from irsol_data_pipeline.core.models import (
    DayProcessingResult,
    MaxDeltaPolicy,
    ObservationDay,
)
from irsol_data_pipeline.pipeline.filesystem import (
    processed_dir_for_day,
    raw_dir_for_day,
    reduced_dir_for_day,
)
from irsol_data_pipeline.pipeline.flatfield_processor import (
    process_observation_day,
)
from irsol_data_pipeline.pipeline.scanner import (
    ScanResult,
    build_scan_flatfield_report_markdown,
    scan_flatfield_dataset,
)
from irsol_data_pipeline.prefect.patch_logging import PrefectLogLevel, setup_logging
from irsol_data_pipeline.prefect.utils import create_prefect_markdown_report
from irsol_data_pipeline.prefect.variables import resolve_dataset_roots


@task(task_run_name="ff-correction/scan-dataset/{root}")
def scan_dataset_task(root: Path, force_override: bool) -> ScanResult:
    """Prefect task: scan the dataset root."""
    scan_result = scan_flatfield_dataset(root, force_override=force_override)
    markdown = build_scan_flatfield_report_markdown(root=root, scan_result=scan_result)
    create_prefect_markdown_report(
        content=markdown,
        description="Dataset scan summary: pending and already processed measurements",
        key=f"ff-correction-scan-report-{root.name}",
    )
    return scan_result


@task(
    task_run_name="ff-correction/process-day/{day_path.name}",
)
def run_day_processing_subflow_task(
    day_path: Path,
    max_delta_hours: float,
    log_level: PrefectLogLevel,
    convert_on_ff_failure: bool,
    force_override: bool,
) -> DayProcessingResult:
    """Prefect task: execute the day-processing flow as a sub-flow."""
    with logger.contextualize(day=day_path.name):
        logger.info("Submitting day flat field correction task")
        result = process_daily_unprocessed_measurements(
            day_path=day_path,
            max_delta_hours=max_delta_hours,
            log_level=log_level,
            convert_on_ff_failure=convert_on_ff_failure,
            force_override=force_override,
        )
        logger.success("Daily flat field correction completed")
        return result


@flow(
    name="ff-correction-full",
    flow_run_name="ff-correction/full",
    description="Scans the dataset roots and processes all days with pending measurements",
)
def process_unprocessed_measurements(
    roots: tuple[str, ...] = tuple(),
    max_delta_hours: float = 2.0,
    max_concurrent_days_to_process: int = max(1, min(12, (os.cpu_count() or 1) - 1)),
    log_level: PrefectLogLevel = PrefectLogLevel.INFO,
    log_file: str | None = "ff-correction-full.log",
    convert_on_ff_failure: bool = True,
    force_override: bool = False,
) -> list[DayProcessingResult]:
    """Scan one or more dataset roots and process all days with pending
    measurements.

    Args:
        roots: Dataset root path(s). If not set, the default path(s) from the Prefect Variable
            ``data-root-path`` are used.
        max_delta_hours: Maximum flat-field time delta in hours.
        max_concurrent_days_to_process: Maximum number of concurrent day processing tasks. Defaults to CPU count - 1, capped at 12.
        log_level: Logging level for the Prefect flow.
        log_file: Path to the rotating log file. Defaults to ``ff-correction-full.log``.
            Pass ``None`` to disable file logging.
        convert_on_ff_failure: When True, measurements that fail flat-field
            correction are converted to ``*_converted.fits`` FITS files with a
            ``*_profile_converted.png`` profile plot so their data is still
            accessible to downstream consumers.
        force_override: When True, all measurements are reprocessed and output
            files are re-written even if they already exist in the target
            folder.  Measurements that would normally be skipped are processed
            again.

    Returns:
        List of DayProcessingResult for each processed day.
    """
    setup_logging(level=log_level, log_file=log_file)
    root_paths = resolve_dataset_roots(roots)
    logger.info(
        "Starting dataset scan flow",
        roots=[str(p) for p in root_paths],
        root_count=len(root_paths),
        max_delta_hours=max_delta_hours,
        convert_on_ff_failure=convert_on_ff_failure,
        force_override=force_override,
    )

    # Scan all roots and collect pending day paths
    all_scan_results = [
        scan_dataset_task(root=root_path, force_override=force_override)
        for root_path in root_paths
    ]

    total_pending = sum(r.total_pending for r in all_scan_results)
    logger.info(
        "Scan complete",
        days=sum(len(r.observation_days) for r in all_scan_results),
        pending=total_pending,
    )

    if total_pending == 0:
        logger.info("No pending measurements found")
        return []

    # Collect all pending day paths across all roots
    selected_day_paths = [
        day.path
        for scan_result in all_scan_results
        for day in scan_result.observation_days
        if day.name in scan_result.pending_measurements
    ]

    logger.info(
        "Submitting day processing tasks",
        day_count=len(selected_day_paths),
        max_concurrent_days_to_process=max_concurrent_days_to_process,
    )

    with ThreadPoolTaskRunner(max_workers=max_concurrent_days_to_process) as runner:
        results = runner.map(
            run_day_processing_subflow_task,
            parameters={
                "day_path": selected_day_paths,
                "max_delta_hours": unmapped(max_delta_hours),
                "log_level": unmapped(log_level),
                "convert_on_ff_failure": unmapped(convert_on_ff_failure),
                "force_override": unmapped(force_override),
            },
        ).result()

    # Summary
    total_processed = sum(r.processed for r in results)
    total_failed = sum(r.failed for r in results)
    logger.success(
        "Pipeline complete",
        processed=total_processed,
        failed=total_failed,
        days=len(results),
    )

    return results


@flow(
    name="ff-correction-daily",
    flow_run_name="ff-correction/daily/{day_path.name}",
    description="Processes a single observation day",
)
def process_daily_unprocessed_measurements(
    day_path: Path,
    max_delta_hours: float = 2.0,
    log_level: PrefectLogLevel = PrefectLogLevel.INFO,
    log_file: str | None = "ff-correction-daily.log",
    convert_on_ff_failure: bool = True,
    force_override: bool = False,
) -> DayProcessingResult:
    """Process a single observation day.

    Args:
        day_path: Path to the observation day directory.
        max_delta_hours: Maximum flat-field time delta in hours.
        log_level: Logging level for the Prefect flow.
        log_file: Path to the rotating log file. Defaults to ``ff-correction-daily.log``.
            Pass ``None`` to disable file logging.
        convert_on_ff_failure: When True, measurements that fail flat-field
            correction are converted to ``*_converted.fits`` FITS files with a
            ``*_profile_converted.png`` profile plot so their data is still
            accessible to downstream consumers.
        force_override: When True, all measurements are reprocessed and output
            files are re-written even if they already exist in the target
            folder.

    Returns:
        DayProcessingResult summary.
    """
    setup_logging(level=log_level, log_file=log_file)
    logger.info(
        "Starting day processing flow",
        day_path=day_path,
        max_delta_hours=max_delta_hours,
        convert_on_ff_failure=convert_on_ff_failure,
        force_override=force_override,
    )

    path = Path(day_path)
    day = ObservationDay(
        path=path,
        raw_dir=raw_dir_for_day(path),
        reduced_dir=reduced_dir_for_day(path),
        processed_dir=processed_dir_for_day(path),
    )

    policy = MaxDeltaPolicy(default_max_delta=datetime.timedelta(hours=max_delta_hours))
    result = process_observation_day(
        day=day,
        max_delta_policy=policy,
        force=force_override,
        convert_on_ff_failure=convert_on_ff_failure,
    )

    logger.success(
        "Day processing complete",
        day=result.day_name,
        processed=result.processed,
        skipped=result.skipped,
        failed=result.failed,
    )

    return result
