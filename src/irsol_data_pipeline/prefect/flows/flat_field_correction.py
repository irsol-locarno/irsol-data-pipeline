"""Prefect 3.x orchestration flows for the flat-field correction pipeline.

Two flows:
1. process_unprocessed_measurements (ff-correction-full) — Scans dataset root and
   processes all days with pending measurements.
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
from irsol_data_pipeline.pipeline.day_processor import (
    process_observation_day,
)
from irsol_data_pipeline.pipeline.filesystem import (
    processed_dir_for_day,
    raw_dir_for_day,
    reduced_dir_for_day,
)
from irsol_data_pipeline.pipeline.scanner import (
    ScanResult,
    build_scan_flatfield_report_markdown,
    scan_flatfield_dataset,
)
from irsol_data_pipeline.prefect.patch_logging import setup_logging
from irsol_data_pipeline.prefect.utils import create_prefect_markdown_report
from irsol_data_pipeline.prefect.variables import resolve_dataset_root


@task(task_run_name="ff-correction/scan-dataset/{root}")
def scan_dataset_task(root: Path) -> ScanResult:
    """Prefect task: scan the dataset root."""
    scan_result = scan_flatfield_dataset(root)
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
    max_delta_hours: float = 2.0,
) -> DayProcessingResult:
    """Prefect task: execute the day-processing flow as a sub-flow."""
    with logger.contextualize(day=day_path.name):
        logger.info("Submitting day flat field correction task")
        result = process_daily_unprocessed_measurements(
            day_path=day_path,
            max_delta_hours=max_delta_hours,
        )
        logger.success("Daily flat field correction completed")
        return result


@flow(
    name="ff-correction-full",
    flow_run_name="ff-correction/full/{root}",
    description="Scans the dataset and processes all days with pending measurements",
)
def process_unprocessed_measurements(
    root: str = "",
    max_delta_hours: float = 2.0,
    max_concurrent_days_to_process: int = max(1, min(12, (os.cpu_count() or 1) - 1)),
) -> list[DayProcessingResult]:
    """Scan the dataset and process all days with pending measurements.

    Args:
        root: Dataset root path, if not set, the default path from Prefect Variable is used.
        max_delta_hours: Maximum flat-field time delta in hours.
        max_concurrent_days_to_process: Maximum number of concurrent day processing tasks. Defaults to CPU count - 1, capped at 12.

    Returns:
        List of DayProcessingResult for each processed day.
    """
    setup_logging()
    dataset_root = resolve_dataset_root(root)
    logger.info(
        "Starting dataset scan flow",
        root=dataset_root,
        max_delta_hours=max_delta_hours,
    )

    # Scan
    scan_result = scan_dataset_task(root=dataset_root)
    logger.info(
        "Scan complete",
        days=len(scan_result.observation_days),
        pending=scan_result.total_pending,
    )

    if scan_result.total_pending == 0:
        logger.info("No pending measurements found")
        return []

    # Process each day with pending measurements via the day sub-flow.
    selected_day_paths = [
        day.path
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
) -> DayProcessingResult:
    """Process a single observation day.

    Args:
        day_path: Path to the observation day directory.
        max_delta_hours: Maximum flat-field time delta in hours.

    Returns:
        DayProcessingResult summary.
    """
    setup_logging()
    logger.info(
        "Starting day processing flow",
        day_path=day_path,
        max_delta_hours=max_delta_hours,
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
    )

    logger.success(
        "Day processing complete",
        day=result.day_name,
        processed=result.processed,
        skipped=result.skipped,
        failed=result.failed,
    )

    return result
