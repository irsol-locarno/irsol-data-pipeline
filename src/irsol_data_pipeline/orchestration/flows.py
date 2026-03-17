"""Prefect 3.x orchestration flows.

Two flows:
1. process_unprocessed_measurements — Scans dataset root and triggers day processing
2. process_daily_unprocessed_measurements — Processes a single observation day

The orchestration layer contains NO scientific logic — it only
calls pipeline functions and handles flow/task coordination.
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path

from loguru import logger
from prefect import flow, task
from prefect.futures import as_completed
from prefect.task_runners import ThreadPoolTaskRunner

from irsol_data_pipeline.core.models import (
    DayProcessingResult,
    MaxDeltaPolicy,
    ObservationDay,
)
from irsol_data_pipeline.orchestration.patch_logging import setup_logging
from irsol_data_pipeline.orchestration.utils import create_prefect_markdown_report
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
    build_scan_report_markdown,
    scan_dataset,
)


@task(task_run_name="find-observations-to-process/{root}")
def scan_dataset_task(root: Path) -> ScanResult:
    """Prefect task: scan the dataset root."""
    scan_result = scan_dataset(root)
    markdown = build_scan_report_markdown(root=root, scan_result=scan_result)
    create_prefect_markdown_report(
        content=markdown,
        description="Dataset scan summary: pending and already processed measurements",
    )
    return scan_result


@task(
    task_run_name="{day_path.name}",
)
def run_day_processing_subflow_task(
    day_path: Path,
    max_delta_hours: float = 2.0,
) -> DayProcessingResult:
    """Prefect task: execute the day-processing flow as a sub-flow."""
    return process_daily_unprocessed_measurements(
        day_path=day_path,
        max_delta_hours=max_delta_hours,
    )


@flow(
    flow_run_name="process-unprocessed-measurements/{root}",
    description="Scans the dataset and processes all days with pending measurements",
)
def process_unprocessed_measurements(
    root: str,
    max_delta_hours: float = 2.0,
    max_concurrent_days_to_process: int = max(1, min(12, (os.cpu_count() or 1) - 1)),
) -> list[DayProcessingResult]:
    """Scan the dataset and process all days with pending measurements.

    Args:
        root: Dataset root path.
        max_delta_hours: Maximum flat-field time delta in hours.
        max_concurrent_days_to_process: Maximum number of concurrent day processing tasks. Defaults to CPU count - 1, capped at 12.

    Returns:
        List of DayProcessingResult for each processed day.
    """
    setup_logging()
    logger.info(
        "Starting dataset scan flow", root=root, max_delta_hours=max_delta_hours
    )

    dataset_root = Path(root)
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
        result_futures = []
        for day_path in selected_day_paths:
            future = runner.submit(
                run_day_processing_subflow_task,
                {
                    "day_path": day_path,
                    "max_delta_hours": max_delta_hours,
                },
            )
            result_futures.append(future)
        results = []
        for result_future in as_completed(result_futures):
            result = result_future.result()
            results.append(result)
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
    flow_run_name="process-unprocessed-daily-measurements/{day_path.name}",
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
