"""Prefect 3.x orchestration flows.

Two flows:
1. dataset_scan_flow — Scans dataset root and triggers day processing
2. day_processing_flow — Processes a single observation day

The orchestration layer contains NO scientific logic — it only
calls pipeline functions and handles flow/task coordination.
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Optional


from loguru import logger
from prefect import unmapped, flow, task
from prefect.task_runners import ThreadPoolTaskRunner
from irsol_data_pipeline.orchestration.patch_logging import setup_logging
from irsol_data_pipeline.io.filesystem import ObservationDay
from irsol_data_pipeline.pipeline.day_processor import (
    DayProcessingResult,
    MaxDeltaPolicy,
    process_observation_day,
)
from irsol_data_pipeline.pipeline.scanner import ScanResult, scan_dataset


max_workers = max(1, min(8, os.cpu_count() or 1))
print(f"Configuring ProcessPoolTaskRunner with {max_workers} workers")
task_worker = ThreadPoolTaskRunner(max_workers=max_workers)


@task(name="scan-dataset", task_run_name="scan-dataset-for-{root}", retries=2)
def scan_dataset_task(root: Path) -> ScanResult:
    """Prefect task: scan the dataset root."""
    return scan_dataset(root)


@task(
    name="process-observation-day",
    task_run_name="process-observation-for-{day.path.name}",
    retries=2,
)
def process_day_task(
    day: ObservationDay,
    max_delta_hours: float = 2.0,
    refdata_dir: Optional[Path] = None,
) -> DayProcessingResult:
    """Prefect task: process a single observation day."""
    policy = MaxDeltaPolicy(default_max_delta=datetime.timedelta(hours=max_delta_hours))
    return process_observation_day(
        day=day,
        max_delta_policy=policy,
        refdata_dir=refdata_dir,
    )


@flow(
    name="dataset-scan",
    flow_run_name="dataset-scan-for-{root}",
    description="Scans the dataset and processes all days with pending measurements",
    task_runner=task_worker,
)
def dataset_scan_flow(
    root: Optional[str] = None,
    max_delta_hours: float = 2.0,
    refdata_dir: Optional[str] = None,
) -> list[DayProcessingResult]:
    """Scan the dataset and process all days with pending measurements.

    Args:
        root: Dataset root path. Falls back to SOLAR_PIPELINE_ROOT env var.
        max_delta_hours: Maximum flat-field time delta in hours.
        refdata_dir: Path to wavelength calibration reference data.

    Returns:
        List of DayProcessingResult for each processed day.
    """
    setup_logging()

    logger.info(
        "Starting dataset scan flow", root=root, max_delta_hours=max_delta_hours
    )
    if root is None:
        root = os.environ.get("SOLAR_PIPELINE_ROOT")
        if root is None:
            raise ValueError(
                "Dataset root not provided and SOLAR_PIPELINE_ROOT not set"
            )

    dataset_root = Path(root)
    ref_path = Path(refdata_dir) if refdata_dir else None

    # Scan
    scan_result = scan_dataset_task(dataset_root)
    logger.info(
        "Scan complete",
        days=len(scan_result.observation_days),
        pending=scan_result.total_pending,
    )

    if scan_result.total_pending == 0:
        logger.info("No pending measurements found")
        return []

    # Process each day with pending measurements
    selected_observation_days = [
        day
        for day in scan_result.observation_days
        if day.name in scan_result.pending_measurements
    ]

    results = process_day_task.map(
        selected_observation_days,
        unmapped(max_delta_hours),
        unmapped(ref_path),
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
    name="day-processing",
    flow_run_name="day-processing-for-{day_path}",
    description="Processes a single observation day",
)
def day_processing_flow(
    day_path: str,
    max_delta_hours: float = 2.0,
    refdata_dir: Optional[str] = None,
) -> DayProcessingResult:
    """Process a single observation day.

    Args:
        day_path: Path to the observation day directory.
        max_delta_hours: Maximum flat-field time delta in hours.
        refdata_dir: Path to wavelength calibration reference data.

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
        raw_dir=path / "raw",
        reduced_dir=path / "reduced",
        processed_dir=path / "processed",
    )

    ref_path = Path(refdata_dir) if refdata_dir else None

    result = process_day_task(
        day=day,
        max_delta_hours=max_delta_hours,
        refdata_dir=ref_path,
    )

    logger.success(
        "Day processing complete",
        day=result.day_name,
        processed=result.processed,
        skipped=result.skipped,
        failed=result.failed,
    )

    return result


if __name__ == "__main__":
    # Example usage: run the dataset scan flow
    dataset_scan_flow(
        root="/home/deldoc/Documents/code/irsol-data-pipeline/data",
        max_delta_hours=2.0,
    )
