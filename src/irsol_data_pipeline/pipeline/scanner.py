"""Dataset scanner.

Scans the dataset root to discover observation days that need
processing.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from irsol_data_pipeline.core.models import ScanResult
from irsol_data_pipeline.pipeline.filesystem import (
    discover_measurement_files,
    discover_observation_days,
    is_measurement_processed,
)


def scan_dataset(root: Path) -> ScanResult:
    """Scan the dataset root and find measurements that need processing.

    For each observation day, checks the ``reduced/`` folder for
    measurement files and the ``processed/`` folder for existing outputs.
    Only measurements without processed outputs are reported.

    Args:
        root: The dataset root directory.

    Returns:
        ScanResult with discovered days and pending measurements.
    """
    days = discover_observation_days(root)
    pending: dict[str, list[Path]] = {}
    total = 0
    total_pending = 0

    for day in days:
        measurements = discover_measurement_files(day.reduced_dir)
        total += len(measurements)

        unprocessed = [
            m
            for m in measurements
            if not is_measurement_processed(day.processed_dir, m.name)
        ]

        if unprocessed:
            pending[day.name] = unprocessed
            total_pending += len(unprocessed)

        logger.info(
            "Scanned observation day",
            day=day.name,
            measurements=len(measurements),
            pending=len(unprocessed),
        )

    return ScanResult(
        observation_days=days,
        pending_measurements=pending,
        total_measurements=total,
        total_pending=total_pending,
    )


def build_scan_report_markdown(root: Path, scan_result: ScanResult) -> str:
    """Build a markdown summary of scan results for Prefect artifacts."""
    total_processed = scan_result.total_measurements - scan_result.total_pending
    lines = [
        "# Dataset Scan Summary",
        "",
        f"- Root: `{root}`",
        f"- Observation days discovered: `{len(scan_result.observation_days)}`",
        f"- Total measurements found: `{scan_result.total_measurements}`",
        f"- Already processed: `{total_processed}`",
        f"- Still to process: `{scan_result.total_pending}`",
        "",
    ]

    if scan_result.total_pending == 0:
        lines.extend(
            [
                "## Pending Measurements",
                "",
                "No pending measurements found.",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "## Pending Measurements",
            "",
            "| Observation Day | Pending Count | Files |",
            "|---|---:|---|",
        ]
    )

    for day_name in sorted(scan_result.pending_measurements):
        files = sorted(p.name for p in scan_result.pending_measurements[day_name])
        lines.append(
            f"| `{day_name}` | {len(files)} | {', '.join(f'`{f}`' for f in files)} |"
        )

    return "\n".join(lines)
