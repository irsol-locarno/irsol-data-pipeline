"""Dataset scanner.

Scans the dataset root to discover observation days that need
processing. Provides scanners for both flat-field correction and slit
image generation, backed by a shared scanning helper.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from loguru import logger

from irsol_data_pipeline.core.models import ObservationDay, ScanResult
from irsol_data_pipeline.pipeline.filesystem import (
    discover_measurement_files,
    discover_observation_days,
    is_measurement_flat_field_processed,
    is_measurement_slit_preview_generated,
)

ObservationDayPredicate = Callable[[ObservationDay], bool]
MeasurementDonePredicate = Callable[[Path, str], bool]


def _scan_dataset(
    root: Path,
    *,
    is_measurment_already_processed: MeasurementDonePredicate,
    day_predicate: ObservationDayPredicate | None = None,
    force_override: bool,
) -> ScanResult:
    """Core dataset scan implementation.

    Discovers observation days (optionally filtered by ``day_predicate``) and,
    for each day, collects the measurements that still need work according to
    the ``is_measurment_already_processed`` callable.

    Args:
        root: The dataset root directory.
        is_measurment_already_processed: Callable ``(processed_dir, source_name) -> bool`` that returns
            ``True`` when a measurement already has its output (e.g.
            :func:`~irsol_data_pipeline.pipeline.filesystem.is_measurement_flat_field_processed`
            for flat-field correction or
            :func:`~irsol_data_pipeline.pipeline.filesystem.is_measurement_slit_preview_generated`
            for slit images).
        day_predicate: Optional filter returning ``True`` for observation days
            that should be included in the scan.
        force_override: When ``True``, all measurements are treated as pending
            regardless of existing output artifacts.

    Returns:
        ScanResult with discovered days and pending measurements.
    """
    days = discover_observation_days(root, predicate=day_predicate)
    pending: dict[str, list[Path]] = {}
    total = 0
    total_pending = 0

    for day in days:
        measurements = discover_measurement_files(day.reduced_dir)
        total += len(measurements)

        if force_override:
            unprocessed = list(measurements)
        else:
            unprocessed = [
                m
                for m in measurements
                if not is_measurment_already_processed(day.processed_dir, m.name)
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


def scan_flatfield_dataset(
    root: Path,
    *,
    force_override: bool = False,
) -> ScanResult:
    """Scan the dataset root and find measurements that need flat-field
    correction.

    For each observation day, checks the ``reduced/`` folder for
    measurement files and the ``processed/`` folder for existing outputs.
    Only measurements without processed outputs are reported unless
    ``force_override`` is ``True``.

    Args:
        root: The dataset root directory.
        force_override: When ``True``, all measurements are treated as pending
            regardless of existing output artifacts.

    Returns:
        ScanResult with discovered days and pending measurements.
    """
    return _scan_dataset(
        root,
        is_measurment_already_processed=is_measurement_flat_field_processed,
        force_override=force_override,
    )


def build_scan_flatfield_report_markdown(root: Path, scan_result: ScanResult) -> str:
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
            ],
        )
        return "\n".join(lines)

    lines.extend(
        [
            "## Pending Measurements",
            "",
            "| Observation Day | Pending Count | Files |",
            "|---|---:|---|",
        ],
    )

    for day_name in sorted(scan_result.pending_measurements):
        files = sorted(p.name for p in scan_result.pending_measurements[day_name])
        lines.append(
            f"| `{day_name}` | {len(files)} | {', '.join(f'`{f}`' for f in files)} |",
        )

    return "\n".join(lines)


def scan_slit_dataset(
    root: Path,
    predicate: ObservationDayPredicate | None = None,
    *,
    force_override: bool = False,
) -> ScanResult:
    """Scan the dataset root and find measurements that need slit preview
    generation.

    For each observation day that satisfies the optional predicate, checks the
    ``reduced/`` folder for measurement files and the ``processed/`` folder for
    existing slit preview outputs.  Only measurements without a slit preview
    (or a slit preview error file) are reported as pending unless
    ``force_override`` is ``True``.

    Args:
        root: The dataset root directory.
        predicate: Optional filter returning ``True`` for observation days that
            should be included in the scan (e.g. a JSOC age predicate).
        force_override: When ``True``, all measurements are treated as pending
            regardless of existing output artifacts.

    Returns:
        ScanResult with discovered days and pending slit-preview measurements.
    """
    return _scan_dataset(
        root,
        is_measurment_already_processed=is_measurement_slit_preview_generated,
        day_predicate=predicate,
        force_override=force_override,
    )


def build_slit_scan_report_markdown(root: Path, scan_result: ScanResult) -> str:
    """Build a markdown summary of slit-image scan results for Prefect
    artifacts.

    Args:
        root: Dataset root directory used for the scan.
        scan_result: Result produced by :func:`scan_slit_dataset`.

    Returns:
        Markdown-formatted report string.
    """
    total_generated = scan_result.total_measurements - scan_result.total_pending
    lines = [
        "# Slit Image Generation Scan Summary",
        "",
        f"- Root: `{root}`",
        f"- Observation days discovered: `{len(scan_result.observation_days)}`",
        f"- Total measurements found: `{scan_result.total_measurements}`",
        f"- Already generated: `{total_generated}`",
        f"- Still to generate: `{scan_result.total_pending}`",
        "",
    ]

    if scan_result.total_pending == 0:
        lines.extend(
            [
                "## Pending Measurements",
                "",
                "No pending slit preview measurements found.",
            ],
        )
        return "\n".join(lines)

    lines.extend(
        [
            "## Pending Measurements",
            "",
            "| Observation Day | Pending Count | Files |",
            "|---|---:|---|",
        ],
    )

    for day_name in sorted(scan_result.pending_measurements):
        files = sorted(p.name for p in scan_result.pending_measurements[day_name])
        lines.append(
            f"| `{day_name}` | {len(files)} | {', '.join(f'`{f}`' for f in files)} |",
        )

    return "\n".join(lines)
