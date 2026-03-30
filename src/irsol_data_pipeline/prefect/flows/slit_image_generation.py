"""Prefect 3.x orchestration flows for the slit image generation pipeline.

Two flows:
1. generate_slit_images (slit-images-full) — Scans dataset root and generates slit
   previews only for observation days that have at least one pending measurement
   (i.e. a measurement without an existing slit preview or error file).
2. generate_daily_slit_images (slit-images-daily) — Generates slit previews for a
   single observation day.

Naming convention: slit-images/<scope>[/<context>]
  Flows:  slit-images-full / slit-images-daily
  Tasks:  slit-images/<verb>-<noun>/<context>
"""

from __future__ import annotations

import datetime
import os
from collections.abc import Callable
from pathlib import Path

from loguru import logger
from prefect import flow, task, unmapped
from prefect.task_runners import ThreadPoolTaskRunner

from irsol_data_pipeline.core.config import DEFAULT_JSOC_DATA_DELAY_DAYS
from irsol_data_pipeline.core.models import (
    DayProcessingResult,
    ObservationDay,
)
from irsol_data_pipeline.pipeline.filesystem import (
    processed_dir_for_day,
    raw_dir_for_day,
    reduced_dir_for_day,
)
from irsol_data_pipeline.pipeline.scanner import (
    ScanResult,
    build_slit_scan_report_markdown,
    scan_slit_dataset,
)
from irsol_data_pipeline.pipeline.slit_images_processor import (
    generate_slit_images_for_day,
)
from irsol_data_pipeline.prefect.patch_logging import setup_logging
from irsol_data_pipeline.prefect.utils import create_prefect_markdown_report
from irsol_data_pipeline.prefect.variables import (
    PrefectVariableName,
    get_variable,
    resolve_dataset_root,
)


def _build_min_age_day_predicate(
    *,
    min_age_days: int,
    today: datetime.date,
) -> Callable[[ObservationDay], bool]:
    """Create an inclusive minimum-age predicate for observation-day folders.

    Args:
        min_age_days: Minimum required age in days.
        today: Current UTC date.

    Returns:
        Predicate returning ``True`` for eligible observation days.
    """

    cutoff = today - datetime.timedelta(days=min_age_days)

    def _predicate(day: ObservationDay) -> bool:
        day_date = day.date
        if day_date is None:
            return False
        return day_date <= cutoff

    return _predicate


def _resolve_jsoc_data_delay_days(raw_value: object) -> int:
    """Resolve the configured JSOC delay in days.

    Args:
        raw_value: Raw value fetched from Prefect Variable storage.

    Returns:
        Non-negative delay in days.
    """

    try:
        delay_days = int(str(raw_value).strip())
    except (TypeError, ValueError):
        logger.warning(
            "Invalid JSOC delay value, using default",
            value=raw_value,
            default=DEFAULT_JSOC_DATA_DELAY_DAYS,
        )
        return DEFAULT_JSOC_DATA_DELAY_DAYS

    if delay_days < 0:
        logger.warning(
            "Negative JSOC delay value, using default",
            value=raw_value,
            default=DEFAULT_JSOC_DATA_DELAY_DAYS,
        )
        return DEFAULT_JSOC_DATA_DELAY_DAYS

    return delay_days


@task(task_run_name="slit-images/scan-dataset/{root}")
def scan_slit_dataset_task(
    root: Path,
    jsoc_data_delay_days: int,
) -> ScanResult:
    """Prefect task: scan the dataset root for pending slit preview work.

    Discovers all observation days that satisfy the JSOC data delay predicate
    and identifies which measurements still need a slit preview image.
    """
    scan_result = scan_slit_dataset(
        root,
        predicate=_build_min_age_day_predicate(
            min_age_days=jsoc_data_delay_days,
            today=datetime.datetime.now(datetime.timezone.utc).date(),
        ),
    )
    markdown = build_slit_scan_report_markdown(root=root, scan_result=scan_result)
    create_prefect_markdown_report(
        content=markdown,
        description="Slit image generation scan summary",
        key=f"slit-image-generation-scan-{root.name}",
    )
    return scan_result


@task(task_run_name="slit-images/generate-day/{day_path.name}")
def run_day_slit_generation_task(
    day_path: Path,
    jsoc_email: str,
    use_limbguider: bool = False,
) -> DayProcessingResult:
    """Prefect task: generate slit images for a single day."""
    with logger.contextualize(day=day_path.name):
        logger.info("Submitting day slit generation task")
        result = generate_daily_slit_images(
            day_path=day_path,
            jsoc_email=jsoc_email,
            use_limbguider=use_limbguider,
        )
        logger.success("Daily slit generation completed")
        return result


@flow(
    name="slit-images-full",
    flow_run_name="slit-images/full/{root}",
    description="Scans the dataset and generates slit preview images for all observation days with pending work",
)
def generate_slit_images(
    root: str = "",
    jsoc_email: str = "",
    use_limbguider: bool = False,
    max_concurrent_days: int = max(1, min(4, (os.cpu_count() or 1) - 1)),
) -> list[DayProcessingResult]:
    """Scan the dataset and generate slit preview images for all days with
    pending work.

    Observation days for which every measurement already has a slit preview
    (or a slit preview error file) are skipped entirely — no sub-task is
    submitted for those days.  This mirrors the behaviour of the flat-field
    correction pipeline.

    Args:
        root: Dataset root path. If not set, the default path from Prefect Variable is used.
        jsoc_email: Optional JSOC email override for DRMS queries. If unset,
            the Prefect Variable ``jsoc-email`` is used.
        use_limbguider: Whether to try using limbguider coordinates.
        max_concurrent_days: Maximum number of concurrent day processing
            tasks. Defaults to CPU count - 1, capped at 4
            (lower than flat-field correction due to network I/O).

    Returns:
        List of DayProcessingResult for each processed day.
    """
    setup_logging()

    email = jsoc_email or get_variable(PrefectVariableName.JSOC_EMAIL, default="")
    if not email:
        logger.error(
            "No JSOC email provided. Set the "
            f"'{PrefectVariableName.JSOC_EMAIL.value}' Prefect Variable "
            "or pass 'jsoc_email' as a flow parameter."
        )
        return []

    dataset_root = resolve_dataset_root(root)

    jsoc_data_delay_days = _resolve_jsoc_data_delay_days(
        get_variable(
            PrefectVariableName.JSOC_DATA_DELAY_DAYS,
            default=str(DEFAULT_JSOC_DATA_DELAY_DAYS),
        )
    )
    logger.info("Starting slit image generation", root=dataset_root, jsoc_email=email)

    scan_result = scan_slit_dataset_task(
        root=dataset_root,
        jsoc_data_delay_days=jsoc_data_delay_days,
    )
    logger.info(
        "Scan complete",
        days=len(scan_result.observation_days),
        pending=scan_result.total_pending,
        jsoc_data_delay_days=jsoc_data_delay_days,
    )

    if scan_result.total_pending == 0:
        logger.info("No pending slit preview measurements found")
        return []

    day_paths = [
        day.path
        for day in scan_result.observation_days
        if day.name in scan_result.pending_measurements
    ]

    with ThreadPoolTaskRunner(max_workers=max_concurrent_days) as runner:
        results = runner.map(
            run_day_slit_generation_task,
            parameters={
                "day_path": day_paths,
                "jsoc_email": unmapped(email),
                "use_limbguider": unmapped(use_limbguider),
            },
        ).result()

    total_processed = sum(r.processed for r in results)
    total_failed = sum(r.failed for r in results)
    logger.success(
        "Slit image generation complete",
        processed=total_processed,
        failed=total_failed,
        days=len(results),
    )

    return results


@flow(
    name="slit-images-daily",
    flow_run_name="slit-images/daily/{day_path.name}",
    description="Generates slit preview images for a single observation day",
)
def generate_daily_slit_images(
    day_path: Path,
    jsoc_email: str = "",
    use_limbguider: bool = False,
) -> DayProcessingResult:
    """Generate slit preview images for a single observation day.

    Args:
        day_path: Path to the observation day directory.
        jsoc_email: Optional JSOC email override for DRMS queries. If unset,
            the Prefect Variable ``jsoc-email`` is used.
        use_limbguider: Whether to try using limbguider coordinates.

    Returns:
        DayProcessingResult summary.
    """
    setup_logging()

    email = jsoc_email or get_variable(PrefectVariableName.JSOC_EMAIL, default="")
    if not email:
        logger.error(
            "No JSOC email provided. Set the "
            f"'{PrefectVariableName.JSOC_EMAIL.value}' Prefect Variable "
            "or pass 'jsoc_email' as a flow parameter."
        )
        return DayProcessingResult(
            day_name=Path(day_path).name, errors=["No JSOC email"]
        )

    logger.info(
        "Starting day slit generation",
        day=day_path.name,
        jsoc_email=email,
        use_limbguider=use_limbguider,
    )

    path = Path(day_path)
    day = ObservationDay(
        path=path,
        raw_dir=raw_dir_for_day(path),
        reduced_dir=reduced_dir_for_day(path),
        processed_dir=processed_dir_for_day(path),
    )

    result = generate_slit_images_for_day(
        day=day,
        jsoc_email=email,
        use_limbguider=use_limbguider,
    )

    logger.success(
        "Day slit generation complete",
        day=result.day_name,
        processed=result.processed,
        skipped=result.skipped,
        failed=result.failed,
    )

    return result
