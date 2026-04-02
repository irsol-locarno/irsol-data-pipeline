"""Prefect 3.x orchestration flows for the slit image generation pipeline.

Two flows:
1. generate_slit_images (slit-images-full) — Scans one or more dataset roots
   and generates slit previews only for observation days that have at least one
   pending measurement (i.e. a measurement without an existing slit preview or
   error file).
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
from irsol_data_pipeline.prefect.patch_logging import PrefectLogLevel, setup_logging
from irsol_data_pipeline.prefect.utils import create_prefect_markdown_report
from irsol_data_pipeline.prefect.variables import (
    PrefectVariableName,
    get_variable,
    resolve_dataset_roots,
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
    force_override: bool,
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
        force_override=force_override,
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
    use_limbguider: bool,
    log_level: PrefectLogLevel,
    force_override: bool,
) -> DayProcessingResult:
    """Prefect task: generate slit images for a single day."""
    with logger.contextualize(day=day_path.name):
        logger.info("Submitting day slit generation task")
        result = generate_daily_slit_images(
            day_path=day_path,
            jsoc_email=jsoc_email,
            use_limbguider=use_limbguider,
            log_level=log_level,
            force_override=force_override,
        )
        logger.success("Daily slit generation completed")
        return result


@flow(
    name="slit-images-full",
    flow_run_name="slit-images/full",
    description="Scans the dataset roots and generates slit preview images for all observation days with pending work",
)
def generate_slit_images(
    roots: tuple[str, ...] = tuple(),
    jsoc_email: str = "",
    use_limbguider: bool = False,
    max_concurrent_days: int = max(1, min(4, (os.cpu_count() or 1) - 1)),
    log_level: PrefectLogLevel = PrefectLogLevel.INFO,
    log_file: str | None = "slit-images-full.log",
    force_override: bool = False,
) -> list[DayProcessingResult]:
    """Scan one or more dataset roots and generate slit preview images for all
    days with pending work.

    Observation days for which every measurement already has a slit preview
    (or a slit preview error file) are skipped entirely — no sub-task is
    submitted for those days.  This mirrors the behaviour of the flat-field
    correction pipeline.  Pass ``force_override=True`` to reprocess all
    measurements regardless of existing artifacts.

    Args:
        roots: Dataset root path(s). If not set, the default path(s) from the Prefect Variable
            ``data-root-path`` are used.
        jsoc_email: Optional JSOC email override for DRMS queries. If unset,
            the Prefect Variable ``jsoc-email`` is used.
        use_limbguider: Whether to try using limbguider coordinates.
        max_concurrent_days: Maximum number of concurrent day processing
            tasks. Defaults to CPU count - 1, capped at 4
            (lower than flat-field correction due to network I/O).
        log_level: Logging level for the Prefect flow.
        log_file: Path to the rotating log file. Defaults to ``slit-images-full.log``.
            Pass ``None`` to disable file logging.
        force_override: When True, all measurements are reprocessed and output
            files are re-written even if they already exist in the target
            folder.

    Returns:
        List of DayProcessingResult for each processed day.
    """
    setup_logging(level=log_level, log_file=log_file)

    email = jsoc_email or get_variable(PrefectVariableName.JSOC_EMAIL, default="")
    if not email:
        logger.error(
            "No JSOC email provided. Set the "
            f"'{PrefectVariableName.JSOC_EMAIL.value}' Prefect Variable "
            "or pass 'jsoc_email' as a flow parameter.",
        )
        return []

    root_paths = resolve_dataset_roots(roots)

    jsoc_data_delay_days = _resolve_jsoc_data_delay_days(
        get_variable(
            PrefectVariableName.JSOC_DATA_DELAY_DAYS,
            default=str(DEFAULT_JSOC_DATA_DELAY_DAYS),
        ),
    )
    logger.info(
        "Starting slit image generation",
        roots=[str(p) for p in root_paths],
        root_count=len(root_paths),
        jsoc_email=email,
        force_override=force_override,
    )

    # Scan all roots and collect pending day paths
    all_scan_results = [
        scan_slit_dataset_task(
            root=root_path,
            jsoc_data_delay_days=jsoc_data_delay_days,
            force_override=force_override,
        )
        for root_path in root_paths
    ]

    total_pending = sum(r.total_pending for r in all_scan_results)
    logger.info(
        "Scan complete",
        days=sum(len(r.observation_days) for r in all_scan_results),
        pending=total_pending,
        jsoc_data_delay_days=jsoc_data_delay_days,
    )

    if total_pending == 0:
        logger.info("No pending slit preview measurements found")
        return []

    day_paths = [
        day.path
        for scan_result in all_scan_results
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
                "log_level": unmapped(log_level),
                "force_override": unmapped(force_override),
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
    log_level: PrefectLogLevel = PrefectLogLevel.INFO,
    log_file: str | None = "slit-images-daily.log",
    force_override: bool = False,
) -> DayProcessingResult:
    """Generate slit preview images for a single observation day.

    Args:
        day_path: Path to the observation day directory.
        jsoc_email: Optional JSOC email override for DRMS queries. If unset,
            the Prefect Variable ``jsoc-email`` is used.
        use_limbguider: Whether to try using limbguider coordinates.
        log_level: Logging level for the Prefect flow.
        log_file: Path to the rotating log file. Defaults to ``slit-images-daily.log``.
            Pass ``None`` to disable file logging.
        force_override: When True, all measurements are reprocessed and output
            files are re-written even if they already exist in the target
            folder.

    Returns:
        DayProcessingResult summary.
    """
    setup_logging(level=log_level, log_file=log_file)

    email = jsoc_email or get_variable(PrefectVariableName.JSOC_EMAIL, default="")
    if not email:
        logger.error(
            "No JSOC email provided. Set the "
            f"'{PrefectVariableName.JSOC_EMAIL.value}' Prefect Variable "
            "or pass 'jsoc_email' as a flow parameter.",
        )
        return DayProcessingResult(
            day_name=Path(day_path).name,
            errors=["No JSOC email"],
        )

    logger.info(
        "Starting day slit generation",
        day=day_path.name,
        jsoc_email=email,
        use_limbguider=use_limbguider,
        force_override=force_override,
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
        force=force_override,
    )

    logger.success(
        "Day slit generation complete",
        day=result.day_name,
        processed=result.processed,
        skipped=result.skipped,
        failed=result.failed,
    )

    return result
