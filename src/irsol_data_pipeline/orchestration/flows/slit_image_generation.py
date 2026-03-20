"""Prefect 3.x orchestration flows for the slit image generation pipeline.

Two flows:
1. generate_slit_images (slit-images-full) — Scans dataset root and generates slit
   previews for all observation days.
2. generate_daily_slit_images (slit-images-daily) — Generates slit previews for a
   single observation day.

Naming convention: slit-images/<scope>[/<context>]
  Flows:  slit-images-full / slit-images-daily
  Tasks:  slit-images/<verb>-<noun>/<context>
"""

from __future__ import annotations

import os
from pathlib import Path

from loguru import logger
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

from irsol_data_pipeline.core.models import (
    DayProcessingResult,
    ObservationDay,
)
from irsol_data_pipeline.orchestration.patch_logging import setup_logging
from irsol_data_pipeline.orchestration.utils import create_prefect_markdown_report
from irsol_data_pipeline.orchestration.variables import (
    PrefectVariableName,
    get_variable,
    resolve_dataset_root,
)
from irsol_data_pipeline.pipeline.filesystem import (
    discover_observation_days,
    processed_dir_for_day,
    raw_dir_for_day,
    reduced_dir_for_day,
)
from irsol_data_pipeline.pipeline.slit_images_processor import (
    generate_slit_images_for_day,
)


@task(task_run_name="slit-images/scan-dataset/{root}")
def scan_observation_days_task(root: Path) -> list[ObservationDay]:
    """Prefect task: discover all observation days under the dataset root."""
    observation_days = discover_observation_days(root)
    summary_lines = [
        "# Slit Image Generation Scan",
        "",
        f"**Root**: `{root}`",
        f"**Observation days**: {len(observation_days)}",
    ]
    if observation_days:
        summary_lines += ["", "## Days found", ""]
        summary_lines += [f"- `{day.name}`" for day in observation_days]
    create_prefect_markdown_report(
        content="\n".join(summary_lines),
        description="Slit image generation scan summary",
        key=f"slit-image-generation-scan-{root.name}",
    )
    return observation_days


@task(task_run_name="slit-images/generate-day/{day_path.name}")
def run_day_slit_generation_task(
    day_path: Path,
    jsoc_email: str,
    use_limbguider: bool = False,
) -> DayProcessingResult:
    """Prefect task: generate slit images for a single day."""
    return generate_daily_slit_images(
        day_path=day_path,
        jsoc_email=jsoc_email,
        use_limbguider=use_limbguider,
    )


@flow(
    name="slit-images-full",
    flow_run_name="slit-images/full/{root}",
    description="Scans the dataset and generates slit preview images for all observation days",
)
def generate_slit_images(
    root: str = "",
    jsoc_email: str = "",
    use_limbguider: bool = False,
    max_concurrent_days: int = max(1, min(4, (os.cpu_count() or 1) - 1)),
) -> list[DayProcessingResult]:
    """Scan the dataset and generate slit preview images for all days.

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
    logger.info("Starting slit image generation", root=dataset_root, jsoc_email=email)

    observation_days = scan_observation_days_task(root=dataset_root)
    logger.info(
        "Scan complete",
        days=len(observation_days),
    )

    if not observation_days:
        logger.info("No observation days found")
        return []

    day_paths = [day.path for day in observation_days]

    with ThreadPoolTaskRunner(max_workers=max_concurrent_days) as runner:
        result_futures = []
        for day_path in day_paths:
            future = runner.submit(
                run_day_slit_generation_task,
                {
                    "day_path": day_path,
                    "jsoc_email": email,
                    "use_limbguider": use_limbguider,
                },
            )
            result_futures.append(future)

        results = [result_future.result() for result_future in result_futures]

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
