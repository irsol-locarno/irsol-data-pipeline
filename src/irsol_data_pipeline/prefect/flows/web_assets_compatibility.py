"""Prefect orchestration flows for web-assets compatibility deployment.

Two flows:
1. publish_web_assets_for_root (web-assets-compatibility-full) — scans a dataset
   root and processes all discovered observation days.
2. publish_web_assets_for_day (web-assets-compatibility-daily) — processes one
   day folder only.
"""

from __future__ import annotations

import os
from pathlib import Path

from loguru import logger
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

from irsol_data_pipeline.core.config import DEFAULT_PIOMBO_BASE_PATH
from irsol_data_pipeline.core.models import DayProcessingResult, ObservationDay
from irsol_data_pipeline.integrations.piombo import SftpRemoteFileSystem
from irsol_data_pipeline.pipeline.filesystem import (
    discover_observation_days,
    processed_dir_for_day,
    raw_dir_for_day,
    reduced_dir_for_day,
)
from irsol_data_pipeline.pipeline.web_asset_compatibility import (
    process_day_web_asset_compatibility,
)
from irsol_data_pipeline.prefect.patch_logging import setup_logging
from irsol_data_pipeline.prefect.utils import create_prefect_markdown_report
from irsol_data_pipeline.prefect.variables import (
    PrefectVariableName,
    get_variable,
    resolve_dataset_root,
)


def _build_remote_fs(
    piombo_hostname: str,
    piombo_username: str,
    piombo_password: str,
    piombo_base_path: str,
) -> SftpRemoteFileSystem:
    """Build a
    :class:`~irsol_data_pipeline.integrations.piombo.SftpRemoteFileSystem` from
    credentials.

    Args:
        piombo_hostname: SSH hostname.  Pass an empty string to use local upload.
        piombo_username: SSH username.  Pass an empty string to use local upload.
        piombo_password: SSH password.  Pass an empty string to use local upload.
        piombo_base_path: Base destination path on Piombo used by SFTP uploads.

    Returns:
        A configured :class:`SftpRemoteFileSystem` when all three credentials
        are provided.

    Raises:
        ValueError: If only some credentials are provided.
    """
    provided = [
        bool(piombo_hostname.strip()),
        bool(piombo_username.strip()),
        bool(piombo_password.strip()),
    ]
    if not all(provided):
        raise ValueError(
            "piombo_hostname, piombo_username, and piombo_password must "
            "all be provided together"
        )
    return SftpRemoteFileSystem(
        hostname=piombo_hostname,
        username=piombo_username,
        password=piombo_password,
        base_path=piombo_base_path,
    )


@task(task_run_name="web-assets-compatibility/scan-dataset/{root}")
def scan_observation_days_task(root: Path) -> list[ObservationDay]:
    """Prefect task: discover observation days under the dataset root.

    Args:
        root: Dataset root in the canonical hierarchy.

    Returns:
        Sorted observation-day contexts.
    """

    observation_days = discover_observation_days(root)
    summary_lines = [
        "# Web Assets Compatibility Scan",
        "",
        f"**Root**: `{root}`",
        f"**Observation days found**: {len(observation_days)}",
    ]
    if observation_days:
        summary_lines += ["", "## Days found", ""]
        summary_lines += [f"- `{day.name}`" for day in observation_days]

    create_prefect_markdown_report(
        content="\n".join(summary_lines),
        description="Web-assets compatibility scan summary",
        key=f"web-assets-compatibility-scan-{root.name}",
    )
    return observation_days


@task(task_run_name="web-assets-compatibility/process-day/{day_path.name}")
def run_day_web_assets_subflow_task(
    day_path: Path,
    piombo_base_path: str = DEFAULT_PIOMBO_BASE_PATH,
    piombo_hostname: str = "",
    piombo_username: str = "",
    piombo_password: str = "",
    jpeg_quality: int = 50,
    force_overwrite: bool = False,
) -> DayProcessingResult:
    """Prefect task: execute the day flow as a sub-flow.

    Args:
        day_path: Observation day folder.
        piombo_base_path: Base destination path on Piombo.
        piombo_hostname: SSH hostname for Piombo upload.
        piombo_username: SSH username for Piombo upload.
        piombo_password: SSH password for Piombo upload.
        jpeg_quality: JPEG quality used for conversion.
        force_overwrite: Whether to overwrite existing JPG outputs.

    Returns:
        Day-level compatibility processing summary.
    """

    return publish_web_assets_for_day(
        day_path=day_path,
        piombo_base_path=piombo_base_path,
        piombo_hostname=piombo_hostname,
        piombo_username=piombo_username,
        piombo_password=piombo_password,
        jpeg_quality=jpeg_quality,
        force_overwrite=force_overwrite,
    )


@flow(
    name="web-assets-compatibility-full",
    flow_run_name="web-assets-compatibility/full/{root}",
    description=(
        "Scans a dataset root and deploys legacy-compatible quicklook/context JPG "
        "assets for all day folders"
    ),
)
def publish_web_assets_for_root(
    root: str = "",
    piombo_base_path: str = "",
    piombo_hostname: str = "",
    piombo_username: str = "",
    piombo_password: str = "",
    jpeg_quality: int = 50,
    force_overwrite: bool = False,
    max_concurrent_days: int = max(1, min(8, (os.cpu_count() or 1) - 1)),
) -> list[DayProcessingResult]:
    """Scan a root and run web-assets compatibility processing per day.

    Args:
        root: Dataset root path; if empty, Prefect variable default is used.
        piombo_base_path: Piombo base path; if not provided, Prefect variable default is used.
        piombo_hostname: SSH hostname for Piombo upload; if not provided, Prefect variable default is used.
        piombo_username: SSH username for Piombo upload; if not provided, Prefect variable default is used.
        piombo_password: SSH password for Piombo upload; if not provided, Prefect variable default is used.
        jpeg_quality: JPEG quality used for conversion.
        force_overwrite: Whether to overwrite existing JPG outputs.
        max_concurrent_days: Maximum number of day subflows to run concurrently.

    Returns:
        One DayProcessingResult per scanned day.
    """

    setup_logging()
    dataset_root = resolve_dataset_root(root)
    piombo_base_path = piombo_base_path or get_variable(
        PrefectVariableName.PIOMBO_BASE_PATH,
        default="",
    )
    piombo_hostname = piombo_hostname or get_variable(
        PrefectVariableName.PIOMBO_HOSTNAME,
        default="",
    )
    piombo_username = piombo_username or get_variable(
        PrefectVariableName.PIOMBO_USERNAME,
        default="",
    )
    piombo_password = piombo_password or get_variable(
        PrefectVariableName.PIOMBO_PASSWORD,
        default="",
    )

    logger.info(
        "Starting web-assets compatibility flow",
        root=dataset_root,
        piombo_base_path=piombo_base_path,
        jpeg_quality=jpeg_quality,
        force_overwrite=force_overwrite,
        max_concurrent_days=max_concurrent_days,
    )

    observation_days = scan_observation_days_task(root=dataset_root)
    if not observation_days:
        logger.info("No observation days found")
        return []

    day_paths = [day.path for day in observation_days]
    with ThreadPoolTaskRunner(max_workers=max_concurrent_days) as runner:
        result_futures = []
        for day_path in day_paths:
            result_futures.append(
                runner.submit(
                    run_day_web_assets_subflow_task,
                    {
                        "day_path": day_path,
                        "piombo_base_path": str(piombo_base_path),
                        "piombo_hostname": str(piombo_hostname),
                        "piombo_username": str(piombo_username),
                        "piombo_password": str(piombo_password),
                        "jpeg_quality": jpeg_quality,
                        "force_overwrite": force_overwrite,
                    },
                )
            )
        results = [result_future.result() for result_future in result_futures]

    logger.success(
        "Web-assets compatibility root flow complete",
        days=len(results),
        processed=sum(result.processed for result in results),
        skipped=sum(result.skipped for result in results),
        failed=sum(result.failed for result in results),
    )
    return results


@flow(
    name="web-assets-compatibility-daily",
    flow_run_name="web-assets-compatibility/daily/{day_path.name}",
    description="Converts and deploys compatible web assets for one day folder",
)
def publish_web_assets_for_day(
    day_path: Path,
    piombo_base_path: str = "",
    piombo_hostname: str = "",
    piombo_username: str = "",
    piombo_password: str = "",
    jpeg_quality: int = 50,
    force_overwrite: bool = False,
) -> DayProcessingResult:
    """Convert and deploy compatible web assets for one day.

    Args:
        day_path: Observation day directory path.
        piombo_base_path: Piombo base path; if not provided, Prefect variable default is used.
        piombo_hostname: SSH hostname for Piombo upload; if not provided, Prefect variable default is used.
        piombo_username: SSH username for Piombo upload; if not provided, Prefect variable default is used.
        piombo_password: SSH password for Piombo upload; if not provided, Prefect variable default is used.
        jpeg_quality: JPEG quality used for conversion.
        force_overwrite: Whether to overwrite existing JPG outputs.

    Returns:
        Day-level compatibility processing summary.
    """

    setup_logging()

    path = Path(day_path)
    day = ObservationDay(
        path=path,
        raw_dir=raw_dir_for_day(path),
        reduced_dir=reduced_dir_for_day(path),
        processed_dir=processed_dir_for_day(path),
    )

    piombo_base_path = piombo_base_path or get_variable(
        PrefectVariableName.PIOMBO_BASE_PATH,
        default="",
    )
    piombo_hostname = piombo_hostname or get_variable(
        PrefectVariableName.PIOMBO_HOSTNAME,
        default="",
    )
    piombo_username = piombo_username or get_variable(
        PrefectVariableName.PIOMBO_USERNAME,
        default="",
    )
    piombo_password = piombo_password or get_variable(
        PrefectVariableName.PIOMBO_PASSWORD,
        default="",
    )

    with _build_remote_fs(
        piombo_hostname=str(piombo_hostname),
        piombo_username=str(piombo_username),
        piombo_password=str(piombo_password),
        piombo_base_path=str(piombo_base_path),
    ) as remote_fs:
        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
            jpeg_quality=jpeg_quality,
            force_overwrite=force_overwrite,
        )

    logger.success(
        "Web-assets compatibility day flow complete",
        day=result.day_name,
        processed=result.processed,
        skipped=result.skipped,
        failed=result.failed,
    )
    return result
