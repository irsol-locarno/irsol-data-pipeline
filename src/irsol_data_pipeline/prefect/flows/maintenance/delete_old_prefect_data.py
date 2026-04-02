"""Maintenance flow for cleaning old Prefect flow-run history.

This module defines an async Prefect flow that removes flow runs older
than a user-provided retention window. It is intended to be served as a
dedicated maintenance deployment, separate from science-processing
deployments.
"""

from __future__ import annotations

import datetime
from uuid import UUID

from loguru import logger
from prefect import flow, get_client, task
from prefect.server.schemas.filters import FlowRunFilter, FlowRunFilterEndTime
from prefect.server.schemas.sorting import FlowRunSort
from prefect.task_runners import ThreadPoolTaskRunner

from irsol_data_pipeline.prefect.patch_logging import PrefectLogLevel, setup_logging
from irsol_data_pipeline.prefect.variables import (
    PrefectVariableName,
    aget_variable,
)


@task(task_run_name="maintenance/delete-run/{flow_run_id}")
async def delete_flow_run_id(flow_run_id: UUID) -> UUID:
    """Delete a Prefect flow run by its ID."""
    async with get_client() as client:
        logger.info("Deleting flow run", flow_run_id=flow_run_id)
        await client.delete_flow_run(flow_run_id=flow_run_id)
    return flow_run_id


_PAGE_SIZE = 200


@task(task_run_name="maintenance/retrieve-old-runs")
async def retrieve_old_flow_ids(dt: datetime.timedelta) -> list[UUID]:
    """Return IDs of flow runs that ended before the provided cutoff.

    Paginates through all results in pages of ``_PAGE_SIZE`` because
    ``read_flow_runs`` returns at most 200 runs per call.

    Args:
        dt: Retention duration. Runs older than `now - dt` are selected.

    Returns:
        List of flow-run UUIDs matching the filter.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    cutoff = now - dt
    logger.info(
        "Retrieving all flows finished before cutoff time",
        cutoff=cutoff.isoformat(),
    )
    flow_run_filter = FlowRunFilter(end_time=FlowRunFilterEndTime(before_=cutoff))
    ids: list[UUID] = []
    offset = 0
    async with get_client() as client:
        while True:
            page = await client.read_flow_runs(
                sort=FlowRunSort.START_TIME_ASC,
                flow_run_filter=flow_run_filter,
                limit=_PAGE_SIZE,
                offset=offset,
            )
            ids.extend(fr.id for fr in page)
            if len(page) < _PAGE_SIZE:
                break
            offset += _PAGE_SIZE
    logger.info("Retrieved old flow runs", count=len(ids))
    return ids


@flow(
    name="maintenance-cleanup",
    task_runner=ThreadPoolTaskRunner(max_workers=4),
    flow_run_name="maintenance/cleanup-flows",
)
async def delete_flow_runs_older_than(
    hours: float = 0.0,
    log_level: PrefectLogLevel = PrefectLogLevel.INFO,
    log_file: str | None = "maintenance-cleanup.log",
) -> bool:
    """Delete Prefect flow runs older than a retention duration.

    Args:
        hours: Optional retention duration in hours. If unset (0), the Prefect
            Variable ``flow-run-expiration-hours`` is used.
        log_level: Logging level for the Prefect flow.
        log_file: Path to the rotating log file. Defaults to ``maintenance-cleanup.log``.
            Pass ``None`` to disable file logging.

    Returns:
        True if any flow runs were deleted, False if no old flow runs were found.
    """
    setup_logging(level=log_level, log_file=log_file)
    hours = hours or float(
        await aget_variable(
            PrefectVariableName.FLOW_RUN_EXPIRATION_HOURS,
            default="672",
        ),
    )
    dt = datetime.timedelta(hours=hours)
    old_flow_run_ids = await retrieve_old_flow_ids(dt)
    if not old_flow_run_ids:
        logger.info("No flow runs found older than the specified cutoff.")
        return False

    delete_flow_run_id.map(old_flow_run_ids).result()
    logger.success("Deletion completed")
    return True
