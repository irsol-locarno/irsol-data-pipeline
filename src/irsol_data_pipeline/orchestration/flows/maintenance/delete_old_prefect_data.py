"""Maintenance flow for cleaning old Prefect flow-run history.

This module defines an async Prefect flow that removes flow runs older
than a user-provided retention window. It is intended to be served as a
dedicated maintenance deployment, separate from science-processing
deployments.
"""

import asyncio
import datetime
from uuid import UUID

import typer
from loguru import logger
from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.server.schemas.filters import FlowRunFilter, FlowRunFilterEndTime
from prefect.server.schemas.sorting import FlowRunSort
from prefect.task_runners import ThreadPoolTaskRunner

from irsol_data_pipeline.orchestration.patch_logging import setup_logging

app = typer.Typer()


@task(task_run_name="maintenance/delete-run/{flow_run_id}")
async def delete_flow_run_id(flow_run_id: UUID) -> UUID:
    """Delete a Prefect flow run by its ID."""
    async with get_client() as client:
        logger.info("Deleting flow run", flow_run_id=flow_run_id)
        await client.delete_flow_run(flow_run_id=flow_run_id)
    return flow_run_id


@task(task_run_name="maintenance/retrieve-old-runs")
async def retrieve_old_flow_ids(dt: datetime.timedelta) -> list[UUID]:
    """Return IDs of flow runs that ended before the provided cutoff.

    Args:
        dt: Retention duration. Runs older than `now - dt` are selected.

    Returns:
        List of flow-run UUIDs matching the filter.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    cutoff = now - dt
    logger.info(
        "Retrieving all flows finished before cutoff time", cutoff=cutoff.isoformat()
    )
    async with get_client() as client:
        old_flow_runs = await client.read_flow_runs(
            sort=FlowRunSort.START_TIME_ASC,
            flow_run_filter=FlowRunFilter(
                end_time=FlowRunFilterEndTime(before_=cutoff)
            ),
        )
    return [fr.id for fr in old_flow_runs]


@flow(
    name="maintenance-cleanup",
    task_runner=ThreadPoolTaskRunner(max_workers=4),
    flow_run_name="maintenance/cleanup/{hours}h",
)
async def delete_flow_runs_older_than(hours: float, interactive: bool = True) -> bool:
    """Delete Prefect flow runs older than a retention duration.

    Args:
        hours: Retention duration in hours. Runs older than `now - hours` are deleted.
        interactive: If True, show IDs and require confirmation before delete.
    """
    setup_logging()
    dt = datetime.timedelta(hours=hours)
    old_flow_run_ids = await retrieve_old_flow_ids(dt)
    if not old_flow_run_ids:
        typer.echo("No flow runs found older than the specified cutoff.")
        return False
    if interactive:
        typer.echo(
            f"Found {len(old_flow_run_ids)} flow run(s) older than {dt} to delete:"
        )
        for fid in old_flow_run_ids:
            typer.echo(f"  - {fid}")
        typer.confirm(
            f"\nDelete these {len(old_flow_run_ids)} flow run(s)?", abort=True
        )
    delete_flow_run_id.map(old_flow_run_ids).result()
    typer.echo("Deletion completed")
    return True


@app.command()
def main(
    hours: float = typer.Option(
        24 * 7 * 4,
        "--hours",
        help="Delete flow runs older than this many hours. Defaults to 4 weeks.",
    ),
    no_interactive: bool = typer.Option(
        False,
        "--no-interactive",
        help="Skip confirmation prompt and delete immediately.",
    ),
):
    """CLI entrypoint to run maintenance cleanup once.

    Args:
        hours: Retention window in hours.
        no_interactive: Disable confirmation prompt when set.
    """
    result = asyncio.run(
        delete_flow_runs_older_than(hours, interactive=not no_interactive)
    )
    if not result:
        raise typer.Exit()


if __name__ == "__main__":
    app()
