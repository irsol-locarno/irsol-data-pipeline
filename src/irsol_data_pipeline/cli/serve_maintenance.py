"""Serve the maintenance Prefect deployments."""

from __future__ import annotations


def main() -> None:
    """Register and serve the maintenance Prefect deployments.

    Sets ``PREFECT_ENABLED=true`` before importing pipeline modules so that
    the conditional Prefect decorators are active, then registers deployments
    and starts serving.  The default ``root`` parameter for cache-cleanup
    points to a ``data/`` sub-directory of the current working directory and
    can be overridden at run time via the Prefect UI or API.
    """
    import os

    os.environ.setdefault("PREFECT_ENABLED", "true")

    from pathlib import Path

    from prefect import serve

    from irsol_data_pipeline.orchestration.flows.maintenance.delete_old_cache_files import (
        delete_old_cache_files,
    )
    from irsol_data_pipeline.orchestration.flows.maintenance.delete_old_prefect_data import (
        delete_flow_runs_older_than,
    )
    from irsol_data_pipeline.orchestration.flows.tags import (
        DeploymentAutomationTag,
        DeploymentScheduleTag,
        DeploymentTopicTag,
    )

    root_path = Path.cwd()

    delete_old_prefect_data_deployment = delete_flow_runs_older_than.to_deployment(
        name="prefect-run-cleanup",
        description="Delete Prefect flow runs older than a retention duration.",
        cron="0 0 * * *",  # Daily at midnight
        tags=[
            DeploymentTopicTag.MAINTENANCE.value,
            DeploymentScheduleTag.DAILY.value,
            DeploymentAutomationTag.SCHEDULED.value,
        ],
    )

    delete_old_cache_files_deployment = delete_old_cache_files.to_deployment(
        name="cache-cleanup",
        parameters={"root": str(root_path / "data")},
        description=(
            "Delete stale .pkl cache files under processed/_cache and "
            "processed/_sdo_cache."
        ),
        cron="30 0 * * *",  # Daily at 00:30
        tags=[
            DeploymentTopicTag.MAINTENANCE.value,
            DeploymentScheduleTag.DAILY.value,
            DeploymentAutomationTag.SCHEDULED.value,
        ],
    )

    serve(delete_old_prefect_data_deployment, delete_old_cache_files_deployment)
