from __future__ import annotations

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


def main():
    root_path = Path(__file__).parent.parent

    delete_old_prefect_data_deployment = delete_flow_runs_older_than.to_deployment(
        name="prefect-run-cleanup",
        parameters={"hours": 24 * 7 * 4, "interactive": False},
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
        parameters={"root": str(root_path / "data"), "hours": 24 * 7 * 4},
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


if __name__ == "__main__":
    main()
