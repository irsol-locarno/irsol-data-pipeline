"""Serve the flat-field correction Prefect deployments."""

from __future__ import annotations


def main() -> None:
    """Register and serve the flat-field correction Prefect deployments.

    Sets ``PREFECT_ENABLED=true`` before importing pipeline modules so that
    the conditional Prefect decorators are active, then registers deployments
    and starts serving.  The default ``root`` parameter points to a ``data/``
    sub-directory of the current working directory and can be overridden at
    run time via the Prefect UI or API.
    """
    import os

    os.environ.setdefault("PREFECT_ENABLED", "true")

    from pathlib import Path

    from prefect import serve

    from irsol_data_pipeline.orchestration.flows.flat_field_correction import (
        process_daily_unprocessed_measurements,
        process_unprocessed_measurements,
    )
    from irsol_data_pipeline.orchestration.flows.tags import (
        DeploymentAutomationTag,
        DeploymentScheduleTag,
        DeploymentTopicTag,
    )

    root_path = Path.cwd()

    process_unprocessed_measurment_deployment = process_unprocessed_measurements.to_deployment(
        name="flat-field-correction-full",
        parameters={"root": str(root_path / "data")},
        description="Run the flat field correction pipeline on all unprocessed measurements.",
        cron="0 1 * * *",  # Daily at 1am
        tags=[
            DeploymentTopicTag.FLAT_FIELD_CORRECTION.value,
            DeploymentScheduleTag.DAILY.value,
            DeploymentAutomationTag.SCHEDULED.value,
        ],
    )

    process_daily_unprocessed_measurement_deployment = process_daily_unprocessed_measurements.to_deployment(
        name="flat-field-correction-daily",
        description="Run the flat field correction pipeline on a specific day folder.",
        tags=[
            DeploymentTopicTag.FLAT_FIELD_CORRECTION.value,
            DeploymentAutomationTag.MANUAL.value,
        ],
    )

    serve(
        process_unprocessed_measurment_deployment,
        process_daily_unprocessed_measurement_deployment,
    )
