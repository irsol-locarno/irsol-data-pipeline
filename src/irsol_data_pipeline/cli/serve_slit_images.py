"""Serve the slit-image generation Prefect deployments."""

from __future__ import annotations


def main() -> None:
    """Register and serve the slit-image generation Prefect deployments.

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

    from irsol_data_pipeline.orchestration.flows.slit_image_generation import (
        generate_daily_slit_images,
        generate_slit_images,
    )
    from irsol_data_pipeline.orchestration.flows.tags import (
        DeploymentAutomationTag,
        DeploymentScheduleTag,
        DeploymentTopicTag,
    )

    root_path = Path.cwd()

    generate_slit_images_deployment = generate_slit_images.to_deployment(
        name="slit-images-full",
        parameters={"root": str(root_path / "data")},
        description="Generate slit preview images for all unprocessed measurements.",
        cron="0 4 * * *",  # Daily at 4am
        tags=[
            DeploymentTopicTag.SLIT_IMAGES.value,
            DeploymentScheduleTag.DAILY.value,
            DeploymentAutomationTag.SCHEDULED.value,
        ],
    )

    generate_daily_slit_images_deployment = generate_daily_slit_images.to_deployment(
        name="slit-images-daily",
        description="Generate slit preview images for a specific observation day.",
        tags=[
            DeploymentTopicTag.SLIT_IMAGES.value,
            DeploymentAutomationTag.MANUAL.value,
        ],
    )

    serve(
        generate_slit_images_deployment,
        generate_daily_slit_images_deployment,
    )
