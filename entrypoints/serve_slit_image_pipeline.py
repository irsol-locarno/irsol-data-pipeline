"""Serve the slit image generation Prefect deployments."""

from __future__ import annotations

import os
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

PREFECT_JSOC_EMAIL_SECRET_BLOCK = "jsoc-email"
PREFECT_JSOC_EMAIL_TEMPLATE = (
    "{{ prefect.blocks.secret." + PREFECT_JSOC_EMAIL_SECRET_BLOCK + " }}"
)


def _slit_image_job_variables() -> dict[str, dict[str, str]]:
    """Build deployment job variables for slit image flows."""
    return {
        "env": {
            "JSOC_EMAIL": os.environ.get(
                "JSOC_EMAIL",
                PREFECT_JSOC_EMAIL_TEMPLATE,
            )
        }
    }


def main():
    """Create and serve the slit image Prefect deployments."""

    root_path = Path(__file__).parent.parent
    job_variables = _slit_image_job_variables()

    generate_slit_images_deployment = (
        generate_slit_images.to_deployment(
            name="slit-images-full",
            parameters={"root": str(root_path / "data")},
            job_variables=job_variables,
            description="Generate slit preview images for all unprocessed measurements.",
            cron="0 4 * * *",  # Daily at 4am
            tags=[
                DeploymentTopicTag.SLIT_IMAGES.value,
                DeploymentScheduleTag.DAILY.value,
                DeploymentAutomationTag.SCHEDULED.value,
            ],
        ),
    )

    generate_daily_slit_images_deployment = generate_daily_slit_images.to_deployment(
        name="slit-images-daily",
        job_variables=job_variables,
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


if __name__ == "__main__":
    main()
