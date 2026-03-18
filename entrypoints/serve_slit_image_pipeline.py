"""Serve the slit image generation Prefect deployments."""

from pathlib import Path

from prefect import serve

from irsol_data_pipeline.orchestration.flows.slit_image_generation import (
    generate_daily_slit_images,
    generate_slit_images,
)


def main():

    root_path = Path(__file__).parent.parent

    generate_slit_images_deployment = generate_slit_images.to_deployment(
        name="run-slit-image-pipeline",
        parameters={"root": str(root_path / "data")},
        description="Generate slit preview images for all unprocessed measurements.",
        cron="0 4 * * *",  # Daily at 4am
        tags=["slit-images", "top-level-pipeline"],
    )

    generate_daily_slit_images_deployment = generate_daily_slit_images.to_deployment(
        name="run-daily-slit-image-pipeline",
        description="Generate slit preview images for a specific observation day.",
        tags=["slit-images"],
    )

    serve(
        generate_slit_images_deployment,
        generate_daily_slit_images_deployment,
    )


if __name__ == "__main__":
    main()
