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


def main():

    root_path = Path(__file__).parent.parent

    process_unprocessed_measurment_deployment = process_unprocessed_measurements.to_deployment(
        name="flat-field-correction/full",
        parameters={"root": str(root_path / "data")},
        description="Run the flat field correction pipeline on all unprocessed measurements.",
        cron="0 1 * * *",  # Daily at 1am
        tags=[
            DeploymentTopicTag.FLAT_FIELD_CORRECTION,
            DeploymentScheduleTag.DAILY,
            DeploymentAutomationTag.SCHEDULED,
        ],
    )

    process_daily_unprocessed_measurement_deployment = process_daily_unprocessed_measurements.to_deployment(
        name="flat-field-correction/daily",
        description="Run the flat field correction pipeline on a specific day folder.",
        tags=[
            DeploymentTopicTag.FLAT_FIELD_CORRECTION,
            DeploymentAutomationTag.MANUAL,
        ],
    )

    serve(
        process_unprocessed_measurment_deployment,
        process_daily_unprocessed_measurement_deployment,
    )


if __name__ == "__main__":
    main()
