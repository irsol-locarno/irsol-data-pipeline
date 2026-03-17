from prefect import serve

from irsol_data_pipeline.orchestration.flows.delete_old_prefect_data import (
    delete_flow_runs_older_than,
)


def main():

    delete_old_prefect_data_deployment = delete_flow_runs_older_than.to_deployment(
        name="delete-old-prefect-data",
        parameters={"interactive": False},
        description="Delete Prefect flow runs older than a retention duration.",
        cron="0 0 * * *",  # Daily at midnight
        tags=["maintenance"],
    )

    serve(delete_old_prefect_data_deployment)


if __name__ == "__main__":
    main()
