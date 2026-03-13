from irsol_data_pipeline.orchestration.flows import process_unprocessed_measurements
from pathlib import Path


def main():

    root_path = Path(__file__).parent.parent
    # Example usage: run the dataset scan flow
    process_unprocessed_measurements.serve(
        name="run-process-unprocessed-measurements",
        parameters={"root": str(root_path / "data")},
    )


if __name__ == "__main__":
    main()
