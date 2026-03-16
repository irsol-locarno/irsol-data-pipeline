"""Day processor — processes all measurements for a single observation day."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from loguru import logger

from irsol_data_pipeline.core.models import (
    DayProcessingResult,
    MaxDeltaPolicy,
    ObservationDay,
)
from irsol_data_pipeline.io.filesystem import (
    discover_flatfield_files,
    discover_measurement_files,
    get_processed_stem,
    is_measurement_processed,
    processed_output_path,
)
from irsol_data_pipeline.io.metadata_store import (
    write_error_metadata,
)
from irsol_data_pipeline.orchestration.decorators import prefect_enabled
from irsol_data_pipeline.orchestration.utils import (
    sanitize_artifact_title,
)
from irsol_data_pipeline.pipeline.flatfield_cache import (
    build_flatfield_cache,
)
from irsol_data_pipeline.pipeline.measurement_processor import (
    process_single_measurement,
)


def process_observation_day(
    day: ObservationDay,
    max_delta_policy: Optional[MaxDeltaPolicy] = None,
    refdata_dir: Optional[Path] = None,
) -> DayProcessingResult:
    """Process all unprocessed measurements for a single observation day.

    Pipeline steps per measurement:
    1. Load measurement
    2. Find closest flat-field correction (by wavelength and time)
    3. Apply flat-field correction
    4. Run wavelength auto-calibration
    5. Save corrected data and metadata

    If any step fails for a measurement, an error file is written and
    processing continues with the next measurement.

    Args:
        day: ObservationDay to process.
        max_delta_policy: Policy for flat-field time matching thresholds.
        refdata_dir: Directory with wavelength calibration reference data.

    Returns:
        DayProcessingResult summary.
    """
    if max_delta_policy is None:
        max_delta_policy = MaxDeltaPolicy()

    result = DayProcessingResult(
        day_name=day.name,
        total_measurements=0,
        processed=0,
        skipped=0,
        failed=0,
    )

    # Ensure processed directory exists
    day.processed_dir.mkdir(parents=True, exist_ok=True)

    # Discover files
    measurement_paths = discover_measurement_files(day.reduced_dir)
    flatfield_paths = discover_flatfield_files(day.reduced_dir)
    result.total_measurements = len(measurement_paths)

    if not measurement_paths:
        logger.info("No measurements found", reduced_dir=str(day.reduced_dir))
        return result

    # Build flat-field cache (analyzed once, reused for all measurements)
    logger.info(
        "Building flat-field cache",
        flatfield_count=len(flatfield_paths),
    )
    ff_cache = build_flatfield_cache(
        flatfield_paths=flatfield_paths,
        max_delta=max_delta_policy.default_max_delta,
    )
    logger.info(
        "Flat-field cache ready",
        corrections=len(ff_cache),
        wavelengths=ff_cache.wavelengths,
    )

    if prefect_enabled():
        from prefect.artifacts import create_progress_artifact, update_progress_artifact

        progress_id = create_progress_artifact(
            0.0,
            key=sanitize_artifact_title(f"progress-{day.name}"),
            description=f"Processing progress for {day.name}",
        )

        def update_progress(processed: int):
            percent = (processed + 1) / len(measurement_paths) * 100
            update_progress_artifact(artifact_id=progress_id, progress=percent)
    else:

        def update_progress(processed: int):
            pass  # No-op if not using Prefect

    for meas_i, meas_path in enumerate(sorted(measurement_paths)):
        update_progress(meas_i)
        if is_measurement_processed(day.processed_dir, meas_path.name):
            logger.debug("Skipping already processed", file=meas_path.name)
            result.skipped += 1
            continue

        try:
            process_single_measurement(
                measurement_path=meas_path,
                processed_dir=day.processed_dir,
                ff_cache=ff_cache,
                max_delta_policy=max_delta_policy,
                refdata_dir=refdata_dir,
            )
            result.processed += 1
        except Exception as e:
            error_msg = f"{meas_path.name}: {e}"
            logger.exception("Failed to process measurement", file=meas_path.name)
            result.failed += 1
            result.errors.append(error_msg)

            # Write error file
            stem = get_processed_stem(meas_path.name)
            error_path = processed_output_path(
                day.processed_dir,
                meas_path.name,
                kind="error_json",
            )
            write_error_metadata(
                error_path,
                source_file=meas_path.name,
                error=str(e),
            )
            if prefect_enabled():
                from prefect.artifacts import create_table_artifact

                with open(error_path, "r", encoding="utf-8") as f:
                    error_content = json.load(f)

                table_rows = []
                for k, v in error_content.items():
                    if isinstance(v, dict):
                        for kk, vv in v.items():
                            table_rows.append({"key": f"{k}.{kk}", "value": str(vv)})
                    else:
                        table_rows.append({"key": k, "value": str(v)})
                create_table_artifact(
                    table=table_rows,
                    key=sanitize_artifact_title(f"error-metadata-{meas_path.name}"),
                    description=f"Error for failed processed measurement {stem}",
                )

    return result
