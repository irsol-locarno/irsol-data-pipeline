"""Day processor — processes all measurements for a single observation day."""

from __future__ import annotations

from typing import Optional

from loguru import logger

from irsol_data_pipeline.core.models import (
    DayProcessingResult,
    MaxDeltaPolicy,
    ObservationDay,
)
from irsol_data_pipeline.io import processing_metadata as processing_metadata_io
from irsol_data_pipeline.pipeline.filesystem import (
    discover_flatfield_files,
    discover_measurement_files,
    get_processed_stem,
    is_measurement_flat_field_processed,
    processed_output_path,
)
from irsol_data_pipeline.pipeline.flatfield_cache import (
    build_flatfield_cache,
)
from irsol_data_pipeline.pipeline.measurement_processor import (
    process_single_measurement,
)
from irsol_data_pipeline.prefect.utils import (
    create_prefect_json_report,
)


def process_observation_day(
    day: ObservationDay,
    max_delta_policy: Optional[MaxDeltaPolicy] = None,
    force: bool = False,
) -> DayProcessingResult:
    """Process all measurements for a single observation day.

    Pipeline steps per measurement:
    1. Load measurement
    2. Find closest flat-field correction (by wavelength and time)
    3. Apply flat-field correction
    4. Run wavelength auto-calibration
    5. Save corrected data and metadata

    If any step fails for a measurement, an error file is written and
    processing continues with the next measurement.

    By default, measurements that have already been processed (i.e. a
    ``*_corrected.fits`` or ``*_error.json`` artifact exists in
    ``day.processed_dir``) are skipped.  Pass ``force=True`` to bypass
    this check and reprocess all measurements regardless of existing
    artifacts.

    Args:
        day: ObservationDay to process.
        max_delta_policy: Policy for flat-field time matching thresholds.
        force: When True, skip the "already processed" check and reprocess
            every measurement even if an output or error artifact already
            exists.

    Returns:
        DayProcessingResult summary.
    """
    max_delta_policy = max_delta_policy or MaxDeltaPolicy()

    with logger.contextualize(day=day.name, reduced_dir=day.reduced_dir):
        result = DayProcessingResult(day_name=day.name)

        # Ensure processed directory exists
        day.processed_dir.mkdir(parents=True, exist_ok=True)

        # Discover files
        measurement_paths = discover_measurement_files(day.reduced_dir)
        if not measurement_paths:
            logger.warning("No measurements found")
            return result

        flatfield_paths = discover_flatfield_files(day.reduced_dir)
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

        for meas_path in sorted(measurement_paths):
            with logger.contextualize(file=meas_path.name):
                if not force and is_measurement_flat_field_processed(
                    day.processed_dir, meas_path.name
                ):
                    logger.debug("Skipping already processed", file=meas_path.name)
                    result.skipped += 1
                    continue

                try:
                    process_single_measurement(
                        measurement_path=meas_path,
                        processed_dir=day.processed_dir,
                        ff_cache=ff_cache,
                        max_delta_policy=max_delta_policy,
                    )
                    result.processed += 1
                except Exception as e:
                    error_msg = f"{meas_path.name}: {e}"
                    logger.exception("Failed to process measurement")
                    result.failed += 1
                    result.errors.append(error_msg)

                    # Write error file
                    stem = get_processed_stem(meas_path.name)
                    error_path = processed_output_path(
                        day.processed_dir,
                        meas_path.name,
                        kind="error_json",
                    )
                    processing_metadata_io.write_error(
                        error_path,
                        source_file=meas_path.name,
                        error=str(e),
                    )
                    create_prefect_json_report(
                        error_path,
                        title=f"Error for failed processed measurement {stem}",
                        key=f"error-metadata-{meas_path.name}",
                    )

        return result
