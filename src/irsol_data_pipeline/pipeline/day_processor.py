"""Day processor — processes all measurements for a single observation day."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from loguru import logger

from irsol_data_pipeline.core.calibration.autocalibrate import calibrate_measurement
from irsol_data_pipeline.core.correction.corrector import apply_correction
from irsol_data_pipeline.core.models import (
    CalibrationResult,
    DayProcessingResult,
    MaxDeltaPolicy,
    ObservationDay,
    StokesParameters,
)
from irsol_data_pipeline.io.dat_reader import load_measurement, read_zimpol_dat
from irsol_data_pipeline.io.dat_writer import save_correction_data, write_corrected_dat
from irsol_data_pipeline.io.filesystem import (
    discover_flatfield_files,
    discover_measurement_files,
    get_processed_stem,
    is_measurement_processed,
)
from irsol_data_pipeline.io.metadata_store import (
    write_error_metadata,
    write_processing_metadata,
)
from irsol_data_pipeline.orchestration.decorators import prefect_enabled, task
from irsol_data_pipeline.pipeline.flatfield_cache import (
    FlatFieldCache,
    build_flatfield_cache,
)
from irsol_data_pipeline.plotting import plot_profile


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

        from irsol_data_pipeline.orchestration.utils import sanitize_artifact_title

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
            _process_single_measurement(
                meas_path=meas_path,
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
            error_path = day.processed_dir / f"{stem}_error.json"
            write_error_metadata(
                error_path,
                source_file=meas_path.name,
                error=str(e),
            )
            if prefect_enabled():
                import json

                from prefect.artifacts import create_table_artifact

                from irsol_data_pipeline.orchestration.utils import (
                    sanitize_artifact_title,
                )

                with open(error_path) as f:
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


def process_single_measurement(
    measurement_path: Path,
    processed_dir: Path,
    ff_cache: FlatFieldCache,
    max_delta_policy: Optional[MaxDeltaPolicy] = None,
    refdata_dir: Optional[Path] = None,
) -> None:
    """Process a single measurement (public API).

    Args:
        measurement_path: Path to the measurement .dat file.
        processed_dir: Output directory for processed files.
        ff_cache: Prebuilt flat-field correction cache.
        max_delta_policy: Policy for flat-field time thresholds.
        refdata_dir: Directory with calibration reference data.
    """
    if max_delta_policy is None:
        max_delta_policy = MaxDeltaPolicy()
    processed_dir.mkdir(parents=True, exist_ok=True)

    _process_single_measurement(
        meas_path=measurement_path,
        processed_dir=processed_dir,
        ff_cache=ff_cache,
        max_delta_policy=max_delta_policy,
        refdata_dir=refdata_dir,
    )


def _plot_data(
    stokes: StokesParameters,
    calibration: CalibrationResult,
    title: str,
    filename_save: Path,
):

    wavelength_offset = calibration.wavelength_offset
    pixel_scale = calibration.pixel_scale
    logger.debug(
        "Plotting profile",
        title=title,
        output_path=str(filename_save),
        wavelength_offset=wavelength_offset,
        pixel_scale=pixel_scale,
    )
    plot_profile(
        stokes,
        title=title,
        filename_save=filename_save,
        a0=wavelength_offset,
        a1=pixel_scale,
    )


@task(task_run_name="process-measurement/{meas_path.name}")
def _process_single_measurement(
    meas_path: Path,
    processed_dir: Path,
    ff_cache: FlatFieldCache,
    max_delta_policy: MaxDeltaPolicy,
    refdata_dir: Optional[Path],
) -> None:
    """Internal: process one measurement.

    Raises on failure so the caller can handle error recording.
    """
    logger.info("Processing measurement", file=meas_path.name)
    stem = get_processed_stem(meas_path.name)

    # 1. Load measurement
    measurement = load_measurement(meas_path)

    # 2. Find closest flat-field
    max_delta = max_delta_policy.get_max_delta(
        wavelength=measurement.wavelength,
        instrument=measurement.metadata.instrument,
        telescope=measurement.metadata.telescope_name,
    )

    ff_correction = ff_cache.find_best_correction(
        wavelength=measurement.wavelength,
        timestamp=measurement.timestamp,
        max_delta=max_delta,
    )

    if ff_correction is None:
        raise RuntimeError(
            f"No flat-field within {max_delta} for wavelength "
            f"{measurement.wavelength} at {measurement.timestamp}"
        )

    ff_time_delta = abs(
        (ff_correction.timestamp - measurement.timestamp).total_seconds()
    )
    logger.info(
        "Using flat-field correction",
        flat_field=ff_correction.source_flatfield_path.name,
        delta_seconds=ff_time_delta,
    )

    # 3. Apply flat-field correction
    corrected_stokes = apply_correction(
        stokes=measurement.stokes,
        dust_flat=ff_correction.dust_flat,
        offset_map=ff_correction.offset_map,
    )

    logger.info("Flat-field correction applied", file=meas_path.name)
    logger.info(
        "Running calibration with reference data",
        refdata_dir=str(refdata_dir) if refdata_dir else "None",
    )

    # 4. Wavelength auto-calibration
    calibration = calibrate_measurement(corrected_stokes, refdata_dir=refdata_dir)
    logger.info(
        "Wavelength calibration complete",
        pixel_scale=calibration.pixel_scale,
        wavelength_offset=calibration.wavelength_offset,
        reference_file=calibration.reference_file,
    )

    # 5. Save corrected data
    _, info_raw = read_zimpol_dat(meas_path)
    #  TODO: rengen info array with new calibration results if needed
    #  (e.g. update wavelength info, add calibration metadata, etc.)
    write_corrected_dat(
        processed_dir / f"{stem}_corrected.dat",
        corrected_stokes,
        info_raw,
    )

    # 6. Save flat-field correction data (pickle)
    save_correction_data(
        processed_dir / f"{stem}_flat_field_correction_data.pkl",
        {
            "dust_flat": ff_correction.dust_flat,
            "offset_map": ff_correction.offset_map,
            "desmiled": ff_correction.desmiled,
            "source_flatfield": ff_correction.source_flatfield_path.name,
        },
    )

    # 7. Save processing metadata
    metadata_path = processed_dir / f"{stem}_metadata.json"
    write_processing_metadata(
        metadata_path,
        source_file=meas_path.name,
        flat_field_timestamp=ff_correction.timestamp,
        measurement_timestamp=measurement.timestamp,
        flat_field_used=ff_correction.source_flatfield_path.name,
        flat_field_time_delta_seconds=ff_time_delta,
        calibration_info=calibration.model_dump(),
    )
    if prefect_enabled():
        import json

        from prefect.artifacts import create_table_artifact

        from irsol_data_pipeline.orchestration.utils import sanitize_artifact_title

        with open(metadata_path) as f:
            metadata_content = json.load(f)

        table_rows = []
        for k, v in metadata_content.items():
            if isinstance(v, dict):
                for kk, vv in v.items():
                    table_rows.append({"key": f"{k}.{kk}", "value": str(vv)})
            else:
                table_rows.append({"key": k, "value": str(v)})
        create_table_artifact(
            table=table_rows,
            key=sanitize_artifact_title(f"processing-metadata-{meas_path.name}"),
            description=f"Metadata for processed measurement {stem}",
        )

    logger.info(
        "Plotting profiles for original and corrected data", file=meas_path.name
    )
    # 8. Generate profile plots for the original and corrected data
    _plot_data(
        stokes=corrected_stokes,
        calibration=calibration,
        title=f"{stem} - Corrected",
        filename_save=processed_dir / f"{stem}_profile_corrected.png",
    )
    _plot_data(
        stokes=measurement.stokes,
        calibration=calibration,
        title=f"{stem} - Original",
        filename_save=processed_dir / f"{stem}_profile_original.png",
    )

    logger.success("Measurement processed", file=meas_path.name)
