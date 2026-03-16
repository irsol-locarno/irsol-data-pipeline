"""Measurement processor — processes a single measurement file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from loguru import logger

from irsol_data_pipeline.core.calibration.autocalibrate import calibrate_measurement
from irsol_data_pipeline.core.correction.corrector import apply_correction
from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MaxDeltaPolicy,
    StokesParameters,
)
from irsol_data_pipeline.io.dat_reader import load_measurement, read_zimpol_dat
from irsol_data_pipeline.io.dat_writer import save_correction_data
from irsol_data_pipeline.io.filesystem import get_processed_stem, processed_output_path
from irsol_data_pipeline.io.fits.exporter import write_stokes_fits
from irsol_data_pipeline.io.metadata_store import write_processing_metadata
from irsol_data_pipeline.orchestration.decorators import prefect_enabled, task
from irsol_data_pipeline.orchestration.utils import sanitize_artifact_title
from irsol_data_pipeline.pipeline.flatfield_cache import FlatFieldCache
from irsol_data_pipeline.plotting import plot_profile


def process_single_measurement(
    measurement_path: Path,
    processed_dir: Path,
    ff_cache: FlatFieldCache,
    max_delta_policy: Optional[MaxDeltaPolicy] = None,
    refdata_dir: Optional[Path] = None,
) -> None:
    """Process a single measurement.

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
) -> None:
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
    logger.info("Applying flat-field correction", file=meas_path.name)
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
    # TODO: regenerate info array with calibration results if needed.
    write_stokes_fits(
        processed_output_path(
            processed_dir,
            meas_path.name,
            kind="corrected_fits",
        ),
        corrected_stokes,
        info_raw,
        calibration=calibration,
    )

    # 6. Save flat-field correction data (pickle)
    save_correction_data(
        processed_output_path(
            processed_dir,
            meas_path.name,
            kind="flatfield_correction_data",
        ),
        {
            "dust_flat": ff_correction.dust_flat,
            "offset_map": ff_correction.offset_map,
            "desmiled": ff_correction.desmiled,
            "source_flatfield": ff_correction.source_flatfield_path.name,
        },
    )

    # 7. Save processing metadata
    metadata_path = processed_output_path(
        processed_dir,
        meas_path.name,
        kind="metadata_json",
    )
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
        from prefect.artifacts import create_table_artifact

        with open(metadata_path, "r", encoding="utf-8") as f:
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
    # 8. Generate profile plots for original and corrected data
    _plot_data(
        stokes=corrected_stokes,
        calibration=calibration,
        title=f"{stem} - Corrected",
        filename_save=processed_output_path(
            processed_dir,
            meas_path.name,
            kind="profile_corrected_png",
        ),
    )
    _plot_data(
        stokes=measurement.stokes,
        calibration=calibration,
        title=f"{stem} - Original",
        filename_save=processed_output_path(
            processed_dir,
            meas_path.name,
            kind="profile_original_png",
        ),
    )

    logger.success("Measurement processed", file=meas_path.name)
