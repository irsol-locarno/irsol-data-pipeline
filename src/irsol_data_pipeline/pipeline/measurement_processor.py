"""Measurement processor — processes a single measurement file."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional

from loguru import logger

from irsol_data_pipeline.core.calibration.autocalibrate import calibrate_measurement
from irsol_data_pipeline.core.correction.corrector import apply_correction
from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MaxDeltaPolicy,
    Measurement,
    MeasurementMetadata,
    StokesParameters,
)
from irsol_data_pipeline.core.solar_orientation import (
    SolarOrientationInfo,
    compute_solar_orientation,
)
from irsol_data_pipeline.exceptions import FlatFieldAssociationNotFoundException
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.io import fits_flatfield as flatfield_io
from irsol_data_pipeline.io import processing_metadata as processing_metadata_io
from irsol_data_pipeline.io.fits.exporter import write_stokes_fits
from irsol_data_pipeline.pipeline.filesystem import (
    get_processed_stem,
    processed_output_path,
)
from irsol_data_pipeline.pipeline.flatfield_cache import FlatFieldCache
from irsol_data_pipeline.plotting import plot_profile
from irsol_data_pipeline.prefect.decorators import task
from irsol_data_pipeline.prefect.utils import create_prefect_json_report


def process_single_measurement(
    measurement_path: Path,
    processed_dir: Path,
    ff_cache: FlatFieldCache,
    max_delta_policy: Optional[MaxDeltaPolicy] = None,
) -> None:
    """Process a single measurement.

    Args:
        measurement_path: Path to the measurement .dat file.
        processed_dir: Output directory for processed files.
        ff_cache: Prebuilt flat-field correction cache.
        max_delta_policy: Policy for flat-field time thresholds.
    """
    max_delta_policy = max_delta_policy or MaxDeltaPolicy()
    _process_single_measurement(
        meas_path=measurement_path,
        processed_dir=processed_dir,
        ff_cache=ff_cache,
        max_delta_policy=max_delta_policy,
    )


def _plot_data(
    stokes: StokesParameters,
    metadata: MeasurementMetadata,
    solar_orientation: SolarOrientationInfo,
    calibration: CalibrationResult,
    filename_save: Path,
) -> None:
    wavelength_offset = calibration.wavelength_offset
    pixel_scale = calibration.pixel_scale
    logger.debug(
        "Plotting profile",
        output_path=str(filename_save),
        wavelength_offset=wavelength_offset,
        pixel_scale=pixel_scale,
    )
    plot_profile(
        stokes,
        filename_save=filename_save,
        a0=wavelength_offset,
        a1=pixel_scale,
        metadata=metadata,
        solar_orientation=solar_orientation,
    )


@task(
    task_run_name="ff-correction/process-measurement/{meas_path.name}",
)
def _process_single_measurement(
    meas_path: Path,
    processed_dir: Path,
    ff_cache: FlatFieldCache,
    max_delta_policy: MaxDeltaPolicy,
) -> None:
    """Internal: process one measurement.

    Raises on failure so the caller can handle error recording.
    """
    with logger.contextualize(file=meas_path.name):
        logger.info("Processing measurement")
        stem = get_processed_stem(meas_path.name)

        # 1. Load measurement
        stokes, info = dat_io.read(meas_path)
        measurement_metadata = MeasurementMetadata.from_info_array(info)
        measurement = Measurement(
            source_path=meas_path,
            metadata=measurement_metadata,
            stokes=stokes,
        )
        solar_orientation = compute_solar_orientation(measurement_metadata)

        with NamedTemporaryFile(suffix=".json") as f:
            with open(f.name, "w") as json_file:
                json.dump(measurement_metadata.model_dump(), json_file, default=str)
            create_prefect_json_report(
                path=Path(f.name), title="Measurement metadata", key=f"meas-{stem}"
            )

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
            raise FlatFieldAssociationNotFoundException(
                measurement=measurement, max_delta=max_delta
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
        logger.info("Applying flat-field correction")
        corrected_stokes = apply_correction(
            stokes=measurement.stokes,
            dust_flat=ff_correction.dust_flat,
            offset_map=ff_correction.offset_map,
        )

        logger.info("Flat-field correction applied")

        # 4. Wavelength auto-calibration
        calibration = calibrate_measurement(corrected_stokes)
        logger.info(
            "Wavelength calibration complete",
            pixel_scale=calibration.pixel_scale,
            wavelength_offset=calibration.wavelength_offset,
            reference_file=calibration.reference_file,
        )

        # 5. Save corrected data
        # TODO: regenerate info array with calibration results if needed.
        write_stokes_fits(
            processed_output_path(
                processed_dir,
                meas_path.name,
                kind="corrected_fits",
            ),
            corrected_stokes,
            measurement_metadata,
            calibration=calibration,
            solar_orientation=solar_orientation,
        )

        # 6. Save flat-field correction data (FITS)
        flatfield_io.write(
            processed_output_path(
                processed_dir,
                meas_path.name,
                kind="flatfield_correction_data",
            ),
            ff_correction,
        )

        # 7. Save processing metadata
        metadata_path = processed_output_path(
            processed_dir,
            meas_path.name,
            kind="metadata_json",
        )
        processing_metadata_io.write(
            metadata_path,
            source_file=meas_path.name,
            flat_field_timestamp=ff_correction.timestamp,
            measurement_timestamp=measurement.timestamp,
            flat_field_used=ff_correction.source_flatfield_path.name,
            flat_field_time_delta_seconds=ff_time_delta,
            calibration_info=calibration.model_dump(),
        )

        create_prefect_json_report(
            metadata_path,
            title="Metadata for processed measurement",
            key=f"processing-metadata-{stem}",
        )

        logger.info("Plotting profiles for original and corrected data")
        # 8. Generate profile plots for original and corrected data
        _plot_data(
            stokes=corrected_stokes,
            calibration=calibration,
            metadata=measurement_metadata,
            solar_orientation=solar_orientation,
            filename_save=processed_output_path(
                processed_dir,
                meas_path.name,
                kind="profile_corrected_png",
            ),
        )
        _plot_data(
            stokes=measurement.stokes,
            calibration=calibration,
            metadata=measurement_metadata,
            solar_orientation=solar_orientation,
            filename_save=processed_output_path(
                processed_dir,
                meas_path.name,
                kind="profile_original_png",
            ),
        )

        logger.success("Measurement processed")
