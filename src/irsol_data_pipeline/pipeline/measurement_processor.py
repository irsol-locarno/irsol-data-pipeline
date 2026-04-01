"""Measurement processor — processes a single measurement file."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import NamedTemporaryFile

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
from irsol_data_pipeline.io import fits as fits_io
from irsol_data_pipeline.io import fits_flatfield as flatfield_io
from irsol_data_pipeline.io import processing_metadata as processing_metadata_io
from irsol_data_pipeline.io.fits.constants import FITS_KEY_FFCORR, FITS_KEY_FFFILE
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
    max_delta_policy: MaxDeltaPolicy | None = None,
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

        processing_history = fits_io.ProcessingHistory()

        # 1. Load measurement
        stokes, metadata = dat_io.read(meas_path)
        measurement = Measurement(
            source_path=meas_path,
            metadata=metadata,
            stokes=stokes,
        )
        solar_orientation = compute_solar_orientation(metadata)

        with NamedTemporaryFile(suffix=".json") as f:
            with Path(f.name).open("w") as json_file:
                json.dump(metadata.model_dump(), json_file, default=str)
            create_prefect_json_report(
                path=Path(f.name),
                title="Measurement metadata",
                key=f"meas-{stem}",
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
            position_angle=measurement.metadata.derotator.position_angle,
        )

        if ff_correction is None:
            raise FlatFieldAssociationNotFoundException(
                measurement=measurement,
                max_delta=max_delta,
            )
        ff_time_delta = abs(
            (ff_correction.timestamp - measurement.timestamp).total_seconds(),
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

        processing_history.record(
            "Flat-field correction",
            details=(
                f"Associated flat-field: {ff_correction.source_flatfield_path.name} "
                f"Delta Time: {abs((ff_correction.timestamp - measurement.timestamp).total_seconds())} seconds"
            ),
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

        processing_history.record(
            "Wavelength auto-calibration",
            details=f"Reference file used: {calibration.reference_file}",
        )

        # 5. Save corrected data
        fits_io.write(
            processed_output_path(
                processed_dir,
                meas_path.name,
                kind="corrected_fits",
            ),
            corrected_stokes,
            metadata,
            calibration=calibration,
            solar_orientation=solar_orientation,
            extra_header={
                **processing_history.to_fits_header_entries(),
                FITS_KEY_FFCORR: (
                    True,
                    "True if pipeline applied flat-field correction",
                ),
                FITS_KEY_FFFILE: (
                    ff_correction.source_flatfield_path.name,
                    "Flat-field file used by pipeline",
                ),
            },
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
            metadata=metadata,
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
            metadata=metadata,
            solar_orientation=solar_orientation,
            filename_save=processed_output_path(
                processed_dir,
                meas_path.name,
                kind="profile_original_png",
            ),
        )

        logger.success("Measurement processed")


def plot_original_profile(
    measurement_path: Path,
    processed_dir: Path,
) -> None:
    """Generate a profile plot for the original (uncorrected) stokes data.

    Intended for use when flat-field correction fails so that downstream
    consumers (e.g. the web-asset pipeline) still have a quick-look plot
    even for non-successful measurements.  Loads the raw ``.dat`` file,
    runs a best-effort wavelength auto-calibration on the uncorrected Stokes
    data, and writes a ``*_profile_original.png`` output.

    Args:
        measurement_path: Path to the measurement ``.dat`` file.
        processed_dir: Output directory where the profile plot is written.
    """
    with logger.contextualize(file=measurement_path.name):
        logger.info("Generating original profile plot for failed measurement")

        stokes, metadata = dat_io.read(measurement_path)
        solar_orientation = compute_solar_orientation(metadata)

        calibration = calibrate_measurement(stokes)
        logger.info(
            "Wavelength calibration complete (best-effort on uncorrected data)",
            pixel_scale=calibration.pixel_scale,
            wavelength_offset=calibration.wavelength_offset,
        )

        processed_dir.mkdir(parents=True, exist_ok=True)

        _plot_data(
            stokes=stokes,
            calibration=calibration,
            metadata=metadata,
            solar_orientation=solar_orientation,
            filename_save=processed_output_path(
                processed_dir,
                measurement_path.name,
                kind="profile_original_png",
            ),
        )

        logger.success("Original profile plot generated")


def convert_measurement_to_fits(
    measurement_path: Path,
    processed_dir: Path,
) -> None:
    """Convert a measurement to FITS without applying flat-field correction.

    This function is intended for measurements where flat-field correction
    has failed or is unavailable.  It loads the raw ``.dat`` file, runs a
    best-effort wavelength auto-calibration on the uncorrected Stokes data,
    and writes the result as a ``*_converted.fits`` file together with a
    ``*_profile_converted.png`` profile plot.

    The generated FITS file includes the ``FFCORR = False`` header keyword so
    downstream consumers can clearly identify that no flat-field correction was
    applied.  The filename suffix (``_converted`` rather than ``_corrected``)
    also serves as an explicit visual distinction.

    Args:
        measurement_path: Path to the measurement ``.dat`` file.
        processed_dir: Output directory where converted artifacts are written.
    """
    with logger.contextualize(file=measurement_path.name):
        logger.info("Converting measurement to FITS without flat-field correction")

        # Load raw measurement
        stokes, metadata = dat_io.read(measurement_path)
        solar_orientation = compute_solar_orientation(metadata)

        # Best-effort wavelength calibration on uncorrected data
        calibration = calibrate_measurement(stokes)
        logger.info(
            "Wavelength calibration complete (best-effort on uncorrected data)",
            pixel_scale=calibration.pixel_scale,
            wavelength_offset=calibration.wavelength_offset,
        )

        processing_dir = processed_dir
        processing_dir.mkdir(parents=True, exist_ok=True)

        # Write converted FITS (no flat-field correction applied)
        fits_io.write(
            processed_output_path(
                processing_dir,
                measurement_path.name,
                kind="converted_fits",
            ),
            stokes,
            metadata,
            calibration=calibration,
            solar_orientation=solar_orientation,
            extra_header={
                FITS_KEY_FFCORR: (
                    False,
                    "True if pipeline applied flat-field correction",
                ),
            },
        )

        # Generate profile plot for the converted (uncorrected) data
        _plot_data(
            stokes=stokes,
            calibration=calibration,
            metadata=metadata,
            solar_orientation=solar_orientation,
            filename_save=processed_output_path(
                processing_dir,
                measurement_path.name,
                kind="profile_converted_png",
            ),
        )

        logger.success("Measurement converted to FITS without flat-field correction")
