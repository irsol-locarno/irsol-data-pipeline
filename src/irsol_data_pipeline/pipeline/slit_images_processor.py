"""Slit image generation processor.

Pipeline-level module that orchestrates slit preview generation for
individual measurements and entire observation days.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from loguru import logger

from irsol_data_pipeline.core.models import (
    DayProcessingResult,
    MeasurementMetadata,
    ObservationDay,
)
from irsol_data_pipeline.core.slit_images.coordinates import compute_slit_geometry
from irsol_data_pipeline.core.slit_images.solar_data import fetch_sdo_maps
from irsol_data_pipeline.exceptions import SlitImageGenerationError
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.io import processing_metadata as processing_metadata_io
from irsol_data_pipeline.orchestration.decorators import task
from irsol_data_pipeline.pipeline.filesystem import (
    discover_measurement_files,
    is_slit_preview_generated,
    processed_output_path,
    sdo_cache_dir_for_day,
)
from irsol_data_pipeline.plotting import plot_slit


@task(
    task_run_name="slit-images/generate-measurement/{measurement_path.name}",
    retries=2,
    retry_delay_seconds=30,
)
def generate_slit_image(
    measurement_path: Path,
    processed_dir: Path,
    jsoc_email: str,
    sdo_cache_dir: Optional[Path] = None,
    use_limbguider: bool = False,
    offset_corrections: tuple[float, float] = (0.0, 0.0),
    angle_correction: float = 0.0,
) -> None:
    """Generate a slit preview image for a single measurement.

    Args:
        measurement_path: Path to the measurement ``.dat`` file.
        processed_dir: Output directory for the slit preview image.
        jsoc_email: JSOC email for DRMS queries.
        sdo_cache_dir: Optional cache directory for SDO FITS files.
        use_limbguider: Whether to try using limbguider coordinates.
        offset_corrections: (x, y) corrections in arcsec.
        angle_correction: Derotator angle correction in degrees.

    Raises:
        SlitImageGenerationError: If the image cannot be generated.
    """

    with logger.contextualize(file=measurement_path.name):
        logger.info("Generating slit preview image")

        output_path = processed_output_path(
            processed_dir, measurement_path.name, kind="slit_preview_png"
        )
        error_path = processed_output_path(
            processed_dir, measurement_path.name, kind="slit_preview_error_json"
        )

        try:
            # 1. Load measurement metadata
            logger.info("Loading measurement metadata")
            stokes, info = dat_io.read(measurement_path)
            metadata = MeasurementMetadata.from_info_array(info)

            if metadata.solar_x is None or metadata.solar_y is None:
                raise SlitImageGenerationError(
                    f"No solar disc coordinates in measurement {measurement_path.name}"
                )

            # 2. Compute slit geometry
            logger.info("Computing slit geometry")
            slit_geometry = compute_slit_geometry(
                metadata=metadata,
                use_limbguider=use_limbguider,
                offset_corrections=offset_corrections,
                angle_correction=angle_correction,
            )

            # 3. Fetch SDO images
            logger.info("Fetching SDO data")
            maps = fetch_sdo_maps(
                start_time=slit_geometry.start_time,
                end_time=slit_geometry.end_time,
                jsoc_email=jsoc_email,
                cache_dir=sdo_cache_dir,
            )

            if all(m is None for _, m in maps):
                raise SlitImageGenerationError(
                    f"No SDO data available for measurement {measurement_path.name} "
                    f"at time {slit_geometry.start_time}"
                )

            # 4. Render the 6-panel image
            logger.info("Rendering slit preview image")
            processed_dir.mkdir(parents=True, exist_ok=True)
            plot_slit(
                maps=maps,
                slit=slit_geometry,
                output_path=output_path,
            )

            logger.success("Slit preview generated", output_path=output_path)

        except Exception as exc:
            logger.exception(
                "Slit preview generation failed",
                file=measurement_path.name,
            )
            processed_dir.mkdir(parents=True, exist_ok=True)
            processing_metadata_io.write_error(
                error_path,
                source_file=measurement_path.name,
                error=str(exc),
            )
            raise


def generate_slit_images_for_day(
    day: ObservationDay,
    jsoc_email: str,
    use_limbguider: bool = False,
) -> DayProcessingResult:
    """Generate slit preview images for all measurements in an observation day.

    Skips measurements that already have a slit preview or slit preview error
    file.

    Args:
        day: Observation day to process.
        jsoc_email: JSOC email for DRMS queries.
        use_limbguider: Whether to try using limbguider coordinates.

    Returns:
        DayProcessingResult summary.
    """

    with logger.contextualize(day=day.name):
        logger.info("Generating slit images for observation day")

        measurement_files = discover_measurement_files(day.reduced_dir)
        if not measurement_files:
            logger.info("No measurement files found")
            return DayProcessingResult(day_name=day.name)

        sdo_cache = sdo_cache_dir_for_day(day.path)

        processed = 0
        skipped = 0
        failed = 0
        errors: list[str] = []

        for measurement_path in measurement_files:
            if is_slit_preview_generated(day.processed_dir, measurement_path.name):
                logger.debug(
                    "Slit preview already exists, skipping", file=measurement_path.name
                )
                skipped += 1
                continue

            try:
                generate_slit_image(
                    measurement_path=measurement_path,
                    processed_dir=day.processed_dir,
                    jsoc_email=jsoc_email,
                    sdo_cache_dir=sdo_cache,
                    use_limbguider=use_limbguider,
                )
                processed += 1
            except Exception as exc:
                failed += 1
                errors.append(f"{measurement_path.name}: {exc}")
                logger.exception(
                    "Failed to generate slit image", file=measurement_path.name
                )

        logger.info(
            "Slit image generation complete",
            day=day.name,
            processed=processed,
            skipped=skipped,
            failed=failed,
        )
        return DayProcessingResult(
            day_name=day.name,
            processed=processed,
            skipped=skipped,
            failed=failed,
            errors=errors,
        )
