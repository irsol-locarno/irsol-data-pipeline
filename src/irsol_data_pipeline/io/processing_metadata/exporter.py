"""JSON metadata storage for processing results."""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

from loguru import logger

from irsol_data_pipeline.version import __version__ as pipeline_version


def write_processing_metadata(
    output_path: Path,
    source_file: str,
    flat_field_used: str,
    flat_field_timestamp: datetime.datetime,
    measurement_timestamp: datetime.datetime,
    flat_field_time_delta_seconds: float,
    flat_field_angle: float | None,
    measurement_angle: float | None,
    calibration_info: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> Path:
    """Write processing metadata as a JSON file.

    Args:
        output_path: Where to write the metadata JSON.
        source_file: Name of the source .dat file.
        flat_field_used: Name of the flat-field .dat file used.
        flat_field_timestamp: Timestamp of the flat-field observation.
        measurement_timestamp: Timestamp of the measurement observation.
        flat_field_time_delta_seconds: Time delta in seconds between
            measurement and flat-field.
        flat_field_angle: Position angle of the flat-field observation.
        measurement_angle: Position angle of the measurement observation.
        calibration_info: Wavelength calibration result dict.
        extra: Any additional metadata to include.

    Returns:
        Path to the written file.
    """
    with logger.contextualize(path=output_path):
        logger.debug("Writing processing metadata JSON")

        data: dict[str, Any] = {
            "source_file": source_file,
            "flat_field_used": flat_field_used,
            "flat_field_timestamp": flat_field_timestamp.isoformat(),
            "measurement_timestamp": measurement_timestamp.isoformat(),
            "flat_field_time_delta_seconds": flat_field_time_delta_seconds,
            "flat_field_angle": flat_field_angle,
            "measurement_angle": measurement_angle,
            "auto_calibrated_wavelength": calibration_info,
            "processing_timestamp": datetime.datetime.now(
                datetime.timezone.utc,
            ).isoformat(),
            "pipeline_version": pipeline_version,
        }
        if extra:
            data.update(extra)

        with output_path.open("w") as f:
            json.dump(data, f, indent=2, default=str)

        logger.debug("Processing metadata JSON written")
        return output_path


def write_error_metadata(
    output_path: Path,
    source_file: str,
    error: str,
) -> Path:
    """Write an error file when processing fails.

    Args:
        output_path: Where to write the error JSON.
        source_file: Name of the source .dat file.
        error: Human-readable error description.

    Returns:
        Path to the written file.
    """
    with logger.contextualize(path=output_path):
        logger.debug("Writing processing error JSON")

        data = {
            "source_file": source_file,
            "error": error,
            "processing_timestamp": datetime.datetime.now(
                datetime.timezone.utc,
            ).isoformat(),
            "pipeline_version": pipeline_version,
        }

        with output_path.open("w") as f:
            json.dump(data, f, indent=2)

        logger.debug("Processing error JSON written")
        return output_path
