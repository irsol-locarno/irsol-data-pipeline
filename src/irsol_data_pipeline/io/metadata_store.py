"""JSON metadata storage for processing results."""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any, Union

import irsol_data_pipeline


def write_processing_metadata(
    output_path: Union[Path, str],
    source_file: str,
    flat_field_used: str,
    flat_field_timestamp: datetime.datetime,
    measurement_timestamp: datetime.datetime,
    flat_field_time_delta_seconds: float,
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
        calibration_info: Wavelength calibration result dict.
        extra: Any additional metadata to include.

    Returns:
        Path to the written file.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data: dict[str, Any] = {
        "source_file": source_file,
        "flat_field_used": flat_field_used,
        "flat_field_timestamp": flat_field_timestamp.isoformat(),
        "measurement_timestamp": measurement_timestamp.isoformat(),
        "flat_field_time_delta_seconds": flat_field_time_delta_seconds,
        "auto_calibrated_wavelength": calibration_info,
        "processing_timestamp": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "pipeline_version": irsol_data_pipeline.__version__,
    }
    if extra:
        data.update(extra)

    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

    return path


def write_error_metadata(
    output_path: Union[Path, str],
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
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "source_file": source_file,
        "error": error,
        "processing_timestamp": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "pipeline_version": irsol_data_pipeline.__version__,
    }

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    return path


def read_metadata(path: Union[Path, str]) -> dict[str, Any]:
    """Read a metadata or error JSON file.

    Args:
        path: Path to the JSON file.

    Returns:
        Parsed dict.
    """
    with open(Path(path)) as f:
        return json.load(f)
