"""Entrypoint to process a single measurement .dat file."""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path

from loguru import logger

from irsol_data_pipeline.core.models import MaxDeltaPolicy
from irsol_data_pipeline.logging_config import setup_logging
from irsol_data_pipeline.pipeline.filesystem import (
    discover_flatfield_files,
    processed_dir_for_measurement,
    processed_output_path,
)
from irsol_data_pipeline.pipeline.flatfield_cache import build_flatfield_cache
from irsol_data_pipeline.pipeline.measurement_processor import (
    process_single_measurement,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run correction and calibration for a single measurement .dat file "
            "using flat-fields from a reduced directory."
        )
    )
    parser.add_argument("measurement_path", type=Path, help="Path to measurement .dat")
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=None,
        help="Output directory (default: ../processed from measurement path)",
    )
    parser.add_argument(
        "--flatfield-dir",
        type=Path,
        default=None,
        help="Flat-field directory (default: measurement parent directory)",
    )
    parser.add_argument(
        "--max-delta-hours",
        type=float,
        default=2.0,
        help="Maximum allowed hours between measurement and flat-field",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG logs",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    setup_logging(level="DEBUG" if args.verbose else "INFO")

    measurement_path = args.measurement_path
    if not measurement_path.is_file():
        raise FileNotFoundError(f"Measurement file not found: {measurement_path}")

    flatfield_dir = (
        args.flatfield_dir
        if args.flatfield_dir is not None
        else measurement_path.parent
    )
    if not flatfield_dir.is_dir():
        raise FileNotFoundError(f"Flat-field directory not found: {flatfield_dir}")

    output_dir = (
        args.output_dir
        if args.output_dir is not None
        else processed_dir_for_measurement(measurement_path)
    )

    flatfield_paths = discover_flatfield_files(flatfield_dir)
    if not flatfield_paths:
        raise RuntimeError(f"No flat-field files found in: {flatfield_dir}")

    max_delta_policy = MaxDeltaPolicy(
        default_max_delta=datetime.timedelta(hours=args.max_delta_hours)
    )
    ff_cache = build_flatfield_cache(
        flatfield_paths=flatfield_paths,
        max_delta=max_delta_policy.default_max_delta,
    )

    process_single_measurement(
        measurement_path=measurement_path,
        processed_dir=output_dir,
        ff_cache=ff_cache,
        max_delta_policy=max_delta_policy,
    )

    logger.info("Measurement processed", path=measurement_path)
    logger.info("Output directory", output_dir=output_dir)
    logger.success(
        "Generated FITS",
        path=processed_output_path(
            output_dir, measurement_path.name, kind="corrected_fits"
        ),
    )


if __name__ == "__main__":
    main()
