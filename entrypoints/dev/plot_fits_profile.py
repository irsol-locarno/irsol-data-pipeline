"""Entrypoint to plot Stokes profiles from a FITS file."""

from __future__ import annotations

import argparse
from pathlib import Path

from astropy.io import fits
from loguru import logger

from irsol_data_pipeline.io.fits.importer import load_fits_measurement
from irsol_data_pipeline.plotting import plot_profile


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load a pipeline FITS file and generate a Stokes profile image "
            "using metadata-based title and wavelength calibration."
        )
    )
    parser.add_argument("fits_path", type=Path, help="Path to input .fits file")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output .png path (default: <fits_stem>_profile.png)",
    )
    return parser.parse_args()


def _build_plot_title(header: fits.Header, fits_path: Path) -> str:
    """Build a descriptive profile plot title from FITS metadata."""
    meas_name = header.get("MEASNAME")
    date_obs = header.get("DATE-OBS")
    wavelength = header.get("WAVELNTH")

    parts: list[str] = []
    if isinstance(meas_name, str) and meas_name.strip():
        parts.append(meas_name.strip())
    else:
        parts.append(fits_path.stem)

    if isinstance(wavelength, (int, float)):
        parts.append(f"{int(wavelength)} A")

    if isinstance(date_obs, str) and date_obs.strip():
        parts.append(date_obs.strip())

    return " | ".join(parts)


def main() -> None:
    args = _parse_args()

    fits_path = args.fits_path
    if not fits_path.is_file():
        raise FileNotFoundError(f"FITS file not found: {fits_path}")

    output_path = args.output or fits_path.with_name(f"{fits_path.stem}_profile.png")

    imported = load_fits_measurement(fits_path)
    title = _build_plot_title(imported.header, fits_path)
    if imported.calibration is not None:
        a0 = imported.calibration.wavelength_offset
        a1 = imported.calibration.pixel_scale
    else:
        a0 = None
        a1 = None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot_profile(
        imported.stokes,
        title=title,
        filename_save=str(output_path),
        a0=a0,
        a1=a1,
    )

    logger.success("Saved profile plot", output_path=output_path)


if __name__ == "__main__":
    main()
