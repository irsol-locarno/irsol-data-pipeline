"""CLI interface for the solar pipeline.

Commands:
    solar-pipeline scan              — Scan dataset for unprocessed measurements
    solar-pipeline process-day       — Process a single observation day
    solar-pipeline process-measurement — Process a single measurement
    solar-pipeline export-fits       — Export a .dat file to FITS format
    solar-pipeline plot-stokes       — Plot Stokes parameters from a measurement file
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from irsol_data_pipeline.logging_config import setup_logging

app = typer.Typer(
    name="solar-pipeline",
    help="IRSOL Solar Observation Data Processing Pipeline",
    no_args_is_help=True,
)

console = Console()


def _setup_logging(verbose: bool = False) -> None:
    level = "TRACE" if verbose else "INFO"
    setup_logging(level=level)


def _get_dataset_root() -> Path:
    """Get the dataset root from environment or raise."""
    root = os.environ.get("SOLAR_PIPELINE_ROOT")
    if root is None:
        console.print(
            "[red]Error:[/red] SOLAR_PIPELINE_ROOT environment variable is not set.\n"
            "Set it to the dataset root directory, e.g.:\n"
            "  export SOLAR_PIPELINE_ROOT=/data/mdata/pdata/irsol/zimpol"
        )
        raise typer.Exit(1)
    path = Path(root)
    if not path.is_dir():
        console.print(f"[red]Error:[/red] Dataset root does not exist: {path}")
        raise typer.Exit(1)
    return path


@app.command()
def scan(
    root: Optional[str] = typer.Option(
        None,
        "--root",
        "-r",
        help="Dataset root directory. Defaults to SOLAR_PIPELINE_ROOT env var.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Scan the dataset for unprocessed measurements."""
    _setup_logging(verbose)
    from irsol_data_pipeline.pipeline.scanner import scan_dataset

    dataset_root = Path(root) if root else _get_dataset_root()
    result = scan_dataset(dataset_root)

    table = Table(title="Dataset Scan Results")
    table.add_column("Observation Day", style="cyan")
    table.add_column("Pending", justify="right", style="yellow")

    for day_name, pending in sorted(result.pending_measurements.items()):
        table.add_row(day_name, str(len(pending)))

    console.print(table)
    console.print(
        f"\nTotal: {result.total_measurements} measurements, "
        f"[yellow]{result.total_pending}[/yellow] pending"
    )


@app.command()
def process_day(
    day_path: str = typer.Argument(
        ...,
        help="Path to the observation day directory (containing reduced/).",
    ),
    max_delta_hours: float = typer.Option(
        2.0,
        "--max-delta",
        help="Maximum hours between measurement and flat-field.",
    ),
    refdata_dir: Optional[str] = typer.Option(
        None,
        "--refdata",
        help="Path to wavelength calibration reference data.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Process all unprocessed measurements for a single observation day."""
    _setup_logging(verbose)
    from irsol_data_pipeline.core.models import MaxDeltaPolicy, ObservationDay
    from irsol_data_pipeline.pipeline.day_processor import (
        process_observation_day,
    )

    path = Path(day_path)
    if not path.is_dir():
        console.print(f"[red]Error:[/red] Directory not found: {path}")
        raise typer.Exit(1)

    day = ObservationDay(
        path=path,
        raw_dir=path / "raw",
        reduced_dir=path / "reduced",
        processed_dir=path / "processed",
    )

    policy = MaxDeltaPolicy(default_max_delta=datetime.timedelta(hours=max_delta_hours))

    ref_path = Path(refdata_dir) if refdata_dir else None

    result = process_observation_day(
        day=day,
        max_delta_policy=policy,
        refdata_dir=ref_path,
    )

    console.print(f"\n[bold]Processing complete: {result.day_name}[/bold]")
    console.print(f"  Total:     {result.total_measurements}")
    console.print(f"  Processed: [green]{result.processed}[/green]")
    console.print(f"  Skipped:   {result.skipped}")
    console.print(f"  Failed:    [red]{result.failed}[/red]")

    if result.errors:
        console.print("\n[red]Errors:[/red]")
        for err in result.errors:
            console.print(f"  - {err}")


@app.command()
def process_measurement(
    measurement_path: str = typer.Argument(
        ..., help="Path to the measurement .dat file."
    ),
    output_dir: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output directory. Defaults to processed/ next to reduced/.",
    ),
    flatfield_dir: Optional[str] = typer.Option(
        None,
        "--flatfield-dir",
        help="Directory with flat-field files. Defaults to same as measurement.",
    ),
    max_delta_hours: float = typer.Option(
        2.0,
        "--max-delta",
        help="Maximum hours between measurement and flat-field.",
    ),
    refdata_dir: Optional[str] = typer.Option(
        None,
        "--refdata",
        help="Path to wavelength calibration reference data.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Process a single measurement file."""
    _setup_logging(verbose)
    from irsol_data_pipeline.core.models import MaxDeltaPolicy
    from irsol_data_pipeline.io.filesystem import discover_flatfield_files
    from irsol_data_pipeline.pipeline.day_processor import (
        process_single_measurement,
    )
    from irsol_data_pipeline.pipeline.flatfield_cache import build_flatfield_cache

    meas = Path(measurement_path)
    if not meas.is_file():
        console.print(f"[red]Error:[/red] File not found: {meas}")
        raise typer.Exit(1)

    ff_dir = Path(flatfield_dir) if flatfield_dir else meas.parent
    processed_dir = Path(output_dir) if output_dir else meas.parent.parent / "processed"
    ref_path = Path(refdata_dir) if refdata_dir else None

    policy = MaxDeltaPolicy(default_max_delta=datetime.timedelta(hours=max_delta_hours))

    ff_paths = discover_flatfield_files(ff_dir)
    if not ff_paths:
        console.print(f"[red]Error:[/red] No flat-field files found in {ff_dir}")
        raise typer.Exit(1)

    console.print(f"Building flat-field cache from {len(ff_paths)} files...")
    ff_cache = build_flatfield_cache(ff_paths, max_delta=policy.default_max_delta)

    try:
        process_single_measurement(
            measurement_path=meas,
            processed_dir=processed_dir,
            ff_cache=ff_cache,
            max_delta_policy=policy,
            refdata_dir=ref_path,
        )
        console.print(f"[green]Successfully processed:[/green] {meas.name}")
    except Exception as e:
        console.print(f"[red]Failed:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def export_fits(
    dat_path: str = typer.Argument(..., help="Path to the .dat file to export."),
    output: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output .fits file path. Auto-generated if not given.",
    ),
    refdata_dir: Optional[str] = typer.Option(
        None,
        "--refdata",
        help="Path to wavelength calibration reference data.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Export a .dat measurement file to FITS format."""
    _setup_logging(verbose)
    from irsol_data_pipeline.fits.exporter import export_to_fits

    dat = Path(dat_path)
    if not dat.is_file():
        console.print(f"[red]Error:[/red] File not found: {dat}")
        raise typer.Exit(1)

    if output is None:
        output_path = dat.with_suffix(".fits")
    else:
        output_path = Path(output)

    ref_path = Path(refdata_dir) if refdata_dir else None

    result = export_to_fits(dat, output_path=output_path, refdata_dir=ref_path)
    if result:
        console.print(f"[green]Exported:[/green] {result}")
    else:
        console.print("[yellow]No output written.[/yellow]")


@app.command()
def plot_stokes(
    measurement_path: str = typer.Argument(
        ..., help="Path to the measurement .dat file."
    ),
    output: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output .png file path. Defaults to <measurement>.png next to the input file.",
    ),
    calibrate: bool = typer.Option(
        False,
        "--calibrate",
        help="Apply wavelength calibration to the plot using reference data.",
    ),
    title: Optional[str] = typer.Option(
        None,
        "--title",
        "-t",
        help="Title for the plot. Defaults to the measurement filename.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Plot Stokes parameters (I, Q/I, U/I, V/I) from a measurement file."""
    _setup_logging(verbose)
    from irsol_data_pipeline.core.calibration.autocalibrate import calibrate_measurement
    from irsol_data_pipeline.io.dat_reader import load_measurement
    from irsol_data_pipeline.plotting import plot_profile

    meas_path = Path(measurement_path)
    if not meas_path.is_file():
        console.print(f"[red]Error:[/red] File not found: {meas_path}")
        raise typer.Exit(1)

    if output is None:
        output_path = meas_path.with_suffix(".png")
    else:
        output_path = Path(output)

    if output_path.exists():
        overwrite = typer.confirm(f"File already exists: {output_path}. Overwrite?")
        if not overwrite:
            console.print("[yellow]Aborted.[/yellow]")
            raise typer.Exit(0)

    console.print(f"Loading measurement: {meas_path.name}")
    measurement = load_measurement(meas_path)

    if calibrate:
        console.print(f"Calibrating measurement: {meas_path.name}")
        calibration_result = calibrate_measurement(measurement.stokes)
        wavelength_offset = calibration_result.wavelength_offset
        pixel_scale = calibration_result.pixel_scale
    else:
        wavelength_offset = None
        pixel_scale = None

    plot_title = title if title else meas_path.stem

    console.print("Plotting Stokes parameters...")
    plot_profile(
        measurement.stokes,
        title=plot_title,
        filename_save=str(output_path),
        a0=wavelength_offset,
        a1=pixel_scale,
    )

    console.print(f"[green]Saved:[/green] {output_path}")


if __name__ == "__main__":
    app()
