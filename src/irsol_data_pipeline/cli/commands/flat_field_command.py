"""Flat-field correction CLI commands.

Provides two sub-commands under ``idp flat-field``:

``apply``
    Apply flat-field correction to a single ``.dat`` measurement file.
    The command discovers all flat-field files in the same directory as
    the measurement, builds a :class:`FlatFieldCache`, and runs the full
    correction + wavelength-calibration pipeline.  Artifacts written to
    ``--output-dir``:

    * ``<stem>_corrected.fits``     - flat-field corrected Stokes FITS file
    * ``<stem>_flat_field_correction_data.fits`` - serialised correction object
    * ``<stem>_metadata.json``      - processing metadata
    * ``<stem>_profile_corrected.png`` - Stokes profile plot (corrected)
    * ``<stem>_profile_original.png``  - Stokes profile plot (original)

    If any of these files already exist in ``--output-dir`` the command
    prompts for confirmation before proceeding (bypassed by ``--force``).

``apply-day``
    Apply flat-field correction to every measurement in an observation day
    directory.  The day must follow the standard IRSOL dataset hierarchy::

        <day>/
            reduced/   ← ``.dat`` measurement and flat-field files
            processed/ ← default output location (override with --output-dir)

    Measurements that already have a ``*_corrected.fits`` **or**
    ``*_error.json`` artifact are silently skipped unless ``--force`` is
    given.  If ``--output-dir`` already exists the command prompts for
    confirmation before starting (bypassed by ``--force``).

    When a measurement fails, an ``*_error.json`` artifact is written and
    processing continues with the remaining measurements.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter, validators
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from irsol_data_pipeline.cli.common import get_console
from irsol_data_pipeline.core.config import REDUCED_DIRNAME
from irsol_data_pipeline.core.models import DayProcessingResult, ObservationDay
from irsol_data_pipeline.pipeline.filesystem import (
    discover_flatfield_files,
    processed_dir_for_day,
    processed_output_path,
)
from irsol_data_pipeline.pipeline.flatfield_cache import build_flatfield_cache
from irsol_data_pipeline.pipeline.flatfield_processor import process_observation_day
from irsol_data_pipeline.pipeline.measurement_processor import (
    convert_measurement_to_fits,
    process_single_measurement,
)

flat_field_app = App(
    name="flat-field",
    help=(
        "Apply flat-field corrections to ZIMPOL measurements. "
        "Use 'apply' for a single .dat file or 'apply-day' for an entire "
        "observation day directory."
    ),
)

_DAT_INPUT = Parameter(
    validator=validators.Path(
        ext=("dat",),
        exists=True,
        dir_okay=False,
    ),
)

_OUTPUT_DIR_OPTION = Parameter(
    name="output-dir",
    help=(
        "Target directory where processed artifacts are written. "
        "Created automatically if it does not exist."
    ),
)

_CACHE_DIR_OPTION = Parameter(
    name="cache-dir",
    help=(
        "Optional directory for flat-field correction cache files. "
        "When omitted the cache is placed under the day's processed/_cache/ "
        "directory derived from the measurement location."
    ),
)

_FORCE_OPTION = Parameter(
    name="--force",
    help=(
        "Bypass skip checks and confirmation prompts. Measurements that would "
        "normally be skipped because an output or error artifact already exists "
        "are reprocessed. Existing output files are overwritten without "
        "confirmation."
    ),
    negative=(),
)

_CONVERT_ON_FF_FAILURE_OPTION = Parameter(
    name="--convert-on-ff-failure",
    help=(
        "When a measurement fails flat-field correction, still convert it to a "
        "``*_converted.fits`` FITS file and generate a ``*_profile_converted.png`` "
        "profile plot.  The output file names and the ``FFCORR = False`` FITS "
        "header keyword clearly distinguish converted artifacts from fully "
        "corrected ones (``*_corrected.fits``)."
    ),
    negative=(),
)


def _print_day_result(result: DayProcessingResult, console: Console) -> None:
    """Render a DayProcessingResult summary table.

    Args:
        result: Processing result to render.
        console: Rich console to print to.
    """
    table = Table(
        title=f"Flat-field correction — {result.day_name}",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Metric", style="white", no_wrap=True)
    table.add_column("Count", style="white")
    table.add_row("Processed", str(result.processed))
    table.add_row("Skipped", str(result.skipped))
    table.add_row("Failed", str(result.failed))
    console.print(table)

    if result.errors:
        console.print("[bold red]Errors:[/bold red]")
        for err in result.errors:
            console.print(f"  [red]• {err}[/red]")


def _find_existing_outputs(processed_dir: Path, source_name: str) -> list[Path]:
    """Return all processed output paths that already exist on disk.

    Args:
        processed_dir: Directory to check.
        source_name: Source ``.dat`` filename.

    Returns:
        List of existing output :class:`Path` objects.
    """
    from irsol_data_pipeline.pipeline.filesystem import ProcessedOutputKind

    kinds: list[ProcessedOutputKind] = [
        "corrected_fits",
        "converted_fits",
        "error_json",
        "metadata_json",
        "flatfield_correction_data",
        "profile_corrected_png",
        "profile_original_png",
        "profile_converted_png",
    ]
    existing = []
    for kind in kinds:
        p = processed_output_path(processed_dir, source_name, kind=kind)
        if p.exists():
            existing.append(p)
    return existing


@flat_field_app.command(
    name="apply",
    help=(
        "Apply flat-field correction and wavelength auto-calibration to a "
        "single .dat measurement file.\n\n"
        "Side effects:\n"
        "  • Reads flat-field .dat files from the same directory as the "
        "measurement.\n"
        "  • Writes multiple output artifacts to --output-dir (corrected FITS, "
        "flat-field correction FITS, metadata JSON, profile PNG plots).\n"
        "  • If --cache-dir is provided, flat-field analysis results are cached "
        "there as .fits files to speed up subsequent runs.\n"
        "  • Prompts for confirmation when output artifacts already exist "
        "(bypassed by --force).\n"
        "  • With --convert-on-ff-failure, a failed measurement is additionally "
        "converted to *_converted.fits and *_profile_converted.png."
    ),
)
def apply(
    measurement_path: Annotated[Path, _DAT_INPUT],
    /,
    *,
    output_dir: Annotated[Path, _OUTPUT_DIR_OPTION],
    cache_dir: Annotated[Path | None, _CACHE_DIR_OPTION] = None,
    force: Annotated[bool, _FORCE_OPTION] = False,
    convert_on_ff_failure: Annotated[bool, _CONVERT_ON_FF_FAILURE_OPTION] = False,
) -> None:
    """Apply flat-field correction to a single measurement .dat file.

    Discovers flat-field files in the same directory as the measurement,
    builds a flat-field cache, and runs the full correction pipeline.

    Args:
        measurement_path: Existing input ``.dat`` measurement file.
        output_dir: Target directory where processed artifacts are written.
        cache_dir: Optional directory for flat-field correction cache files.
        force: When True, skip confirmation prompts and "already processed"
            checks.
        convert_on_ff_failure: When True, a failed measurement is converted to
            ``*_converted.fits`` and ``*_profile_converted.png`` even if
            flat-field correction cannot be applied.
    """
    console = get_console()
    resolved_measurement = measurement_path.resolve()
    resolved_output_dir = output_dir.resolve()
    resolved_cache_dir = cache_dir.resolve() if cache_dir is not None else None

    # Check for existing output files and prompt unless --force
    if not force:
        existing_outputs = _find_existing_outputs(
            resolved_output_dir,
            resolved_measurement.name,
        )
        if existing_outputs:
            console.print(
                "[yellow]The following output files already exist in "
                f"{resolved_output_dir}:[/yellow]",
            )
            for p in existing_outputs:
                console.print(f"  [yellow]• {p.name}[/yellow]")
            if not Confirm.ask("Overwrite existing files and proceed?", default=False):
                console.print("[bold red]Aborted.[/bold red]")
                sys.exit(1)

    # Discover flat-field files from the same directory as the measurement
    reduced_dir = resolved_measurement.parent
    flatfield_paths = discover_flatfield_files(reduced_dir)

    if not flatfield_paths:
        if convert_on_ff_failure:
            console.print(
                f"[yellow]No flat-field files found in {reduced_dir}. "
                "Flat-field correction is not possible — converting measurement "
                "to FITS without correction (--convert-on-ff-failure).[/yellow]",
            )
            resolved_output_dir.mkdir(parents=True, exist_ok=True)
            try:
                with console.status(f"Converting {resolved_measurement.name}…"):
                    convert_measurement_to_fits(
                        measurement_path=resolved_measurement,
                        processed_dir=resolved_output_dir,
                    )
                console.print(
                    f"[bold yellow]⚠ Converted (no flat-field correction) "
                    f"{resolved_measurement.name}[/bold yellow]",
                )
                console.print(f"  Output directory: {resolved_output_dir}")
            except Exception as exc:
                console.print(
                    f"[bold red]✗ Failed to convert "
                    f"{resolved_measurement.name}: {exc}[/bold red]",
                )
                sys.exit(1)
            return
        console.print(
            f"[bold red]No flat-field files found in {reduced_dir}. "
            "Cannot apply flat-field correction.[/bold red]",
        )
        sys.exit(1)

    console.print(
        f"[cyan]Found {len(flatfield_paths)} flat-field file(s) in {reduced_dir}[/cyan]",
    )

    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    with console.status("Building flat-field cache…"):
        ff_cache = build_flatfield_cache(
            flatfield_paths=flatfield_paths,
            cache_dir=resolved_cache_dir,
        )

    console.print(
        f"[cyan]Flat-field cache ready — "
        f"{len(ff_cache)} correction(s), "
        f"wavelengths: {ff_cache.wavelengths}[/cyan]",
    )

    try:
        with console.status(f"Processing {resolved_measurement.name}…"):
            process_single_measurement(
                measurement_path=resolved_measurement,
                processed_dir=resolved_output_dir,
                ff_cache=ff_cache,
            )
        console.print(
            f"[bold green]✓ Successfully processed "
            f"{resolved_measurement.name}[/bold green]",
        )
        console.print(f"  Output directory: {resolved_output_dir}")
    except Exception as exc:
        console.print(
            f"[bold red]✗ Failed to process "
            f"{resolved_measurement.name}: {exc}[/bold red]",
        )
        if convert_on_ff_failure:
            console.print(
                "[yellow]Attempting to convert measurement to FITS without "
                "flat-field correction (--convert-on-ff-failure)…[/yellow]",
            )
            try:
                with console.status(f"Converting {resolved_measurement.name}…"):
                    convert_measurement_to_fits(
                        measurement_path=resolved_measurement,
                        processed_dir=resolved_output_dir,
                    )
                console.print(
                    f"[bold yellow]⚠ Converted (no flat-field correction) "
                    f"{resolved_measurement.name}[/bold yellow]",
                )
                console.print(f"  Output directory: {resolved_output_dir}")
            except Exception as conv_exc:
                console.print(
                    f"[bold red]✗ Conversion also failed: {conv_exc}[/bold red]",
                )
        sys.exit(1)


@flat_field_app.command(
    name="apply-day",
    help=(
        "Apply flat-field correction and wavelength auto-calibration to all "
        "measurements in an observation day directory.\n\n"
        "The day must contain a 'reduced/' sub-directory with .dat measurement "
        "and flat-field files.\n\n"
        "Side effects:\n"
        "  • Reads all .dat measurement files from <day>/reduced/.\n"
        "  • Writes multiple artifacts per measurement to --output-dir "
        "(corrected FITS, flat field correction FITS, metadata JSON, profile PNG "
        "plots).\n"
        "  • Measurements with an existing *_corrected.fits, *_converted.fits, or "
        "*_error.json artifact are silently skipped (use --force to reprocess "
        "them).\n"
        "  • When a measurement fails, an *_error.json artifact is written and "
        "processing continues with the remaining measurements.\n"
        "  • With --convert-on-ff-failure, failed measurements are additionally "
        "converted to *_converted.fits and *_profile_converted.png.\n"
        "  • Prompts for confirmation when --output-dir already exists "
        "(bypassed by --force)."
    ),
)
def apply_day(
    day_path: Annotated[
        Path,
        Parameter(
            validator=validators.Path(exists=True, dir_okay=True, file_okay=False),
            help="Path to the observation day directory (must contain a 'reduced/' sub-directory).",
        ),
    ],
    /,
    *,
    output_dir: Annotated[
        Path | None,
        Parameter(
            name="output-dir",
            help=(
                "Target directory where processed artifacts are written. "
                "Defaults to <day>/processed/. Created automatically if it "
                "does not exist."
            ),
        ),
    ] = None,
    force: Annotated[bool, _FORCE_OPTION] = False,
    convert_on_ff_failure: Annotated[bool, _CONVERT_ON_FF_FAILURE_OPTION] = False,
) -> None:
    """Apply flat-field correction to all measurements in an observation day.

    Discovers measurement and flat-field files from ``<day_path>/reduced/``,
    builds a shared flat-field cache, and processes every unprocessed
    measurement.

    Args:
        day_path: Path to the observation day directory.
        output_dir: Target directory for processed artifacts. Defaults to
            ``<day_path>/processed/``.
        force: When True, skip confirmation prompts and reprocess every
            measurement regardless of existing artifacts.
        convert_on_ff_failure: When True, measurements that fail flat-field
            correction are converted to ``*_converted.fits`` and
            ``*_profile_converted.png``.
    """
    console = get_console()
    resolved_day_path = day_path.resolve()

    # Validate that the reduced directory exists
    reduced_dir = resolved_day_path / REDUCED_DIRNAME
    if not reduced_dir.is_dir():
        console.print(
            f"[bold red]No 'reduced/' directory found under {resolved_day_path}. "
            "Expected the observation day to contain a 'reduced/' sub-directory.[/bold red]",
        )
        sys.exit(1)

    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else processed_dir_for_day(resolved_day_path)
    )

    # Prompt if output directory already exists and has content (unless --force)
    if (
        not force
        and resolved_output_dir.exists()
        and any(resolved_output_dir.iterdir())
    ):
        console.print(
            f"[yellow]Output directory already exists and is not empty: "
            f"{resolved_output_dir}[/yellow]",
        )
        if not Confirm.ask(
            "Proceed? Previously processed measurements will be skipped "
            "(use --force to reprocess them).",
            default=False,
        ):
            console.print("[bold red]Aborted.[/bold red]")
            sys.exit(1)

    observation_day = ObservationDay(
        path=resolved_day_path,
        raw_dir=resolved_day_path / "raw",
        reduced_dir=reduced_dir,
        processed_dir=resolved_output_dir,
    )

    console.print(f"[cyan]Processing observation day: {resolved_day_path.name}[/cyan]")
    console.print(f"  Reduced directory : {reduced_dir}")
    console.print(f"  Output directory  : {resolved_output_dir}")
    if force:
        console.print(
            "  [yellow]--force: all measurements will be reprocessed[/yellow]",
        )
    if convert_on_ff_failure:
        console.print(
            "  [yellow]--convert-on-ff-failure: failed measurements will be "
            "converted to FITS without flat-field correction[/yellow]",
        )

    try:
        with console.status(f"Processing day {resolved_day_path.name}…"):
            result = process_observation_day(
                day=observation_day,
                force=force,
                convert_on_ff_failure=convert_on_ff_failure,
            )
    except Exception as exc:
        console.print(
            f"[bold red]✗ Unexpected error processing day "
            f"{resolved_day_path.name}: {exc}[/bold red]",
        )
        sys.exit(1)

    _print_day_result(result, console)

    if result.failed > 0:
        console.print(f"[bold red]✗ {result.failed} measurement(s) failed.[/bold red]")
        sys.exit(1)
    elif result.processed == 0 and result.skipped > 0:
        console.print(
            "[yellow]All measurements were already processed and skipped. "
            "Use --force to reprocess.[/yellow]",
        )
    else:
        console.print(
            f"[bold green]✓ Day {resolved_day_path.name} complete.[/bold green]",
        )
