"""Slit-image generation CLI commands.

Provides two sub-commands under ``idp slit-image``:

``generate``
    Generate a six-panel slit context image for a single ``.dat``
    measurement file.  The command fetches SDO/AIA maps from the JSOC
    service and overlays the IRSOL slit geometry on top.

    Artifact written to ``--output-dir``:

    * ``<stem>_slit_preview.png`` - six-panel slit context image

    On failure an ``<stem>_slit_preview_error.json`` file is written
    instead.  If either artifact already exists in ``--output-dir``
    the command reports a skip (bypassed by ``--force``).

``generate-day``
    Generate slit context images for every measurement in an observation
    day directory.  The day must follow the standard IRSOL dataset
    hierarchy::

        <day>/
            reduced/   ← ``.dat`` measurement files
            processed/ ← default output location (override with --output-dir)

    Measurements that already have a ``*_slit_preview.png`` **or**
    ``*_slit_preview_error.json`` artifact are silently skipped unless
    ``--force`` is given.  If ``--output-dir`` already exists the command
    prompts for confirmation before starting (bypassed by ``--force``).

    When a measurement fails, a ``*_slit_preview_error.json`` artifact is
    written and processing continues with the remaining measurements.
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
    is_measurement_slit_preview_generated,
    processed_dir_for_day,
    processed_output_path,
)
from irsol_data_pipeline.pipeline.slit_images_processor import (
    generate_slit_image,
    generate_slit_images_for_day,
)

slit_image_app = App(
    name="slit-image",
    help=(
        "Generate slit context images from ZIMPOL measurements using SDO/AIA data. "
        "Use 'generate' for a single .dat file or 'generate-day' for an entire "
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

_JSOC_EMAIL_OPTION = Parameter(
    name="jsoc-email",
    help=(
        "Email address registered with the JSOC DRMS service for SDO/AIA data "
        "requests. Required for fetching solar context images."
    ),
)

_OUTPUT_DIR_OPTION = Parameter(
    name="output-dir",
    help=(
        "Target directory where slit preview PNG files are written. "
        "Created automatically if it does not exist."
    ),
)

_CACHE_DIR_OPTION = Parameter(
    name="cache-dir",
    help=(
        "Optional directory for caching downloaded SDO/AIA FITS files. "
        "Reusing the same cache across runs avoids redundant JSOC downloads. "
        "When omitted a temporary directory is used."
    ),
)

_FORCE_OPTION = Parameter(
    name="--force",
    help=(
        "Bypass skip checks and confirmation prompts. Measurements that would "
        "normally be skipped because a slit preview or error artifact already "
        "exists are regenerated. Existing output files are overwritten without "
        "confirmation."
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
        title=f"Slit-image generation — {result.day_name}",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Metric", style="white", no_wrap=True)
    table.add_column("Count", style="white")
    table.add_row("Generated", str(result.processed))
    table.add_row("Skipped", str(result.skipped))
    table.add_row("Failed", str(result.failed))
    console.print(table)

    if result.errors:
        console.print("[bold red]Errors:[/bold red]")
        for err in result.errors:
            console.print(f"  [red]• {err}[/red]")


@slit_image_app.command(
    name="generate",
    help=(
        "Generate a six-panel slit context image for a single .dat measurement "
        "file using SDO/AIA data from the JSOC service.\n\n"
        "Side effects:\n"
        "  • Fetches SDO/AIA FITS maps from the JSOC DRMS service "
        "(requires internet access and a registered JSOC email).\n"
        "  • SDO data may be cached to --cache-dir (or a temporary directory) "
        "to avoid redundant downloads.\n"
        "  • Writes a *_slit_preview.png file to --output-dir on success.\n"
        "  • Writes a *_slit_preview_error.json file to --output-dir on failure.\n"
        "  • Skips generation (with a warning) when a slit preview or error "
        "artifact already exists in --output-dir (bypassed by --force)."
    ),
)
def generate(
    measurement_path: Annotated[Path, _DAT_INPUT],
    /,
    *,
    jsoc_email: Annotated[str, _JSOC_EMAIL_OPTION],
    output_dir: Annotated[Path, _OUTPUT_DIR_OPTION],
    cache_dir: Annotated[Path | None, _CACHE_DIR_OPTION] = None,
    force: Annotated[bool, _FORCE_OPTION] = False,
) -> None:
    """Generate a slit context image for a single measurement.

    Fetches SDO/AIA maps matching the measurement timestamp and renders a
    six-panel slit context image showing the IRSOL slit geometry overlaid on
    solar full-disk and zoomed views.

    Args:
        measurement_path: Existing input ``.dat`` measurement file.
        jsoc_email: Email address registered with the JSOC DRMS service.
        output_dir: Target directory where the slit preview PNG is written.
        cache_dir: Optional directory for caching SDO/AIA FITS files.
        force: When True, skip the "already generated" check and regenerate
            the slit image even if an artifact already exists.
    """
    console = get_console()
    resolved_measurement = measurement_path.expanduser().resolve()
    resolved_output_dir = output_dir.expanduser().resolve()
    resolved_cache_dir = (
        cache_dir.expanduser().resolve() if cache_dir is not None else None
    )

    # Check if a slit preview or error artifact already exists
    if not force and is_measurement_slit_preview_generated(
        resolved_output_dir,
        resolved_measurement.name,
    ):
        preview_path = processed_output_path(
            resolved_output_dir,
            resolved_measurement.name,
            kind="slit_preview_png",
        )
        error_path = processed_output_path(
            resolved_output_dir,
            resolved_measurement.name,
            kind="slit_preview_error_json",
        )
        existing = [p for p in (preview_path, error_path) if p.exists()]
        console.print(
            "[yellow]Slit preview artifact already exists — skipping "
            f"{resolved_measurement.name}:[/yellow]",
        )
        for p in existing:
            console.print(f"  [yellow]• {p.name}[/yellow]")
        console.print("[yellow]Use --force to regenerate.[/yellow]")
        return

    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    try:
        with console.status(f"Generating slit image for {resolved_measurement.name}…"):
            generate_slit_image(
                measurement_path=resolved_measurement,
                processed_dir=resolved_output_dir,
                jsoc_email=jsoc_email,
                sdo_cache_dir=resolved_cache_dir,
            )
        console.print(
            f"[bold green]✓ Slit image generated for "
            f"{resolved_measurement.name}[/bold green]",
        )
        console.print(f"  Output directory: {resolved_output_dir}")
    except Exception as exc:
        console.print(
            f"[bold red]✗ Failed to generate slit image for "
            f"{resolved_measurement.name}: {exc}[/bold red]",
        )
        error_path = processed_output_path(
            resolved_output_dir,
            resolved_measurement.name,
            kind="slit_preview_error_json",
        )
        if error_path.exists():
            console.print(f"  Error details written to: {error_path}")
        sys.exit(1)


@slit_image_app.command(
    name="generate-day",
    help=(
        "Generate slit context images for all measurements in an observation "
        "day directory.\n\n"
        "The day must contain a 'reduced/' sub-directory with .dat measurement "
        "files.\n\n"
        "Side effects:\n"
        "  • Fetches SDO/AIA FITS maps from the JSOC DRMS service for each "
        "measurement (requires internet access and a registered JSOC email).\n"
        "  • SDO data is cached under <day>/processed/_cache/sdo/ (or "
        "--output-dir/_cache/sdo/ when --output-dir is provided) to avoid "
        "redundant downloads.\n"
        "  • Writes *_slit_preview.png files to --output-dir on success.\n"
        "  • Writes *_slit_preview_error.json files to --output-dir on failure.\n"
        "  • Measurements with an existing *_slit_preview.png or "
        "*_slit_preview_error.json artifact are silently skipped "
        "(use --force to regenerate them).\n"
        "  • Prompts for confirmation when --output-dir already exists "
        "(bypassed by --force)."
    ),
)
def generate_day(
    day_path: Annotated[
        Path,
        Parameter(
            validator=validators.Path(exists=True, dir_okay=True, file_okay=False),
            help=(
                "Path to the observation day directory "
                "(must contain a 'reduced/' sub-directory)."
            ),
        ),
    ],
    /,
    *,
    jsoc_email: Annotated[str, _JSOC_EMAIL_OPTION],
    output_dir: Annotated[
        Path | None,
        Parameter(
            name="output-dir",
            help=(
                "Target directory where slit preview PNG files are written. "
                "Defaults to <day>/processed/. Created automatically if it "
                "does not exist."
            ),
        ),
    ] = None,
    force: Annotated[bool, _FORCE_OPTION] = False,
) -> None:
    """Generate slit context images for all measurements in an observation day.

    Discovers measurement files from ``<day_path>/reduced/`` and generates a
    slit preview image for each unprocessed measurement.

    Args:
        day_path: Path to the observation day directory.
        jsoc_email: Email address registered with the JSOC DRMS service.
        output_dir: Target directory for slit preview PNG files. Defaults to
            ``<day_path>/processed/``.
        force: When True, skip confirmation prompts and regenerate slit images
            for every measurement regardless of existing artifacts.
    """
    console = get_console()
    resolved_day_path = day_path.expanduser().resolve()

    # Validate that the reduced directory exists
    reduced_dir = resolved_day_path / REDUCED_DIRNAME
    if not reduced_dir.is_dir():
        console.print(
            f"[bold red]No 'reduced/' directory found under {resolved_day_path}. "
            "Expected the observation day to contain a 'reduced/' sub-directory.[/bold red]",
        )
        sys.exit(1)

    resolved_output_dir = (
        output_dir.expanduser().resolve()
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
            "Proceed? Measurements with existing artifacts will be skipped "
            "(use --force to regenerate them).",
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

    console.print(
        f"[cyan]Generating slit images for observation day: "
        f"{resolved_day_path.name}[/cyan]",
    )
    console.print(f"  Reduced directory : {reduced_dir}")
    console.print(f"  Output directory  : {resolved_output_dir}")
    if force:
        console.print(
            "  [yellow]--force: all measurements will be regenerated[/yellow]",
        )

    try:
        with console.status(
            f"Generating slit images for day {resolved_day_path.name}…",
        ):
            result = generate_slit_images_for_day(
                day=observation_day,
                jsoc_email=jsoc_email,
                force=force,
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
            "Use --force to regenerate.[/yellow]",
        )
    else:
        console.print(
            f"[bold green]✓ Day {resolved_day_path.name} complete.[/bold green]",
        )
