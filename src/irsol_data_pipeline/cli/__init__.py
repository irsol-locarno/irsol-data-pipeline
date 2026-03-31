"""Command-line entry points for the irsol-data-pipeline package."""

from __future__ import annotations

import sys
from typing import Annotated

from cyclopts import App, Parameter

from irsol_data_pipeline.cli.common import ensure_prefect_enabled
from irsol_data_pipeline.logging_config import LOG_LEVEL, setup_logging
from irsol_data_pipeline.version import (
    _DISTRIBUTION_NAME,
    __relevant_distribution_versions__,
    __version__,
)


def _build_version_string():
    version = f"{_DISTRIBUTION_NAME}={__version__}"
    version += "\n\ndistributions:\n"
    for distribution, d_version in __relevant_distribution_versions__:
        version += f" - {distribution}={d_version}\n"
    return version


app = App(
    name="idp",
    help=(
        "IRSOL Data Pipeline CLI — process ZIMPOL spectropolarimetric solar "
        "observations.\n"
        "Commands:\n"
        "  info          Show runtime and operational information.\n"
        "  flat-field    Apply flat-field corrections to measurements.\n"
        "  slit-image    Generate slit context images from measurements.\n"
        "  plot          Render Stokes profile and slit context plots.\n"
        "  prefect       Run Prefect server commands.\n"
        "  setup         Configure local or server Prefect profiles."
    ),
    version=_build_version_string(),
)
app.register_install_completion_command()

app.command(
    "irsol_data_pipeline.cli.commands.info_command:info",
    name="info",
    help="Show runtime and operational information.",
)
app.command(
    "irsol_data_pipeline.cli.commands.flat_field_command:flat_field_app",
    name="flat-field",
    help="Apply flat-field corrections to measurements.",
)
app.command(
    "irsol_data_pipeline.cli.commands.slit_image_command:slit_image_app",
    name="slit-image",
    help="Generate slit context images from measurements.",
)
app.command(
    "irsol_data_pipeline.cli.commands.prefect_command:prefect_app",
    name="prefect",
    help="Run Prefect server commands through the unified CLI.",
)
app.command(
    "irsol_data_pipeline.cli.commands.plot_command:plot_app",
    name="plot",
    help="Render plots from observation files.",
)
app.command(
    "irsol_data_pipeline.cli.commands.setup_command:setup_app",
    name="setup",
    help="Configure local (user) or server (maintainer) Prefect profiles.",
)

_VERBOSE_TO_LOG_LEVEL: dict[int, LOG_LEVEL] = {
    0: "INFO",
    1: "DEBUG",
    2: "TRACE",
}


@app.meta.default
def _meta(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    verbose: Annotated[
        int,
        Parameter(
            name="-v",
            count=True,
            help="Increase log verbosity: -v → DEBUG, -vv → TRACE.",
        ),
    ] = 0,
    log_level: Annotated[
        LOG_LEVEL | None,
        Parameter(
            name="--log-level",
            help="Explicit log level. Mutually exclusive with -v.",
        ),
    ] = None,
) -> None:
    """Configure global runtime options and dispatch to the selected command.

    Args:
        *tokens: Remaining command tokens forwarded to the actual command app.
        verbose: Verbosity count; each -v raises the level (INFO → DEBUG → TRACE).
        log_level: Explicit log level override. Mutually exclusive with -v.
    """
    if verbose and log_level is not None:
        print("Error: -v and --log-level are mutually exclusive.", file=sys.stderr)
        sys.exit(1)
    level: LOG_LEVEL = (
        log_level
        if log_level is not None
        else _VERBOSE_TO_LOG_LEVEL.get(verbose, "TRACE")
    )
    setup_logging(level=level, force=True)
    ensure_prefect_enabled()
    app(tokens)


def main() -> None:
    """Run the root CLI application."""

    app.meta()
