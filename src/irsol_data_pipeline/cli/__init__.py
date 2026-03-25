"""Command-line entry points for the irsol-data-pipeline package."""

from __future__ import annotations

import sys
from typing import Annotated

from cyclopts import App, Parameter

from irsol_data_pipeline.cli.common import ensure_prefect_enabled
from irsol_data_pipeline.logging_config import LOG_LEVEL, setup_logging
from irsol_data_pipeline.version import __version__

app = App(
    name="idp",
    help=(
        "IRSOL Data Pipeline Operational CLI for Prefect server operations and inspecting runtime "
        "information."
    ),
    version=__version__,
)
app.register_install_completion_command()

app.command(
    "irsol_data_pipeline.cli.commands.info_command:info",
    name="info",
    help="Show runtime and operational information.",
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
    "irsol_data_pipeline.cli.commands.configure_command:configure_prefect",
    name="configure",
    help="(Maintainer) Create or update the Prefect server profile with database and API settings.",
)
app.command(
    "irsol_data_pipeline.cli.commands.setup_command:setup",
    name="setup",
    help="(User) Configure your Prefect client profile to connect to the shared server.",
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
