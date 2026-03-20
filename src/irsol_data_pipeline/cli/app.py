"""Root Cyclopts application for idp."""

from __future__ import annotations

import sys
from typing import Annotated

from cyclopts import App, Parameter

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
    "irsol_data_pipeline.cli.info:info",
    name="info",
    help="Show runtime and operational information.",
)
app.command(
    "irsol_data_pipeline.cli.prefect_command:prefect_app",
    name="prefect",
    help="Run Prefect server commands through the unified CLI.",
)
app.command(
    "irsol_data_pipeline.cli.plot:plot_app",
    name="plot",
    help="Render plots from observation files.",
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
    app(tokens)


def main() -> None:
    """Run the root CLI application."""

    app.meta()


if __name__ == "__main__":
    main()
