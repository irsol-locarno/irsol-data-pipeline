"""Root Cyclopts application for idp."""

from __future__ import annotations

from cyclopts import App

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
    "irsol_data_pipeline.cli.prefect:prefect_app",
    name="prefect",
    help="Run Prefect server commands through the unified CLI.",
)
app.command(
    "irsol_data_pipeline.cli.plot:plot_app",
    name="plot",
    help="Render plots from observation files.",
)


def main() -> None:
    """Run the root CLI application."""

    app()


if __name__ == "__main__":
    main()
