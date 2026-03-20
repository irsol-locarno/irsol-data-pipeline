"""Root Cyclopts application for idp."""

from __future__ import annotations

from cyclopts import App

from irsol_data_pipeline.version import __version__

app = App(
    name="idp",
    help=(
        "IRSOL Data Pipeline Operational CLI for serving Prefect flow groups, configuring Prefect "
        "variables, and inspecting runtime information."
    ),
    version=__version__,
)
app.register_install_completion_command()

app.command(
    "irsol_data_pipeline.cli.flows:flows_app",
    name="flows",
    help="List and serve Prefect flow groups.",
)
app.command(
    "irsol_data_pipeline.cli.variables:variables_app",
    name="variables",
    help="List and configure Prefect variables.",
)
app.command(
    "irsol_data_pipeline.cli.info:info",
    name="info",
    help="Show runtime and operational information.",
)


def main() -> None:
    """Run the root CLI application."""

    app()


if __name__ == "__main__":
    main()
