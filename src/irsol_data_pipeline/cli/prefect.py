"""Prefect command group integrations for the unified CLI."""

from __future__ import annotations

from cyclopts import App

prefect_app = App(name="prefect", help="Run Prefect server commands.")

prefect_app.command(
    "prefect.cli.server:start",
    name="start",
    help="Start a Prefect server instance.",
)

prefect_app.command(
    "prefect.cli.server:reset",
    name="reset-database",
    help="Reset the Prefect server database. This is a destructive operation, which will delete all flow run history from the Prefect server. Use with caution.",
)

prefect_app.command(
    "irsol_data_pipeline.cli.flows:flows_app",
    name="flows",
    help="List and serve Prefect flow groups.",
)

prefect_app.command(
    "irsol_data_pipeline.cli.variables:variables_app",
    name="variables",
    help="List and configure Prefect variables.",
)
