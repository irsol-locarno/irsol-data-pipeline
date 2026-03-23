"""Prefect command group integrations for the unified CLI."""

from __future__ import annotations

import subprocess
import sys
from typing import Any, cast

from cyclopts import App

from irsol_data_pipeline.prefect.config import PREFECT_PROFILE_SETTINGS

prefect_app = App(name="prefect", help="Run Prefect server commands.")


@prefect_app.command(name="start")
def start_prefect_server() -> None:
    """Start the Prefect server after applying local dashboard config.

    This keeps local development behavior aligned with the ``prefect/setup``
    make target by ensuring API URL and analytics settings are persisted in
    Prefect config before the server starts.
    """

    from prefect.settings.profiles import update_current_profile

    update_current_profile(cast(dict[Any, Any], PREFECT_PROFILE_SETTINGS))

    result = subprocess.run(
        [sys.executable, "-m", "prefect", "server", "start"], check=False
    )
    sys.exit(result.returncode)


prefect_app.command(
    "prefect.cli.server:reset",
    name="reset-database",
    help="Reset the Prefect server database. This is a destructive operation, which will delete all flow run history from the Prefect server. Use with caution.",
)

prefect_app.command(
    "irsol_data_pipeline.cli.commands.prefect_command.flows_command:flows_app",
    name="flows",
    help="List and serve Prefect flow groups.",
)

prefect_app.command(
    "irsol_data_pipeline.cli.commands.prefect_command.status_command:status",
    name="status",
    help="Check whether the local Prefect dashboard is reachable.",
)

prefect_app.command(
    "irsol_data_pipeline.cli.commands.prefect_command.variables_command:variables_app",
    name="variables",
    help="List and configure Prefect variables.",
)
