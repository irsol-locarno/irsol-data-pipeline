"""Prefect command group integrations for the unified CLI."""

from __future__ import annotations

import subprocess
import sys
from urllib.parse import urlparse

from cyclopts import App
from prefect.settings import PREFECT_API_URL, load_profiles

from irsol_data_pipeline.prefect.config import PREFECT_SERVER_PORT

prefect_app = App(name="prefect", help="Run Prefect server commands.")


def _resolve_server_port_from_active_profile() -> int:
    """Resolve the Prefect server port from the active profile API URL.

    Returns:
        Port declared in the active profile ``PREFECT_API_URL`` setting.
        Falls back to the project default port when unset or invalid.
    """

    profiles = load_profiles()
    active_profile = profiles.active_profile
    if active_profile is None:
        return PREFECT_SERVER_PORT

    api_url = active_profile.settings.get(PREFECT_API_URL)
    if not isinstance(api_url, str) or not api_url.strip():
        return PREFECT_SERVER_PORT

    parsed_url = urlparse(api_url)
    if parsed_url.port is None:
        return PREFECT_SERVER_PORT

    return parsed_url.port


@prefect_app.command(name="start")
def start_prefect_server() -> None:
    """Start the Prefect server using the active profile API port."""

    server_port = _resolve_server_port_from_active_profile()

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "prefect",
            "server",
            "start",
            "--port",
            f"{server_port}",
        ],
        check=False,
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
    "irsol_data_pipeline.cli.commands.prefect_command.configure_command:configure_prefect",
    name="configure",
    help="Create or update the default Prefect profile used by the CLI.",
)

prefect_app.command(
    "irsol_data_pipeline.cli.commands.prefect_command.variables_command:variables_app",
    name="variables",
    help="List and configure Prefect variables.",
)
