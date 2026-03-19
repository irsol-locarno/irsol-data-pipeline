"""Start the Prefect server dashboard."""

from __future__ import annotations

import subprocess
import sys


def main() -> None:
    """Configure Prefect for local development and start the server dashboard.

    Equivalent to the ``make prefect/dashboard`` target: sets the local API
    URL and disables analytics before launching ``prefect server start``.
    """
    subprocess.run(
        [
            "prefect",
            "config",
            "set",
            "PREFECT_API_URL=http://localhost:4200/api",
        ],
        check=True,
    )
    subprocess.run(
        [
            "prefect",
            "config",
            "set",
            "PREFECT_SERVER_ANALYTICS_ENABLED=false",
        ],
        check=True,
    )
    result = subprocess.run(["prefect", "server", "start"])
    sys.exit(result.returncode)
