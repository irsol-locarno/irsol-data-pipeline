"""Shared configuration for local Prefect server operations."""

from __future__ import annotations

PREFECT_SERVER_HOST = "127.0.0.1"
PREFECT_SERVER_PORT = 4200


def build_prefect_server_base_url(host: str, port: int) -> str:
    """Build the Prefect server base URL.

    Args:
        host: Prefect server host.
        port: Prefect server port.

    Returns:
        Base URL for the Prefect server.
    """
    return f"http://{host}:{port}"


def build_prefect_api_url(host: str, port: int) -> str:
    """Build the Prefect API URL.

    Args:
        host: Prefect server host.
        port: Prefect server port.

    Returns:
        API base URL for the Prefect server.
    """
    return f"{build_prefect_server_base_url(host, port)}/api"


def build_prefect_api_healthcheck_url(host: str, port: int) -> str:
    """Build the Prefect API health-check URL.

    Args:
        host: Prefect server host.
        port: Prefect server port.

    Returns:
        Health-check URL for the Prefect server API.
    """
    return f"{build_prefect_api_url(host, port)}/health"


PREFECT_SERVER_BASE_URL = build_prefect_server_base_url(
    PREFECT_SERVER_HOST,
    PREFECT_SERVER_PORT,
)
PREFECT_API_URL = build_prefect_api_url(PREFECT_SERVER_HOST, PREFECT_SERVER_PORT)
PREFECT_API_HEALTHCHECK_URL = build_prefect_api_healthcheck_url(
    PREFECT_SERVER_HOST,
    PREFECT_SERVER_PORT,
)
