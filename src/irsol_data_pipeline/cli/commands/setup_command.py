"""User-facing Prefect client setup CLI command."""

from __future__ import annotations

from prefect.settings import (
    PREFECT_API_URL,
    PREFECT_SERVER_ANALYTICS_ENABLED,
    Profile,
    load_profiles,
    save_profiles,
)

from irsol_data_pipeline.prefect.config import (
    PREFECT_SERVER_HOST,
    PREFECT_SERVER_PORT,
    build_prefect_api_url,
)

DEFAULT_PREFECT_PROFILE_NAME = "default"


def _prompt_server_host() -> str:
    """Prompt for the Prefect server hostname or IP address.

    Returns:
        Selected server host.
    """

    raw = input(f"Prefect server host [{PREFECT_SERVER_HOST}]: ").strip()
    return raw if raw else PREFECT_SERVER_HOST


def _prompt_server_port() -> int:
    """Prompt for the Prefect server port.

    Returns:
        Selected server port.
    """

    while True:
        raw = input(f"Prefect server port [{PREFECT_SERVER_PORT}]: ").strip()
        if not raw:
            return PREFECT_SERVER_PORT

        try:
            port = int(raw)
        except ValueError:
            print("  x Port must be an integer.")
            continue

        if 1 <= port <= 65535:
            return port

        print("  x Port must be in range 1–65535.")


def setup() -> int:
    """Configure IDP and the Prefect client profile to connect to the shared
    server.

    Creates or updates the ``default`` Prefect profile so that all ``idp``
    commands contact the correct server.  Only the API URL and analytics flag
    are written — database settings are not touched because client users do not
    run the server locally.

    Returns:
        Exit code for the command.
    """

    print("Prefect Client Setup\n")
    print(
        "This command configures your local Prefect profile so that `idp` commands\n"
        "connect to the shared Prefect server on this machine.\n"
    )

    host = _prompt_server_host()
    port = _prompt_server_port()
    api_url = build_prefect_api_url(host, port)

    settings = {
        PREFECT_API_URL: api_url,
        PREFECT_SERVER_ANALYTICS_ENABLED: "false",
    }

    profiles = load_profiles()
    profile_exists = DEFAULT_PREFECT_PROFILE_NAME in profiles.names

    if profile_exists:
        profiles.update_profile(DEFAULT_PREFECT_PROFILE_NAME, settings)
    else:
        profiles.add_profile(
            Profile(name=DEFAULT_PREFECT_PROFILE_NAME, settings=settings)
        )

    profiles.set_active(DEFAULT_PREFECT_PROFILE_NAME)
    save_profiles(profiles)

    action = "Updated" if profile_exists else "Created"
    print(f"\n{action} Prefect profile '{DEFAULT_PREFECT_PROFILE_NAME}'.")
    print(f"  PREFECT_API_URL={api_url}")
    print("  PREFECT_SERVER_ANALYTICS_ENABLED=false")
    print("\nRun `idp info` to verify that Prefect variables are resolved correctly.")

    return 0
